#!/usr/bin/env python3
"""
Lead Validation ETL Pipeline
Extracts leads from Salesforce and runs comprehensive validation analysis
"""

import os
import sys
import argparse
import logging
import requests
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, Any, List
import pandas as pd
import duckdb
from simple_salesforce import Salesforce
from dotenv import load_dotenv

# Add project root to path for imports
project_root = Path(__file__).parent
sys.path.append(str(project_root))

from config.settings import (
    SF_CLIENT_ID, SF_CLIENT_SECRET, SF_USERNAME, SF_PASSWORD, 
    SF_SECURITY_TOKEN, DUCKDB_PATH, DATA_RETENTION_DAYS
)
from src.utils.validation_parser import ValidationDataParser


class LeadValidationETL:
    """Main ETL pipeline for lead validation."""
    
    def __init__(self):
        self.auth_info = None
        self.parser = ValidationDataParser()
        self.db_path = DUCKDB_PATH
        self.results = {
            'tasks_extracted': 0,
            'validations_parsed': 0,
            'high_quality_leads': 0,
            'low_quality_leads': 0,
            'parsing_errors': 0
        }
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
    
    def authenticate_salesforce(self) -> bool:
        """Authenticate to Salesforce using OAuth 2.0 Password Flow."""
        try:
            self.logger.info("🔐 Authenticating to Salesforce...")
            
            # Get credentials from environment
            client_id = os.getenv('SF_CLIENT_ID')
            client_secret = os.getenv('SF_CLIENT_SECRET')
            username = os.getenv('SF_USERNAME')
            password = os.getenv('SF_PASSWORD')
            security_token = os.getenv('SF_SECURITY_TOKEN')
            token_url = os.getenv('SF_TOKEN_URL')
            
            # Validate required credentials
            if not all([client_id, client_secret, username, password, security_token, token_url]):
                self.logger.error("Missing required Salesforce credentials in environment variables")
                return False
            
            # Build request parameters
            params = {
                'grant_type': 'password',
                'client_id': client_id,
                'client_secret': client_secret,
                'username': username,
                'password': f"{password}{security_token}"
            }
            
            # Make authentication request (matching analytics project)
            headers = {'Content-Type': 'application/x-www-form-urlencoded'}
            response = requests.post(token_url, data=params, headers=headers)
            
            if response.status_code == 200:
                token_data = response.json()
                self.auth_info = {
                    'access_token': token_data['access_token'],
                    'instance_url': token_data['instance_url']  # Always use the URL from auth response
                }
                    
                self.logger.info(f"✅ Successfully authenticated to Salesforce")
                self.logger.info(f"Instance URL: {self.auth_info['instance_url']}")
                return True
            else:
                self.logger.error(f"Authentication failed: {response.status_code} - {response.text}")
                return False
            
        except Exception as e:
            self.logger.error(f"❌ Salesforce authentication failed: {e}")
            return False
    
    def extract_validation_tasks(self, force_refresh: bool = False, days_back: int = 30) -> pd.DataFrame:
        """Extract lead validation tasks from Salesforce."""
        try:
            self.logger.info("📊 Extracting lead validation tasks from Salesforce...")
            
            # Re-authenticate right before the query to ensure fresh token
            if not self.authenticate_salesforce():
                self.logger.error("Failed to authenticate for data extraction")
                return pd.DataFrame()
            
            # Determine date filter
            if force_refresh:
                # Get all validation tasks
                date_filter = ""
                self.logger.info("🔄 Force refresh mode: extracting all validation tasks")
            else:
                # Get tasks from last N days
                cutoff_date = datetime.now() - timedelta(days=days_back)
                date_filter = f"AND LastModifiedDate >= {cutoff_date.strftime('%Y-%m-%dT%H:%M:%SZ')}"
                self.logger.info(f"📅 Incremental mode: extracting tasks from last {days_back} days")
            
            # SOQL query for validation task extraction
            soql_query = f"""
                SELECT Id,
                       WhoId,
                       WhatId,
                       TYPEOF Who
                         WHEN Lead THEN LeadSource, Company, Email
                       END,
                       Subject,
                       Description,
                       CreatedDate,
                       LastModifiedDate
                FROM Task
                WHERE Subject LIKE 'Lead Validation%'
                AND   WhoId IN (SELECT Id FROM Lead)
                {date_filter}
                ORDER BY LastModifiedDate DESC
            """
            
            # Execute query with pagination handling (like analytics project)
            access_token = self.auth_info['access_token']
            instance_url = self.auth_info['instance_url']
            
            # Salesforce REST API endpoint
            query_url = f"{instance_url}/services/data/v58.0/query"
            headers = {
                'Authorization': f'Bearer {access_token}',
                'Content-Type': 'application/json'
            }
            
            all_records = []
            next_records_url = None
            batch_count = 0
            
            self.logger.info(f"📊 Executing SOQL query...")
            
            # First query
            params = {'q': soql_query.strip()}
            response = requests.get(query_url, headers=headers, params=params)
            
            if response.status_code != 200:
                self.logger.error(f"Query failed: {response.status_code} - {response.text}")
                return pd.DataFrame()
                
            result = response.json()
            total_size = result.get('totalSize', 0)
            self.logger.info(f"📈 Total records found: {total_size:,}")
            
            # Process first batch
            records = result.get('records', [])
            all_records.extend(records)
            batch_count += 1
            if records:
                self.logger.info(f"📦 Batch {batch_count}: {len(records):,} records")
            
            # Handle pagination if there are more records
            next_records_url = result.get('nextRecordsUrl')
            
            while next_records_url:
                full_url = f"{instance_url}{next_records_url}"
                response = requests.get(full_url, headers=headers)
                
                if response.status_code != 200:
                    self.logger.error(f"Pagination failed: {response.status_code}")
                    break
                    
                result = response.json()
                records = result.get('records', [])
                all_records.extend(records)
                batch_count += 1
                self.logger.info(f"📦 Batch {batch_count}: {len(records):,} records (total: {len(all_records):,})")
                
                next_records_url = result.get('nextRecordsUrl')
            
            self.logger.info(f"✅ Extraction complete: {len(all_records):,} total records")
            records = all_records
            
            self.logger.info(f"📈 Retrieved {len(records)} validation tasks from Salesforce")
            
            # Convert to DataFrame
            tasks_df = pd.DataFrame(records)
            
            if tasks_df.empty:
                self.logger.warning("No validation tasks found")
                return pd.DataFrame()
            
            # Handle the TYPEOF polymorphic query results
            # Flatten the Who relationship data
            who_data = []
            for record in records:
                who_info = record.get('Who', {})
                if who_info:
                    who_data.append({
                        'WhoId': record.get('WhoId'),
                        'LeadSource': who_info.get('LeadSource'),
                        'Company': who_info.get('Company'), 
                        'Email': who_info.get('Email')
                    })
                else:
                    who_data.append({
                        'WhoId': record.get('WhoId'),
                        'LeadSource': None,
                        'Company': None,
                        'Email': None
                    })
            
            # Create DataFrame from flattened Who data
            who_df = pd.DataFrame(who_data)
            
            # Remove Salesforce metadata columns
            metadata_columns = ['attributes', 'Who']
            tasks_df = tasks_df.drop(columns=[col for col in metadata_columns if col in tasks_df.columns])
            
            # Merge Who data back into tasks
            tasks_df = tasks_df.merge(who_df, on='WhoId', how='left')
            
            # Convert date columns
            date_columns = ['CreatedDate', 'LastModifiedDate']
            for col in date_columns:
                if col in tasks_df.columns:
                    tasks_df[col] = pd.to_datetime(tasks_df[col], errors='coerce')
            
            self.results['leads_extracted'] = len(tasks_df)
            self.logger.info(f"✅ Successfully extracted {len(tasks_df)} validation tasks")
            
            return tasks_df
            
        except Exception as e:
            self.logger.error(f"❌ Error extracting leads: {e}")
            return pd.DataFrame()
    
    def parse_validation_data(self, tasks_df: pd.DataFrame) -> pd.DataFrame:
        """Parse validation data from task descriptions."""
        if tasks_df.empty:
            self.logger.warning("⚠️ No validation tasks to parse")
            return pd.DataFrame()
        
        try:
            self.logger.info(f"🔍 Starting parsing of {len(tasks_df)} validation tasks...")
            
            # Use the parser to extract validation data
            parsed_df = self.parser.parse_batch(tasks_df)
            
            if parsed_df.empty:
                self.logger.warning("No validation data could be parsed")
                return pd.DataFrame()
            
            # Calculate summary statistics
            self.results['validations_parsed'] = len(parsed_df)
            
            # Count high/low quality leads based on API scores
            high_quality_threshold = 7  # Based on quality_score or api_quality_score
            
            high_quality = 0
            low_quality = 0
            parsing_errors = 0
            
            for _, row in parsed_df.iterrows():
                if 'parse_error' in row and pd.notna(row['parse_error']):
                    parsing_errors += 1
                    continue
                
                # Use quality_score if available, otherwise api_quality_score
                quality_score = row.get('quality_score') or row.get('api_quality_score')
                
                if pd.notna(quality_score):
                    if quality_score >= high_quality_threshold:
                        high_quality += 1
                    else:
                        low_quality += 1
            
            self.results['high_quality_leads'] = high_quality
            self.results['low_quality_leads'] = low_quality
            self.results['parsing_errors'] = parsing_errors
            
            self.logger.info(f"✅ Parsing complete:")
            self.logger.info(f"   📊 Total validations parsed: {self.results['validations_parsed']}")
            self.logger.info(f"   ✅ High quality leads: {self.results['high_quality_leads']}")
            self.logger.info(f"   ❌ Low quality leads: {self.results['low_quality_leads']}")
            self.logger.info(f"   ⚠️ Parsing errors: {self.results['parsing_errors']}")
            
            return parsed_df
            
        except Exception as e:
            self.logger.error(f"❌ Error during parsing: {e}")
            return pd.DataFrame()
    
    def setup_database(self):
        """Setup DuckDB database and tables."""
        try:
            self.logger.info("🗄️ Setting up database...")
            
            with duckdb.connect(self.db_path) as conn:
                # Skip schema creation and use table names directly
                
                # Create validation tasks table
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS validation_tasks (
                        task_id VARCHAR PRIMARY KEY,
                        who_id VARCHAR,
                        what_id VARCHAR,
                        subject VARCHAR,
                        description TEXT,
                        lead_source VARCHAR,
                        lead_company VARCHAR,
                        lead_email VARCHAR,
                        created_date TIMESTAMP,
                        last_modified_date TIMESTAMP,
                        extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # Create parsed validation results table
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS parsed_validations (
                        task_id VARCHAR PRIMARY KEY,
                        who_id VARCHAR,
                        lead_source VARCHAR,
                        
                        -- Basic validation scores
                        lead_score INTEGER,
                        quality_score INTEGER,
                        data_quality INTEGER,
                        fraud_score INTEGER,
                        recommendation VARCHAR,
                        quality_level VARCHAR,
                        fraud_risk VARCHAR,
                        market_segment VARCHAR,
                        
                        -- Phone validation
                        phone_valid BOOLEAN,
                        phone_carrier VARCHAR,
                        phone_type VARCHAR,
                        phone_national_format VARCHAR,
                        
                        -- Email validation  
                        email_valid BOOLEAN,
                        email_sendable BOOLEAN,
                        bounce_likely BOOLEAN,
                        gibberish_score VARCHAR,
                        
                        -- Email summary
                        total_emails INTEGER,
                        valid_emails INTEGER,
                        sendable_emails INTEGER,
                        email_quality_score INTEGER,
                        
                        -- API response fields (flattened)
                        api_lead_score INTEGER,
                        api_quality_score INTEGER,
                        api_fraud_score INTEGER,
                        api_data_quality_score INTEGER,
                        api_recommendation VARCHAR,
                        api_quality_level VARCHAR,
                        api_fraud_risk_level VARCHAR,
                        api_market_segment VARCHAR,
                        api_phone_valid BOOLEAN,
                        api_phone_carrier VARCHAR,
                        api_phone_location VARCHAR,
                        api_email_valid BOOLEAN,
                        api_email_sendable BOOLEAN,
                        api_bounce_likely BOOLEAN,
                        api_gibberish_email BOOLEAN,
                        api_fake_phone BOOLEAN,
                        api_fake_lead BOOLEAN,
                        api_disposable_email BOOLEAN,
                        api_business_strength_score INTEGER,
                        api_first_name VARCHAR,
                        api_last_name VARCHAR,
                        api_company VARCHAR,
                        api_email VARCHAR,
                        api_phone VARCHAR,
                        api_state VARCHAR,
                        api_postal_code VARCHAR,
                        api_total_emails INTEGER,
                        api_valid_emails INTEGER,
                        api_sendable_emails INTEGER,
                        api_email_summary_quality_score INTEGER,
                        api_quality_factors TEXT,
                        api_fraud_factors TEXT,
                        api_summary_notes TEXT,
                        
                        -- Lead information
                        lead_company VARCHAR,
                        lead_email VARCHAR,
                        
                        -- Metadata
                        subject VARCHAR,
                        created_date TIMESTAMP,
                        last_modified_date TIMESTAMP,
                        parse_error TEXT,
                        raw_api_response TEXT,
                        raw_description TEXT,
                        
                        parsed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                self.logger.info("✅ Database setup complete")
                
        except Exception as e:
            self.logger.error(f"❌ Database setup failed: {e}")
            raise
    
    def save_data(self, tasks_df: pd.DataFrame, parsed_df: pd.DataFrame):
        """Save extracted and parsed validation data to database."""
        try:
            self.logger.info("💾 Saving data to database...")
            
            with duckdb.connect(self.db_path) as conn:
                # Save raw validation tasks
                if not tasks_df.empty:
                    # Add extraction timestamp
                    tasks_df['extracted_at'] = datetime.now()
                    
                    # Use INSERT OR REPLACE to handle duplicates
                    conn.register('tasks_df', tasks_df)
                    conn.execute("""
                        INSERT OR REPLACE INTO validation_tasks 
                        SELECT Id as task_id, WhoId as who_id, WhatId as what_id, Subject as subject, 
                               Description as description, LeadSource as lead_source, Company as lead_company, 
                               Email as lead_email, CreatedDate as created_date, LastModifiedDate as last_modified_date,
                               extracted_at 
                        FROM tasks_df
                    """)
                    
                    self.logger.info(f"💾 Saved {len(tasks_df)} validation tasks")
                
                # Save parsed validation results
                if not parsed_df.empty:
                    # Filter out records with missing task_id (required field)
                    parsed_df = parsed_df[parsed_df['task_id'].notna() & (parsed_df['task_id'] != '')]
                    
                    if parsed_df.empty:
                        self.logger.warning("⚠️ No valid records to save after filtering missing task_ids")
                        return
                    
                    # Convert raw_api_response to JSON string with proper encoding (if column exists)
                    import json
                    if 'raw_api_response' in parsed_df.columns:
                        parsed_df['raw_api_response'] = parsed_df['raw_api_response'].apply(
                            lambda x: json.dumps(x, ensure_ascii=True) if x and isinstance(x, dict) else str(x) if x else '{}'
                        )
                    
                    # Use INSERT OR REPLACE to handle duplicates (specify columns to avoid type mismatches)
                    conn.register('parsed_df', parsed_df)
                    
                    # Get the column names from the parsed_df that match our table structure
                    table_columns = [
                        'task_id', 'who_id', 'lead_source', 'lead_score', 'quality_score', 'data_quality', 
                        'fraud_score', 'recommendation', 'quality_level', 'fraud_risk', 'market_segment',
                        'phone_valid', 'phone_carrier', 'phone_type', 'phone_national_format',
                        'email_valid', 'email_sendable', 'bounce_likely', 'gibberish_score',
                        'total_emails', 'valid_emails', 'sendable_emails', 'email_quality_score',
                        'api_lead_score', 'api_quality_score', 'api_fraud_score', 'api_data_quality_score',
                        'api_recommendation', 'api_quality_level', 'api_fraud_risk_level', 'api_market_segment',
                        'api_phone_valid', 'api_phone_carrier', 'api_phone_location',
                        'api_email_valid', 'api_email_sendable', 'api_bounce_likely', 'api_gibberish_email',
                        'api_fake_phone', 'api_fake_lead', 'api_disposable_email', 'api_business_strength_score',
                        'api_first_name', 'api_last_name', 'api_company', 'api_email', 'api_phone',
                        'api_state', 'api_postal_code', 'api_total_emails', 'api_valid_emails',
                        'api_sendable_emails', 'api_email_summary_quality_score', 'api_quality_factors',
                        'api_fraud_factors', 'api_summary_notes', 'lead_company', 'lead_email',
                        'subject', 'created_date', 'last_modified_date', 'parse_error', 'raw_api_response', 'raw_description'
                    ]
                    
                    # Filter to only columns that exist in parsed_df
                    existing_columns = [col for col in table_columns if col in parsed_df.columns]
                    
                    # Insert only the existing columns
                    column_list = ', '.join(existing_columns)
                    conn.execute(f"""
                        INSERT OR REPLACE INTO parsed_validations ({column_list})
                        SELECT {column_list} FROM parsed_df
                    """)
                    
                    self.logger.info(f"💾 Saved {len(parsed_df)} parsed validation results")
                
                # Create backup
                self._create_backup(conn)
                
        except Exception as e:
            self.logger.error(f"❌ Error saving data: {e}")
            raise
    
    def _create_backup(self, conn):
        """Create backup of validation results."""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_file = project_root / "reports" / f"validation_backup_{timestamp}.csv"
            
            # Export parsed validation results
            conn.execute(f"""
                COPY (SELECT * FROM parsed_validations) 
                TO '{backup_file}' (FORMAT CSV, HEADER)
            """)
            
            self.logger.info(f"💾 Backup created: {backup_file}")
            
        except Exception as e:
            self.logger.warning(f"⚠️ Backup creation failed: {e}")
    
    def run_full_pipeline(self, force_refresh: bool = False, validation_only: bool = False, days_back: int = 30):
        """Run the complete ETL pipeline."""
        try:
            start_time = datetime.now()
            self.logger.info("🚀 Starting Lead Validation ETL Pipeline")
            self.logger.info(f"⚙️ Configuration: force_refresh={force_refresh}, validation_only={validation_only}, days_back={days_back}")
            
            # Setup database
            self.setup_database()
            
            if not validation_only:
                # Extract validation tasks (authentication happens within extract method)
                
                tasks_df = self.extract_validation_tasks(force_refresh=force_refresh, days_back=days_back)
                if tasks_df.empty:
                    self.logger.warning("⚠️ No validation tasks extracted, ending pipeline")
                    return
            else:
                # Load existing tasks from database for parsing only
                with duckdb.connect(self.db_path) as conn:
                    tasks_df = conn.execute("SELECT * FROM leads.validation_tasks").df()
                self.logger.info(f"📊 Loaded {len(tasks_df)} existing validation tasks for parsing")
            
            # Parse validation data
            parsed_df = self.parse_validation_data(tasks_df)
            if parsed_df.empty:
                self.logger.warning("⚠️ No validation data could be parsed")
                return
            
            # Save data
            if not validation_only:
                self.save_data(tasks_df, parsed_df)
            else:
                self.save_data(pd.DataFrame(), parsed_df)  # Only save parsed results
            
            # Print summary
            end_time = datetime.now()
            duration = end_time - start_time
            
            self.logger.info("🎯 Pipeline Completed Successfully!")
            self.logger.info(f"⏱️ Duration: {duration}")
            self.logger.info("📊 Results Summary:")
            for key, value in self.results.items():
                self.logger.info(f"   {key}: {value}")
            
            # Print validation insights
            self._print_validation_insights(parsed_df)
            
        except Exception as e:
            self.logger.error(f"❌ Pipeline failed: {e}")
            raise
    
    def _print_validation_insights(self, parsed_df: pd.DataFrame):
        """Print key validation insights."""
        if parsed_df.empty:
            self.logger.info("🔍 Validation Insights: No data to analyze")
            return
        
        try:
            self.logger.info(f"\n🔍 Validation Insights: Processed {len(parsed_df)} records")
            self.logger.info(f"   📊 Available columns: {len(parsed_df.columns)}")
            
            # Simple record count analysis
            if 'lead_source' in parsed_df.columns:
                source_counts = parsed_df['lead_source'].value_counts().head(5)
                self.logger.info("   📊 Top Lead Sources:")
                for source, count in source_counts.items():
                    self.logger.info(f"      {source}: {count} records")
            
            return  # Skip complex analysis for now to avoid column errors
        
        except Exception as e:
            self.logger.warning(f"   ⚠️ Could not generate insights: {e}")
            return
        
        self.logger.info("\n🔍 Validation Insights:")
        
        # Quality score distribution (check which quality score column exists)
        quality_scores = None
        if 'api_quality_score' in parsed_df.columns:
            quality_scores = parsed_df['api_quality_score'].dropna()
        elif 'quality_score' in parsed_df.columns:
            quality_scores = parsed_df['quality_score'].dropna()
        if quality_scores is not None and not quality_scores.empty:
            score_ranges = [
                (9, 10, "Excellent"),
                (7, 9, "Good"),
                (5, 7, "Fair"),
                (3, 5, "Poor"),
                (0, 3, "Invalid")
            ]
            
            for min_score, max_score, label in score_ranges:
                count = len(quality_scores[(quality_scores >= min_score) & (quality_scores < max_score)])
                percentage = (count / len(quality_scores)) * 100
                self.logger.info(f"   {label} ({min_score}-{max_score}): {count} leads ({percentage:.1f}%)")
        
        # Average scores by lead source
        if 'lead_source' in parsed_df.columns:
            # Build aggregation dict based on available columns
            agg_dict = {}
            if 'api_quality_score' in parsed_df.columns:
                agg_dict['api_quality_score'] = 'mean'
            elif 'quality_score' in parsed_df.columns:
                agg_dict['quality_score'] = 'mean'
            
            if 'api_lead_score' in parsed_df.columns:
                agg_dict['api_lead_score'] = 'mean'
            
            if 'api_fraud_score' in parsed_df.columns:
                agg_dict['api_fraud_score'] = 'mean'
            
            if 'task_id' in parsed_df.columns:
                agg_dict['task_id'] = 'count'
            
            if agg_dict:
                source_analysis = parsed_df.groupby('lead_source').agg(agg_dict).round(2)
            
            source_analysis.columns = ['Avg_Quality', 'Avg_Lead', 'Avg_Fraud', 'Count']
            source_analysis = source_analysis.sort_values('Avg_Quality', ascending=False)
            
            self.logger.info("\n📈 Performance by Lead Source:")
            self.logger.info("   Source | Quality | Lead | Fraud | Count")
            self.logger.info("   " + "-" * 45)
            
            for source, row in source_analysis.head(10).iterrows():
                self.logger.info(f"   {source[:15]:<15} | {row['Avg_Quality']:>7.1f} | {row['Avg_Lead']:>4.1f} | {row['Avg_Fraud']:>5.1f} | {row['Count']:>5}")
        
        # Worst performing sources
        if 'lead_source' in parsed_df.columns and not quality_scores.empty:
            worst_sources = source_analysis.tail(3)
            self.logger.info("\n🚨 Sources Needing Attention:")
            for source, row in worst_sources.iterrows():
                self.logger.info(f"   ⚠️ {source}: Quality Score {row['Avg_Quality']:.1f} ({row['Count']} leads)")
        
        # Data quality indicators
        fraud_indicators = parsed_df['api_fake_lead'].sum() if 'api_fake_lead' in parsed_df.columns else 0
        disposable_emails = parsed_df['api_disposable_email'].sum() if 'api_disposable_email' in parsed_df.columns else 0
        fake_phones = parsed_df['api_fake_phone'].sum() if 'api_fake_phone' in parsed_df.columns else 0
        
        self.logger.info(f"\n📊 Data Quality Flags:")
        self.logger.info(f"   🚨 Fake leads detected: {fraud_indicators}")
        self.logger.info(f"   📧 Disposable emails: {disposable_emails}")
        self.logger.info(f"   📱 Fake phone numbers: {fake_phones}")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Lead Validation ETL Pipeline")
    parser.add_argument("--force-refresh", action="store_true", help="Force refresh all leads")
    parser.add_argument("--validation-only", action="store_true", help="Run validation only (skip extraction)")
    parser.add_argument("--days-back", type=int, default=30, help="Days back to extract leads (default: 30)")
    
    args = parser.parse_args()
    
    # Load environment variables
    load_dotenv()
    
    # Run pipeline
    etl = LeadValidationETL()
    etl.run_full_pipeline(
        force_refresh=args.force_refresh,
        validation_only=args.validation_only,
        days_back=args.days_back
    )


if __name__ == "__main__":
    main()
