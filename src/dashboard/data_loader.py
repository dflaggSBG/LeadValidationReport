"""Data loading functions for the Lead Validation Dashboard."""
import duckdb
import pandas as pd
from pathlib import Path
import sys
from typing import Optional, Dict, Any
import logging

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from config.settings import DUCKDB_PATH


def get_database_connection():
    """Get a DuckDB database connection."""
    return duckdb.connect(DUCKDB_PATH)


class DuckDBManager:
    """Context manager for DuckDB connections."""
    
    def __init__(self, db_path: str = DUCKDB_PATH):
        self.db_path = db_path
        self.conn = None
    
    def __enter__(self):
        self.conn = duckdb.connect(self.db_path)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            self.conn.close()
    
    def execute_query(self, query: str) -> pd.DataFrame:
        """Execute a query and return results as DataFrame."""
        try:
            return self.conn.execute(query).df()
        except Exception as e:
            logging.error(f"Query execution failed: {e}")
            return pd.DataFrame()
    
    def table_exists(self, schema: str, table: str) -> bool:
        """Check if a table exists."""
        try:
            result = self.conn.execute(f"""
                SELECT COUNT(*) as count 
                FROM information_schema.tables 
                WHERE table_schema = '{schema}' AND table_name = '{table}'
            """).fetchone()
            return result[0] > 0 if result else False
        except:
            return False


def load_validation_metrics() -> pd.DataFrame:
    """Load overall validation metrics."""
    with DuckDBManager() as db:
        # Check if parsed_validations table exists
        try:
            db.conn.execute("SELECT COUNT(*) FROM parsed_validations LIMIT 1")
            table_exists = True
        except:
            table_exists = False
        
        if not table_exists:
            return pd.DataFrame()
        
        # Ensure views exist
        create_views(db)
        
        query = """
        SELECT * FROM lead_validation_overview
        """
        return db.execute_query(query)


def load_validation_summary() -> pd.DataFrame:
    """Load parsed validation data summary."""
    with DuckDBManager() as db:
        # Check if parsed_validations table exists
        try:
            db.conn.execute("SELECT COUNT(*) FROM parsed_validations LIMIT 1")
            table_exists = True
        except:
            table_exists = False
        
        if not table_exists:
            return pd.DataFrame()
        
        create_views(db)
        
        query = """
        SELECT 
            task_id,
            who_id as lead_id,
            lead_source,
            COALESCE(api_quality_score, quality_score) as overall_score,
            CASE 
                WHEN COALESCE(api_quality_score, quality_score) >= 9 THEN 'Excellent'
                WHEN COALESCE(api_quality_score, quality_score) >= 7 THEN 'Good'
                WHEN COALESCE(api_quality_score, quality_score) >= 5 THEN 'Fair'
                WHEN COALESCE(api_quality_score, quality_score) >= 3 THEN 'Poor'
                ELSE 'Invalid'
            END as validation_status,
            COALESCE(api_first_name, '') as first_name,
            COALESCE(api_last_name, '') as last_name,
            COALESCE(api_email, lead_email) as email,
            COALESCE(api_phone, '') as phone,
            COALESCE(api_company, lead_company) as company,
            created_date,
            last_modified_date,
            FALSE as is_converted,  -- Not tracking conversions in this data
            NULL as converted_date,
            parsed_at as validation_timestamp
        FROM parsed_validations
        WHERE parse_error IS NULL
        ORDER BY parsed_at DESC
        LIMIT 1000
        """
        return db.execute_query(query)


def load_validation_by_source() -> pd.DataFrame:
    """Load validation metrics by lead source."""
    with DuckDBManager() as db:
        # Check if parsed_validations table exists
        try:
            db.conn.execute("SELECT COUNT(*) FROM parsed_validations LIMIT 1")
            table_exists = True
        except:
            table_exists = False
        
        if not table_exists:
            return pd.DataFrame()
        
        create_views(db)
        
        query = """
        SELECT * FROM lead_source_quality_summary
        ORDER BY avg_quality_score DESC
        """
        return db.execute_query(query)


def load_validation_trends() -> pd.DataFrame:
    """Load validation trends data."""
    with DuckDBManager() as db:
        # Check if parsed_validations table exists
        try:
            db.conn.execute("SELECT COUNT(*) FROM parsed_validations LIMIT 1")
            table_exists = True
        except:
            table_exists = False
        
        if not table_exists:
            return pd.DataFrame()
        
        create_views(db)
        
        query = """
        WITH daily_trends AS (
            SELECT 
                DATE_TRUNC('day', created_date) as period_start,
                COUNT(*) as leads_validated,
                AVG(COALESCE(api_quality_score, quality_score)) as avg_score,
                COUNTIF(COALESCE(api_quality_score, quality_score) >= 7) as quality_leads,
                COUNTIF(COALESCE(api_fake_lead, false)) as fake_leads,
                COUNT(DISTINCT lead_source) as unique_sources
            FROM parsed_validations
            WHERE parse_error IS NULL
            AND created_date >= CURRENT_DATE - INTERVAL '30 days'
            GROUP BY DATE_TRUNC('day', created_date)
        )
        SELECT 
            'daily' as period_type,
            period_start,
            period_start + INTERVAL '1 day' as period_end,
            leads_validated,
            ROUND(avg_score, 4) as avg_score,
            quality_leads,
            ROUND((quality_leads::DOUBLE / leads_validated) * 100, 2) as quality_percentage,
            fake_leads as converted_leads,  -- Reusing this field for fake leads
            ROUND((fake_leads::DOUBLE / leads_validated) * 100, 2) as conversion_rate,  -- Actually fake lead rate
            unique_sources,
            
            -- Trend indicators
            LAG(avg_score) OVER (ORDER BY period_start) as prev_avg_score,
            ROUND(avg_score - LAG(avg_score) OVER (ORDER BY period_start), 4) as score_change,
            LAG(leads_validated) OVER (ORDER BY period_start) as prev_volume,
            leads_validated - LAG(leads_validated) OVER (ORDER BY period_start) as volume_change
        FROM daily_trends
        ORDER BY period_start DESC
        LIMIT 30
        """
        return db.execute_query(query)


def load_recent_validations(limit: int = 100) -> pd.DataFrame:
    """Load recent validation results."""
    with DuckDBManager() as db:
        # Check if parsed_validations table exists
        try:
            db.conn.execute("SELECT COUNT(*) FROM parsed_validations LIMIT 1")
            table_exists = True
        except:
            table_exists = False
        
        if not table_exists:
            return pd.DataFrame()
        
        query = f"""
        SELECT 
            task_id as lead_id,
            parsed_at as validation_timestamp,
            COALESCE(api_quality_score, quality_score) as overall_score,
            CASE 
                WHEN COALESCE(api_quality_score, quality_score) >= 9 THEN 'Excellent'
                WHEN COALESCE(api_quality_score, quality_score) >= 7 THEN 'Good'
                WHEN COALESCE(api_quality_score, quality_score) >= 5 THEN 'Fair'
                WHEN COALESCE(api_quality_score, quality_score) >= 3 THEN 'Poor'
                ELSE 'Invalid'
            END as validation_status,
            COALESCE(api_first_name, '') as first_name,
            COALESCE(api_last_name, '') as last_name,
            COALESCE(api_email, lead_email) as email,
            COALESCE(api_phone, '') as phone,
            COALESCE(api_company, lead_company) as company,
            lead_source,
            false as is_converted
        FROM parsed_validations
        WHERE parse_error IS NULL
        ORDER BY parsed_at DESC
        LIMIT {limit}
        """
        return db.execute_query(query)


def load_problematic_leads(score_threshold: float = 6) -> pd.DataFrame:
    """Load leads with validation issues."""
    with DuckDBManager() as db:
        # Check if parsed_validations table exists
        try:
            db.conn.execute("SELECT COUNT(*) FROM parsed_validations LIMIT 1")
            table_exists = True
        except:
            table_exists = False
        
        if not table_exists:
            return pd.DataFrame()
        
        create_views(db)
        
        query = f"""
        SELECT 
            task_id as lead_id,
            COALESCE(api_quality_score, quality_score) as overall_score,
            CASE 
                WHEN COALESCE(api_quality_score, quality_score) >= 9 THEN 'Excellent'
                WHEN COALESCE(api_quality_score, quality_score) >= 7 THEN 'Good'
                WHEN COALESCE(api_quality_score, quality_score) >= 5 THEN 'Fair'
                WHEN COALESCE(api_quality_score, quality_score) >= 3 THEN 'Poor'
                ELSE 'Invalid'
            END as validation_status,
            COALESCE(api_first_name, '') as first_name,
            COALESCE(api_last_name, '') as last_name,
            COALESCE(api_email, lead_email) as email,
            COALESCE(api_phone, '') as phone,
            COALESCE(api_company, lead_company) as company,
            lead_source,
            parsed_at as validation_timestamp,
            false as is_converted
        FROM parsed_validations
        WHERE parse_error IS NULL
        AND COALESCE(api_quality_score, quality_score) < {score_threshold}
        ORDER BY COALESCE(api_quality_score, quality_score) ASC, parsed_at DESC
        LIMIT 500
        """
        return db.execute_query(query)


def load_conversion_analysis() -> pd.DataFrame:
    """Load quality score analysis data (replacing conversion since we don't track conversions)."""
    with DuckDBManager() as db:
        # Check if parsed_validations table exists
        try:
            db.conn.execute("SELECT COUNT(*) FROM parsed_validations LIMIT 1")
            table_exists = True
        except:
            table_exists = False
        
        if not table_exists:
            return pd.DataFrame()
        
        create_views(db)
        
        query = """
        WITH categorized AS (
            SELECT 
                CASE 
                    WHEN COALESCE(api_quality_score, quality_score) >= 9 THEN 'Excellent'
                    WHEN COALESCE(api_quality_score, quality_score) >= 7 THEN 'Good'
                    WHEN COALESCE(api_quality_score, quality_score) >= 5 THEN 'Fair'
                    WHEN COALESCE(api_quality_score, quality_score) >= 3 THEN 'Poor'
                    ELSE 'Invalid'
                END as score_category,
                COALESCE(api_quality_score, quality_score) as quality_score,
                COALESCE(api_fake_lead, false) as is_fake
            FROM parsed_validations
            WHERE parse_error IS NULL
        )
        SELECT 
            score_category,
            COUNT(*) as total_leads,
            COUNTIF(is_fake) as converted_leads,  -- Using fake leads as proxy
            ROUND((COUNTIF(is_fake)::DOUBLE / COUNT(*)) * 100, 2) as conversion_rate,  -- Actually fraud rate
            AVG(quality_score) as avg_score
        FROM categorized
        GROUP BY score_category
        ORDER BY 
            CASE score_category
                WHEN 'Excellent' THEN 1
                WHEN 'Good' THEN 2
                WHEN 'Fair' THEN 3
                WHEN 'Poor' THEN 4
                WHEN 'Invalid' THEN 5
            END
        """
        return db.execute_query(query)


def create_views(db: DuckDBManager):
    """Create analytical views if they don't exist."""
    # Read and execute view SQL files
    views_dir = project_root / "sql" / "views"
    
    # Create views in dependency order
    view_files = [
        "lead_source_quality_summary.sql",  # First - no dependencies
        "worst_lead_sources.sql",           # Second - depends on lead_source_quality_summary
        "lead_validation_overview.sql"      # Last - depends on worst_lead_sources
    ]
    
    for view_file in view_files:
        view_path = views_dir / view_file
        if view_path.exists():
            try:
                with open(view_path, 'r') as f:
                    sql_content = f.read()
                db.conn.execute(sql_content)
            except Exception as e:
                logging.warning(f"Failed to create view from {view_file}: {e}")


def load_worst_lead_sources() -> pd.DataFrame:
    """Load worst performing lead sources that need attention."""
    with DuckDBManager() as db:
        # Check if parsed_validations table exists
        try:
            db.conn.execute("SELECT COUNT(*) FROM parsed_validations LIMIT 1")
            table_exists = True
        except:
            table_exists = False
        
        if not table_exists:
            return pd.DataFrame()
        
        create_views(db)
        
        query = """
        SELECT * FROM worst_lead_sources
        ORDER BY problem_score DESC
        LIMIT 20
        """
        return db.execute_query(query)


def get_data_freshness() -> Dict[str, Any]:
    """Get data freshness information."""
    with DuckDBManager() as db:
        # Check if parsed_validations table exists
        try:
            db.conn.execute("SELECT COUNT(*) FROM parsed_validations LIMIT 1")
            table_exists = True
        except:
            table_exists = False
        
        if not table_exists:
            return {"status": "No data", "last_validation": None, "total_leads": 0}
        
        query = """
        SELECT 
            MAX(parsed_at) as last_validation,
            COUNT(DISTINCT task_id) as total_leads,
            COUNT(*) as total_validations,
            AVG(COALESCE(api_quality_score, quality_score)) as avg_score
        FROM parsed_validations
        WHERE parse_error IS NULL
        """
        result = db.execute_query(query)
        
        if result.empty:
            return {"status": "No data", "last_validation": None, "total_leads": 0}
        
        row = result.iloc[0]
        
        if row['last_validation'] is None:
            return {"status": "No data", "last_validation": None, "total_leads": 0}
        
        # Calculate freshness status
        last_validation = pd.to_datetime(row['last_validation'])
        hours_since = (pd.Timestamp.now() - last_validation).total_seconds() / 3600
        
        if hours_since <= 24:
            status = "Fresh"
        elif hours_since <= 168:  # 1 week
            status = "Recent"
        else:
            status = "Stale"
        
        return {
            "status": status,
            "last_validation": last_validation,
            "hours_since_validation": hours_since,
            "total_leads": int(row['total_leads']),
            "total_validations": int(row['total_validations']),
            "avg_score": float(row['avg_score']) if row['avg_score'] is not None else 0.0
        }
