"""Parser for lead validation data from Salesforce Task descriptions."""
import json
import re
from typing import Dict, Any, Optional
import pandas as pd
import logging


class ValidationDataParser:
    """Parse lead validation data from Task description fields."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def parse_description(self, description: str) -> Dict[str, Any]:
        """
        Parse validation data from Task description field.
        
        Args:
            description: The full description text containing validation results
            
        Returns:
            Dictionary with parsed validation data
        """
        if not description:
            return {}
        
        try:
            # Initialize result dictionary
            result = {
                'raw_description': description,
                'validation_sections': {},
                'raw_api_response': {}
            }
            
            # Extract sections
            result.update(self._extract_lead_validation_section(description))
            result.update(self._extract_phone_validation_section(description))
            result.update(self._extract_email_validation_section(description))
            result.update(self._extract_email_summary_section(description))
            
            # Extract and parse JSON from RAW API RESPONSE
            raw_json = self._extract_raw_api_response(description)
            if raw_json:
                result['raw_api_response'] = raw_json
                result.update(self._flatten_api_response(raw_json))
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error parsing description: {e}")
            return {'parse_error': str(e), 'raw_description': description}
    
    def _extract_lead_validation_section(self, description: str) -> Dict[str, Any]:
        """Extract lead validation results section."""
        section = {}
        
        # Look for the LEAD VALIDATION RESULTS section
        pattern = r'=== LEAD VALIDATION RESULTS ===(.*?)(?==== |$)'
        match = re.search(pattern, description, re.DOTALL)
        
        if match:
            content = match.group(1).strip()
            
            # Extract individual fields
            fields = {
                'lead_score': r'Lead Score:\s*(\d+)',
                'quality_score': r'Quality Score:\s*(\d+)', 
                'data_quality': r'Data Quality:\s*(\d+)%',
                'fraud_score': r'Fraud Score:\s*(\d+)',
                'recommendation': r'Recommendation:\s*(\w+)',
                'quality_level': r'Quality Level:\s*(\w+)',
                'fraud_risk': r'Fraud Risk:\s*(\w+)',
                'market_segment': r'Market Segment:\s*(.+)'
            }
            
            for field, pattern in fields.items():
                match = re.search(pattern, content, re.IGNORECASE)
                if match:
                    value = match.group(1).strip()
                    # Convert numeric values
                    if field in ['lead_score', 'quality_score', 'data_quality', 'fraud_score']:
                        try:
                            section[field] = int(value)
                        except ValueError:
                            section[field] = value
                    else:
                        section[field] = value
        
        return section
    
    def _extract_phone_validation_section(self, description: str) -> Dict[str, Any]:
        """Extract phone validation section."""
        section = {}
        
        pattern = r'=== PHONE VALIDATION ===(.*?)(?==== |$)'
        match = re.search(pattern, description, re.DOTALL)
        
        if match:
            content = match.group(1).strip()
            
            fields = {
                'phone_valid': r'Phone Valid:\s*(true|false)',
                'phone_carrier': r'Carrier:\s*(.+)',
                'phone_type': r'Type:\s*(.+)',
                'phone_national_format': r'National Format:\s*(.+)'
            }
            
            for field, pattern in fields.items():
                match = re.search(pattern, content, re.IGNORECASE)
                if match:
                    value = match.group(1).strip()
                    if field == 'phone_valid':
                        section[field] = value.lower() == 'true'
                    elif value.lower() == 'null':
                        section[field] = None
                    else:
                        section[field] = value
        
        return section
    
    def _extract_email_validation_section(self, description: str) -> Dict[str, Any]:
        """Extract email validation section."""
        section = {}
        
        pattern = r'=== EMAIL VALIDATION ===(.*?)(?==== |$)'
        match = re.search(pattern, description, re.DOTALL)
        
        if match:
            content = match.group(1).strip()
            
            fields = {
                'email_valid': r'Email Valid:\s*(true|false)',
                'email_sendable': r'Email Sendable:\s*(true|false)', 
                'bounce_likely': r'Bounce Likely:\s*(true|false)',
                'gibberish_score': r'Gibberish Score:\s*(.+)'
            }
            
            for field, pattern in fields.items():
                match = re.search(pattern, content, re.IGNORECASE)
                if match:
                    value = match.group(1).strip()
                    if field in ['email_valid', 'email_sendable', 'bounce_likely']:
                        section[field] = value.lower() == 'true'
                    elif value.lower() == 'null':
                        section[field] = None
                    else:
                        section[field] = value
        
        return section
    
    def _extract_email_summary_section(self, description: str) -> Dict[str, Any]:
        """Extract email summary section."""
        section = {}
        
        pattern = r'=== EMAIL SUMMARY ===(.*?)(?==== |$)'
        match = re.search(pattern, description, re.DOTALL)
        
        if match:
            content = match.group(1).strip()
            
            fields = {
                'total_emails': r'Total Emails:\s*(\d+)',
                'valid_emails': r'Valid Emails:\s*(\d+)',
                'sendable_emails': r'Sendable Emails:\s*(\d+)',
                'email_quality_score': r'Email Quality Score:\s*(\d+)'
            }
            
            for field, pattern in fields.items():
                match = re.search(pattern, content, re.IGNORECASE)
                if match:
                    try:
                        section[field] = int(match.group(1))
                    except ValueError:
                        section[field] = match.group(1)
        
        return section
    
    def _extract_raw_api_response(self, description: str) -> Optional[Dict[str, Any]]:
        """Extract and parse JSON from RAW API RESPONSE section."""
        pattern = r'=== RAW API RESPONSE ===(.*?)(?==== |$)'
        match = re.search(pattern, description, re.DOTALL)
        
        if match:
            json_content = match.group(1).strip()
            
            try:
                # The JSON might have some formatting issues, try to clean it up
                json_content = self._clean_json_content(json_content)
                return json.loads(json_content)
            except json.JSONDecodeError as e:
                self.logger.warning(f"Failed to parse JSON: {e}")
                return {'json_parse_error': str(e), 'raw_content': json_content}
        
        return None
    
    def _clean_json_content(self, content: str) -> str:
        """Clean up JSON content for parsing."""
        # Remove any non-JSON content before and after the JSON
        content = content.strip()
        
        # Find the start and end of JSON
        start = content.find('{')
        end = content.rfind('}')
        
        if start != -1 and end != -1:
            content = content[start:end+1]
        
        return content
    
    def _flatten_api_response(self, api_response: Dict[str, Any]) -> Dict[str, Any]:
        """Flatten important fields from API response for easier querying."""
        flattened = {}
        
        # Direct mappings from API response
        field_mappings = {
            'api_lead_score': 'leadScore',
            'api_quality_score': 'qualityScore', 
            'api_fraud_score': 'fraudScore',
            'api_data_quality_score': 'dataQualityScore',
            'api_recommendation': 'recommendation',
            'api_quality_level': 'qualityLevel',
            'api_fraud_risk_level': 'fraudRiskLevel',
            'api_market_segment': 'marketSegment',
            'api_phone_valid': 'phoneValid',
            'api_phone_carrier': 'phoneCarrier',
            'api_phone_location': 'phoneLocation',
            'api_email_valid': 'emailValid',
            'api_email_sendable': 'emailSendable',
            'api_bounce_likely': 'isBounceLikely',
            'api_gibberish_email': 'isGibberishEmail',
            'api_fake_phone': 'isFakePhone',
            'api_fake_lead': 'isFakeLead',
            'api_disposable_email': 'isDisposable',
            'api_business_strength_score': 'businessStrengthScore',
            'api_first_name': 'first_name',
            'api_last_name': 'last_name',
            'api_company': 'company',
            'api_email': 'email',
            'api_phone': 'phone',
            'api_state': 'state',
            'api_postal_code': 'postalCode'
        }
        
        for new_field, api_field in field_mappings.items():
            if api_field in api_response:
                flattened[new_field] = api_response[api_field]
        
        # Handle nested structures
        if 'emailSummary' in api_response:
            email_summary = api_response['emailSummary']
            flattened.update({
                'api_total_emails': email_summary.get('totalEmails'),
                'api_valid_emails': email_summary.get('validEmails'),
                'api_sendable_emails': email_summary.get('sendableEmails'),
                'api_email_summary_quality_score': email_summary.get('qualityScore')
            })
        
        # Handle arrays as comma-separated strings
        if 'qualityFactors' in api_response:
            flattened['api_quality_factors'] = ', '.join(api_response['qualityFactors'])
        
        if 'fraudFactors' in api_response:
            flattened['api_fraud_factors'] = ', '.join(api_response['fraudFactors'])
        
        if 'summaryNotes' in api_response:
            flattened['api_summary_notes'] = ', '.join(api_response['summaryNotes'])
        
        return flattened
    
    def parse_batch(self, tasks_df: pd.DataFrame) -> pd.DataFrame:
        """Parse validation data for a batch of Task records."""
        if tasks_df.empty:
            return pd.DataFrame()
        
        parsed_data = []
        
        for idx, row in tasks_df.iterrows():
            if idx % 100 == 0:
                self.logger.info(f"Parsing validation data {idx + 1}/{len(tasks_df)}")
            
            # Parse the description
            parsed = self.parse_description(row.get('Description', ''))
            
            # Add Task metadata
            parsed.update({
                'task_id': row.get('Id'),
                'who_id': row.get('WhoId'),
                'what_id': row.get('WhatId'),
                'subject': row.get('Subject'),
                'lead_source': row.get('LeadSource'),
                'lead_company': row.get('Company'),
                'lead_email': row.get('Email'),
                'created_date': row.get('CreatedDate'),
                'last_modified_date': row.get('LastModifiedDate')
            })
            
            parsed_data.append(parsed)
        
        return pd.DataFrame(parsed_data)
