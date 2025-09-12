"""Lead validation rules engine."""
import re
import phonenumbers
from email_validator import validate_email, EmailNotValidError
from typing import Dict, List, Any, Optional
import pandas as pd
from fuzzywuzzy import fuzz
from .fraud_detection import FraudDetectionEngine


class LeadValidationRules:
    """Comprehensive lead validation rules engine."""
    
    def __init__(self, strict_mode: bool = True):
        self.strict_mode = strict_mode
        self.validation_results = {}
        self.fraud_engine = FraudDetectionEngine()
    
    def validate_lead(self, lead_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate a single lead and return comprehensive results.
        
        Args:
            lead_data: Dictionary containing lead information
            
        Returns:
            Dictionary with validation results and scores
        """
        results = {
            'lead_id': lead_data.get('Id', 'Unknown'),
            'validation_timestamp': pd.Timestamp.now(),
            'overall_score': 0.0,
            'data_quality_score': 0.0,
            'fraud_score': 0.0,
            'validation_details': {}
        }
        
        # Run individual validation checks
        results['validation_details']['email'] = self._validate_email(lead_data.get('Email'))
        results['validation_details']['phone'] = self._validate_phone(lead_data.get('Phone'))
        results['validation_details']['name'] = self._validate_name(lead_data.get('FirstName'), lead_data.get('LastName'))
        results['validation_details']['company'] = self._validate_company(lead_data.get('Company'))
        results['validation_details']['completeness'] = self._validate_completeness(lead_data)
        results['validation_details']['data_quality'] = self._validate_data_quality(lead_data)
        
        # Calculate separate scores
        results['data_quality_score'] = self._calculate_data_quality_score(results['validation_details'])
        
        # Calculate fraud score using dedicated engine
        fraud_analysis = self.fraud_engine.calculate_fraud_score(lead_data)
        results['fraud_score'] = fraud_analysis['fraud_score']
        results['validation_details']['fraud'] = fraud_analysis
        
        # Calculate overall score (combining both)
        results['overall_score'] = self._calculate_combined_score(
            results['data_quality_score'], 
            results['fraud_score']
        )
        results['validation_status'] = self._determine_validation_status(results['overall_score'])
        
        return results
    
    def _validate_email(self, email: Optional[str]) -> Dict[str, Any]:
        """Validate email address."""
        result = {
            'field': 'email',
            'value': email,
            'is_valid': False,
            'score': 0.0,
            'issues': []
        }
        
        if not email:
            result['issues'].append('Email is required')
            return result
        
        try:
            # Validate email format
            validated_email = validate_email(email)
            result['normalized_value'] = validated_email.email
            result['is_valid'] = True
            result['score'] = 1.0
            
            # Additional checks
            if email.count('@') != 1:
                result['issues'].append('Multiple @ symbols')
                result['score'] *= 0.5
            
            # Check for common typos in domains
            domain = email.split('@')[1].lower()
            common_domains = ['gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com']
            typo_suggestions = self._check_domain_typos(domain, common_domains)
            if typo_suggestions:
                result['issues'].append(f'Possible domain typo: {typo_suggestions}')
                result['score'] *= 0.8
                
        except EmailNotValidError as e:
            result['issues'].append(f'Invalid email format: {str(e)}')
            result['score'] = 0.0
        
        return result
    
    def _validate_phone(self, phone: Optional[str]) -> Dict[str, Any]:
        """Validate phone number."""
        result = {
            'field': 'phone',
            'value': phone,
            'is_valid': False,
            'score': 0.0,
            'issues': []
        }
        
        if not phone:
            result['issues'].append('Phone number is required')
            return result
        
        try:
            # Parse phone number
            parsed_phone = phonenumbers.parse(phone, "US")  # Default to US
            
            if phonenumbers.is_valid_number(parsed_phone):
                result['is_valid'] = True
                result['score'] = 1.0
                result['normalized_value'] = phonenumbers.format_number(parsed_phone, phonenumbers.PhoneNumberFormat.E164)
                result['formatted_national'] = phonenumbers.format_number(parsed_phone, phonenumbers.PhoneNumberFormat.NATIONAL)
                
                # Check if it's a mobile number
                number_type = phonenumbers.number_type(parsed_phone)
                if number_type == phonenumbers.PhoneNumberType.MOBILE:
                    result['is_mobile'] = True
                elif number_type == phonenumbers.PhoneNumberType.FIXED_LINE:
                    result['is_landline'] = True
                    
            else:
                result['issues'].append('Invalid phone number')
                result['score'] = 0.0
                
        except phonenumbers.NumberParseException as e:
            result['issues'].append(f'Phone parsing error: {str(e)}')
            result['score'] = 0.0
        
        return result
    
    def _validate_name(self, first_name: Optional[str], last_name: Optional[str]) -> Dict[str, Any]:
        """Validate name fields."""
        result = {
            'field': 'name',
            'first_name': first_name,
            'last_name': last_name,
            'is_valid': False,
            'score': 0.0,
            'issues': []
        }
        
        score_components = []
        
        # First name validation
        if not first_name or len(first_name.strip()) < 1:
            result['issues'].append('First name is required')
        else:
            if len(first_name.strip()) >= 2:
                score_components.append(0.5)
            else:
                result['issues'].append('First name too short')
                score_components.append(0.25)
        
        # Last name validation
        if not last_name or len(last_name.strip()) < 1:
            result['issues'].append('Last name is required')
        else:
            if len(last_name.strip()) >= 2:
                score_components.append(0.5)
            else:
                result['issues'].append('Last name too short')
                score_components.append(0.25)
        
        # Check for suspicious patterns
        if first_name and last_name:
            full_name = f"{first_name} {last_name}".lower()
            suspicious_patterns = ['test', 'unknown', 'n/a', 'null', 'admin']
            if any(pattern in full_name for pattern in suspicious_patterns):
                result['issues'].append('Suspicious name pattern detected')
                score_components = [s * 0.5 for s in score_components]
        
        result['score'] = sum(score_components)
        result['is_valid'] = result['score'] >= 0.8
        
        return result
    
    def _validate_company(self, company: Optional[str]) -> Dict[str, Any]:
        """Validate company field."""
        result = {
            'field': 'company',
            'value': company,
            'is_valid': False,
            'score': 0.0,
            'issues': []
        }
        
        if not company:
            result['issues'].append('Company name is required')
            return result
        
        company_clean = company.strip()
        
        if len(company_clean) < 2:
            result['issues'].append('Company name too short')
            result['score'] = 0.2
        elif len(company_clean) >= 2:
            result['score'] = 0.8
            result['is_valid'] = True
        
        # Check for suspicious patterns
        suspicious_patterns = ['test', 'unknown', 'n/a', 'null', 'none', 'company']
        if company_clean.lower() in suspicious_patterns:
            result['issues'].append('Suspicious company name')
            result['score'] *= 0.3
            result['is_valid'] = False
        
        return result
    
    def _validate_completeness(self, lead_data: Dict[str, Any]) -> Dict[str, Any]:
        """Validate lead data completeness."""
        from config.settings import REQUIRED_FIELDS, IMPORTANT_FIELDS
        
        result = {
            'field': 'completeness',
            'score': 0.0,
            'required_fields_filled': 0,
            'important_fields_filled': 0,
            'total_fields_filled': 0,
            'issues': []
        }
        
        # Check required fields
        required_filled = 0
        for field in REQUIRED_FIELDS:
            if lead_data.get(field) and str(lead_data.get(field)).strip():
                required_filled += 1
            else:
                result['issues'].append(f'Missing required field: {field}')
        
        result['required_fields_filled'] = required_filled
        required_score = required_filled / len(REQUIRED_FIELDS)
        
        # Check important fields
        important_filled = 0
        for field in IMPORTANT_FIELDS:
            if lead_data.get(field) and str(lead_data.get(field)).strip():
                important_filled += 1
        
        result['important_fields_filled'] = important_filled
        important_score = important_filled / len(IMPORTANT_FIELDS) if IMPORTANT_FIELDS else 0
        
        # Calculate total filled fields
        all_fields = set(REQUIRED_FIELDS + IMPORTANT_FIELDS)
        total_filled = sum(1 for field in all_fields if lead_data.get(field) and str(lead_data.get(field)).strip())
        result['total_fields_filled'] = total_filled
        
        # Weighted completeness score (70% required, 30% important)
        result['score'] = (required_score * 0.7) + (important_score * 0.3)
        
        return result
    
    def _validate_data_quality(self, lead_data: Dict[str, Any]) -> Dict[str, Any]:
        """Validate overall data quality indicators."""
        result = {
            'field': 'data_quality',
            'score': 1.0,
            'issues': []
        }
        
        quality_issues = 0
        total_checks = 0
        
        # Check for placeholder values
        placeholder_patterns = ['test', 'unknown', 'n/a', 'null', 'none', 'tbd', 'pending']
        for field, value in lead_data.items():
            if value and isinstance(value, str):
                total_checks += 1
                if value.lower().strip() in placeholder_patterns:
                    quality_issues += 1
                    result['issues'].append(f'Placeholder value in {field}: {value}')
        
        # Check for suspicious formatting
        for field, value in lead_data.items():
            if value and isinstance(value, str):
                # Check for excessive capitalization
                if value.isupper() and len(value) > 10:
                    result['issues'].append(f'All caps formatting in {field}')
                    quality_issues += 1
                
                # Check for excessive spacing
                if '  ' in value:  # Multiple spaces
                    result['issues'].append(f'Multiple spaces in {field}')
                    quality_issues += 1
        
        # Calculate quality score
        if total_checks > 0:
            quality_ratio = 1 - (quality_issues / total_checks)
            result['score'] = max(0.0, quality_ratio)
        
        return result
    
    def _check_domain_typos(self, domain: str, common_domains: List[str]) -> Optional[str]:
        """Check for common domain typos."""
        for common_domain in common_domains:
            similarity = fuzz.ratio(domain, common_domain)
            if 60 <= similarity < 90:  # Potential typo range
                return common_domain
        return None
    
    def _calculate_data_quality_score(self, validation_details: Dict[str, Any]) -> float:
        """Calculate weighted data quality score (excluding fraud indicators)."""
        weights = {
            'email': 0.30,      # Email validity and format
            'phone': 0.30,      # Phone validity and format  
            'name': 0.15,       # Name completeness and format
            'company': 0.10,    # Company field presence
            'completeness': 0.15 # Overall field completeness
        }
        
        total_score = 0.0
        total_weight = 0.0
        
        for field, weight in weights.items():
            if field in validation_details:
                score = validation_details[field].get('score', 0.0)
                total_score += score * weight
                total_weight += weight
        
        return total_score / total_weight if total_weight > 0 else 0.0
    
    def _calculate_combined_score(self, data_quality_score: float, fraud_score: float) -> float:
        """
        Calculate combined overall score.
        
        Args:
            data_quality_score: Score from 0-1 (higher = better quality)
            fraud_score: Score from 0-1 (higher = more fraudulent)
            
        Returns:
            Combined score from 0-1 (higher = better overall)
        """
        # Convert fraud score to quality score (invert it)
        fraud_quality_score = 1.0 - fraud_score
        
        # Weight: 70% data quality, 30% fraud prevention
        combined_score = (data_quality_score * 0.7) + (fraud_quality_score * 0.3)
        
        return combined_score
    
    def _determine_validation_status(self, overall_score: float) -> str:
        """Determine validation status based on overall score."""
        if overall_score >= 0.9:
            return 'Excellent'
        elif overall_score >= 0.8:
            return 'Good'
        elif overall_score >= 0.6:
            return 'Fair'
        elif overall_score >= 0.4:
            return 'Poor'
        else:
            return 'Invalid'
    
    def validate_batch(self, leads_df: pd.DataFrame) -> pd.DataFrame:
        """Validate a batch of leads and return results DataFrame."""
        validation_results = []
        
        for _, lead_row in leads_df.iterrows():
            lead_data = lead_row.to_dict()
            validation_result = self.validate_lead(lead_data)
            validation_results.append(validation_result)
        
        return pd.DataFrame(validation_results)
