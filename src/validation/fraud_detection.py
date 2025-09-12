"""Fraud detection rules and scoring for lead validation."""
import re
import pandas as pd
from typing import Dict, List, Any, Optional
from fuzzywuzzy import fuzz


class FraudDetectionEngine:
    """Dedicated fraud detection and scoring engine."""
    
    def __init__(self):
        self.fraud_indicators = {
            'fake_email_patterns': [
                r'test.*@.*',
                r'fake.*@.*',
                r'.*@test\.com',
                r'.*@fake\.com',
                r'.*@example\.com',
                r'.*@temp.*\.com',
                r'.*@throwaway.*',
                r'.*@guerrilla.*',
                r'.*@mailinator.*'
            ],
            'fake_phone_patterns': [
                '1234567890',
                '0000000000', 
                '1111111111',
                '5555555555',
                '8888888888',
                '9999999999'
            ],
            'fake_name_patterns': [
                'test', 'fake', 'john doe', 'jane doe', 'admin', 
                'user', 'sample', 'demo', 'example', 'unknown',
                'asdf', 'qwerty', 'temp', 'temporary'
            ],
            'suspicious_companies': [
                'test', 'fake', 'company', 'corp', 'inc', 'llc',
                'business', 'enterprise', 'solutions', 'services',
                'consulting', 'group', 'organization', 'n/a', 'none'
            ]
        }
    
    def calculate_fraud_score(self, lead_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculate fraud score for a single lead.
        
        Returns:
            Dictionary with fraud score (0-1, where 1 = highest fraud risk)
        """
        fraud_indicators = []
        fraud_score = 0.0
        
        # Email fraud checks
        email_fraud = self._check_email_fraud(lead_data.get('Email'))
        fraud_indicators.extend(email_fraud['indicators'])
        fraud_score += email_fraud['score'] * 0.3
        
        # Phone fraud checks  
        phone_fraud = self._check_phone_fraud(lead_data.get('Phone'))
        fraud_indicators.extend(phone_fraud['indicators'])
        fraud_score += phone_fraud['score'] * 0.25
        
        # Name fraud checks
        name_fraud = self._check_name_fraud(
            lead_data.get('FirstName'), 
            lead_data.get('LastName')
        )
        fraud_indicators.extend(name_fraud['indicators'])
        fraud_score += name_fraud['score'] * 0.2
        
        # Company fraud checks
        company_fraud = self._check_company_fraud(lead_data.get('Company'))
        fraud_indicators.extend(company_fraud['indicators'])
        fraud_score += company_fraud['score'] * 0.15
        
        # Pattern consistency checks
        consistency_fraud = self._check_pattern_consistency(lead_data)
        fraud_indicators.extend(consistency_fraud['indicators'])
        fraud_score += consistency_fraud['score'] * 0.1
        
        return {
            'fraud_score': min(fraud_score, 1.0),  # Cap at 1.0
            'fraud_indicators': fraud_indicators,
            'is_likely_fake': fraud_score >= 0.7,
            'risk_level': self._determine_risk_level(fraud_score)
        }
    
    def _check_email_fraud(self, email: Optional[str]) -> Dict[str, Any]:
        """Check for fraudulent email patterns."""
        result = {'score': 0.0, 'indicators': []}
        
        if not email:
            return result
            
        email_lower = email.lower()
        
        # Check against fake email patterns
        for pattern in self.fraud_indicators['fake_email_patterns']:
            if re.match(pattern, email_lower):
                result['score'] = 1.0
                result['indicators'].append(f'Fake email pattern: {email}')
                return result
        
        # Check for disposable email domains
        domain = email_lower.split('@')[1] if '@' in email_lower else ''
        disposable_domains = [
            '10minutemail', 'tempmail', 'throwaway', 'guerrillamail',
            'mailinator', 'yopmail', 'temp-mail'
        ]
        
        for disposable in disposable_domains:
            if disposable in domain:
                result['score'] = 0.9
                result['indicators'].append(f'Disposable email domain: {domain}')
                break
        
        # Check for suspicious patterns
        if re.search(r'\d{8,}', email_lower):  # 8+ consecutive digits
            result['score'] = max(result['score'], 0.6)
            result['indicators'].append('Email contains long number sequence')
        
        return result
    
    def _check_phone_fraud(self, phone: Optional[str]) -> Dict[str, Any]:
        """Check for fraudulent phone patterns."""
        result = {'score': 0.0, 'indicators': []}
        
        if not phone:
            return result
        
        # Clean phone number (remove formatting)
        phone_clean = re.sub(r'[^\d]', '', phone)
        
        # Check for fake phone patterns
        if phone_clean in self.fraud_indicators['fake_phone_patterns']:
            result['score'] = 1.0
            result['indicators'].append(f'Fake phone pattern: {phone}')
            return result
        
        # Check for repeated digits
        if len(set(phone_clean)) <= 2 and len(phone_clean) >= 10:
            result['score'] = 0.9
            result['indicators'].append('Phone has too few unique digits')
        
        # Check for sequential numbers
        if phone_clean in ['1234567890', '0123456789', '9876543210']:
            result['score'] = 0.95
            result['indicators'].append('Sequential phone number pattern')
        
        return result
    
    def _check_name_fraud(self, first_name: Optional[str], last_name: Optional[str]) -> Dict[str, Any]:
        """Check for fraudulent name patterns."""
        result = {'score': 0.0, 'indicators': []}
        
        if not first_name or not last_name:
            return result
        
        full_name = f"{first_name} {last_name}".lower()
        
        # Check against fake name patterns
        for fake_pattern in self.fraud_indicators['fake_name_patterns']:
            if fake_pattern in full_name:
                result['score'] = 0.8
                result['indicators'].append(f'Suspicious name pattern: {fake_pattern}')
                break
        
        # Check for identical first and last names
        if first_name.lower() == last_name.lower():
            result['score'] = max(result['score'], 0.7)
            result['indicators'].append('Identical first and last name')
        
        # Check for single character names
        if len(first_name.strip()) == 1 or len(last_name.strip()) == 1:
            result['score'] = max(result['score'], 0.5)
            result['indicators'].append('Single character name')
        
        return result
    
    def _check_company_fraud(self, company: Optional[str]) -> Dict[str, Any]:
        """Check for fraudulent company patterns."""
        result = {'score': 0.0, 'indicators': []}
        
        if not company:
            return result
        
        company_lower = company.lower().strip()
        
        # Check for generic/suspicious company names
        for suspicious in self.fraud_indicators['suspicious_companies']:
            if company_lower == suspicious:
                result['score'] = 0.6
                result['indicators'].append(f'Generic company name: {company}')
                break
        
        # Check for very short company names
        if len(company_lower) <= 2:
            result['score'] = max(result['score'], 0.4)
            result['indicators'].append('Company name too short')
        
        return result
    
    def _check_pattern_consistency(self, lead_data: Dict[str, Any]) -> Dict[str, Any]:
        """Check for suspicious patterns across fields."""
        result = {'score': 0.0, 'indicators': []}
        
        # Check if name appears in email
        email = lead_data.get('Email', '').lower()
        first_name = lead_data.get('FirstName', '').lower()
        last_name = lead_data.get('LastName', '').lower()
        
        if email and first_name and last_name:
            if first_name not in email and last_name not in email:
                # Name completely absent from email could be suspicious
                result['score'] = 0.3
                result['indicators'].append('Name not reflected in email address')
        
        return result
    
    def _determine_risk_level(self, fraud_score: float) -> str:
        """Determine risk level based on fraud score."""
        if fraud_score >= 0.8:
            return 'CRITICAL'
        elif fraud_score >= 0.6:
            return 'HIGH'
        elif fraud_score >= 0.4:
            return 'MEDIUM'
        elif fraud_score >= 0.2:
            return 'LOW'
        else:
            return 'MINIMAL'
    
    def batch_fraud_detection(self, leads_df: pd.DataFrame) -> pd.DataFrame:
        """Process fraud detection for multiple leads."""
        fraud_results = []
        
        for _, lead_row in leads_df.iterrows():
            lead_data = lead_row.to_dict()
            fraud_result = self.calculate_fraud_score(lead_data)
            fraud_result['lead_id'] = lead_data.get('Id', 'Unknown')
            fraud_results.append(fraud_result)
        
        return pd.DataFrame(fraud_results)
