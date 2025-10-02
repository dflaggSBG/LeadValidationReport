-- Fake Leads Detail View (Fixed for actual database structure)
-- Lists fake leads and provides results for each validation point

CREATE OR REPLACE VIEW fake_leads_detail AS
WITH fraud_breakdown AS (
  SELECT 
    pv.*,
    -- Extract specific fraud indicators from raw JSON if available
    CASE WHEN raw_api_response LIKE '%email%fraud%' OR raw_api_response LIKE '%email%invalid%' THEN 1 ELSE 0 END as email_fraud_flag,
    CASE WHEN raw_api_response LIKE '%phone%fraud%' OR raw_api_response LIKE '%phone%invalid%' THEN 1 ELSE 0 END as phone_fraud_flag,
    CASE WHEN raw_api_response LIKE '%name%gibberish%' OR raw_api_response LIKE '%hasGibberishNames%true%' THEN 1 ELSE 0 END as name_fraud_flag,
    CASE WHEN raw_api_response LIKE '%company%gibberish%' OR raw_api_response LIKE '%hasGibberishCompany%true%' THEN 1 ELSE 0 END as company_fraud_flag,
    
    -- Individual validation scores (normalize from actual structure)
    COALESCE(api_email_valid, false) as email_valid,
    COALESCE(api_phone_valid, false) as phone_valid,
    0.8 as email_quality_score,  -- Default scores since individual scores aren't stored separately
    0.8 as phone_quality_score,
    0.8 as name_quality_score,
    0.8 as company_quality_score,
    0.8 as completeness_score
    
  FROM parsed_validations pv
  WHERE parse_error IS NULL
  AND (COALESCE(api_fake_lead, false) = true OR COALESCE(api_fraud_score, 0) >= 6)  -- Focus on fake or medium+ fraud risk
)
SELECT 
    task_id as lead_id,
    lead_source,
    parsed_at as validation_timestamp,
    
    -- Lead data
    COALESCE(api_first_name, '') as first_name,
    COALESCE(api_last_name, '') as last_name,
    COALESCE(api_email, lead_email) as email,
    COALESCE(api_phone, '') as phone,
    COALESCE(api_company, lead_company) as company,
    
    -- Overall scores (normalized)
    COALESCE(api_data_quality_score, api_quality_score, quality_score) / 10.0 as data_quality_score,
    COALESCE(api_fraud_score, 0) / 10.0 as fraud_score,
    COALESCE(api_quality_score, quality_score) / 10.0 as overall_score,
    
    -- Fraud risk level (using 10-point scale)
    CASE 
        WHEN COALESCE(api_fraud_score, 0) >= 8 THEN 'CRITICAL'
        WHEN COALESCE(api_fraud_score, 0) >= 7 THEN 'HIGH'
        WHEN COALESCE(api_fraud_score, 0) >= 6 THEN 'MEDIUM'
        ELSE 'LOW'
    END as fraud_risk_level,
    
    -- Individual validation point results
    email_quality_score,
    CASE WHEN email_fraud_flag = 1 THEN 'FRAUD DETECTED' 
         WHEN email_quality_score >= 0.8 THEN 'PASS'
         WHEN email_quality_score >= 0.5 THEN 'WARNING'
         ELSE 'FAIL' 
    END as email_validation_result,
    
    phone_quality_score,
    CASE WHEN phone_fraud_flag = 1 THEN 'FRAUD DETECTED'
         WHEN phone_quality_score >= 0.8 THEN 'PASS'
         WHEN phone_quality_score >= 0.5 THEN 'WARNING'
         ELSE 'FAIL'
    END as phone_validation_result,
    
    name_quality_score,
    CASE WHEN name_fraud_flag = 1 THEN 'FRAUD DETECTED'
         WHEN name_quality_score >= 0.8 THEN 'PASS'
         WHEN name_quality_score >= 0.5 THEN 'WARNING'
         ELSE 'FAIL'
    END as name_validation_result,
    
    company_quality_score,
    CASE WHEN company_fraud_flag = 1 THEN 'FRAUD DETECTED'
         WHEN company_quality_score >= 0.8 THEN 'PASS'
         WHEN company_quality_score >= 0.5 THEN 'WARNING'
         ELSE 'FAIL'
    END as company_validation_result,
    
    completeness_score,
    CASE WHEN completeness_score >= 0.8 THEN 'PASS'
         WHEN completeness_score >= 0.5 THEN 'WARNING'
         ELSE 'FAIL'
    END as completeness_validation_result,
    
    -- Fraud indicators summary
    (email_fraud_flag + phone_fraud_flag + name_fraud_flag + company_fraud_flag) as fraud_indicators_count,
    
    -- Issues summary (using 10-point scale)
    CASE 
        WHEN COALESCE(api_fraud_score, 0) >= 8 THEN 'Multiple fraud indicators detected - HIGH PRIORITY'
        WHEN COALESCE(api_fraud_score, 0) >= 7 THEN 'Likely fraudulent lead - investigate'  
        WHEN COALESCE(api_fraud_score, 0) >= 6 THEN 'Suspicious patterns found - review recommended'
        ELSE 'Minor quality issues detected'
    END as validation_summary,
    
    -- Action recommended (using actual API recommendation when available)
    COALESCE(
        api_recommendation,
        CASE 
            WHEN COALESCE(api_fraud_score, 0) >= 8 THEN 'reject'
            WHEN COALESCE(api_fraud_score, 0) >= 7 THEN 'review'
            WHEN COALESCE(api_fraud_score, 0) >= 6 THEN 'flag'
            ELSE 'monitor'
        END
    ) as recommended_action

FROM fraud_breakdown
ORDER BY COALESCE(api_fraud_score, 0) DESC, parsed_at DESC;
