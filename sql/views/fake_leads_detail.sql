-- Fake Leads Detail View
-- Lists fake leads and provides results for each validation point

CREATE OR REPLACE VIEW leads.fake_leads_detail AS
WITH fraud_breakdown AS (
  SELECT 
    vr.*,
    -- Extract specific fraud indicators from JSON validation details
    -- Note: These would be populated from the fraud_detection engine results
    CASE WHEN JSON_EXTRACT(validation_details, '$.fraud.fraud_indicators') LIKE '%email%' THEN 1 ELSE 0 END as email_fraud_flag,
    CASE WHEN JSON_EXTRACT(validation_details, '$.fraud.fraud_indicators') LIKE '%phone%' THEN 1 ELSE 0 END as phone_fraud_flag,
    CASE WHEN JSON_EXTRACT(validation_details, '$.fraud.fraud_indicators') LIKE '%name%' THEN 1 ELSE 0 END as name_fraud_flag,
    CASE WHEN JSON_EXTRACT(validation_details, '$.fraud.fraud_indicators') LIKE '%company%' THEN 1 ELSE 0 END as company_fraud_flag,
    
    -- Individual validation scores
    COALESCE(JSON_EXTRACT_SCALAR(validation_details, '$.email.score'), '0')::DOUBLE as email_quality_score,
    COALESCE(JSON_EXTRACT_SCALAR(validation_details, '$.phone.score'), '0')::DOUBLE as phone_quality_score,
    COALESCE(JSON_EXTRACT_SCALAR(validation_details, '$.name.score'), '0')::DOUBLE as name_quality_score,
    COALESCE(JSON_EXTRACT_SCALAR(validation_details, '$.company.score'), '0')::DOUBLE as company_quality_score,
    COALESCE(JSON_EXTRACT_SCALAR(validation_details, '$.completeness.score'), '0')::DOUBLE as completeness_score
    
  FROM leads.validation_results vr
  WHERE fraud_score >= 0.6  -- Focus on medium to high fraud risk
)
SELECT 
    lead_id,
    lead_source,
    validation_timestamp,
    
    -- Lead data
    first_name,
    last_name,
    email,
    phone,
    company,
    
    -- Overall scores
    data_quality_score,
    fraud_score,
    overall_score,
    
    -- Fraud risk level
    CASE 
        WHEN fraud_score >= 0.8 THEN 'CRITICAL'
        WHEN fraud_score >= 0.7 THEN 'HIGH'
        WHEN fraud_score >= 0.6 THEN 'MEDIUM'
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
    
    -- Issues summary
    CASE 
        WHEN fraud_score >= 0.8 THEN 'Multiple fraud indicators detected - HIGH PRIORITY'
        WHEN fraud_score >= 0.7 THEN 'Likely fraudulent lead - investigate'  
        WHEN fraud_score >= 0.6 THEN 'Suspicious patterns found - review recommended'
        ELSE 'Minor quality issues detected'
    END as validation_summary,
    
    -- Action recommended
    CASE 
        WHEN fraud_score >= 0.8 THEN 'REJECT - Do not contact'
        WHEN fraud_score >= 0.7 THEN 'QUARANTINE - Manual review required'
        WHEN fraud_score >= 0.6 THEN 'FLAG - Use caution in outreach'
        ELSE 'MONITOR - Track for patterns'
    END as recommended_action

FROM fraud_breakdown
ORDER BY fraud_score DESC, validation_timestamp DESC;
