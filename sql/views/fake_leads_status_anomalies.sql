-- Fake Leads Status Anomalies Detection
-- Identifies leads that were flagged as fake but later became Qualified/Converted
-- This helps detect false positives and process compliance issues

CREATE OR REPLACE VIEW leads.fake_leads_status_anomalies AS
WITH fake_lead_validations AS (
    -- Get all leads that were flagged as fake or high fraud risk
    SELECT 
        task_id,
        who_id as lead_id,
        COALESCE(api_first_name, '') as first_name,
        COALESCE(api_last_name, '') as last_name,
        COALESCE(api_email, lead_email) as email,
        COALESCE(api_phone, '') as phone,
        COALESCE(api_company, lead_company) as company,
        lead_source,
        created_date,
        parsed_at as validation_timestamp,
        
        -- Fraud indicators
        COALESCE(api_fake_lead, false) as was_flagged_fake,
        COALESCE(api_fraud_score, 0) as fraud_score,
        COALESCE(api_fraud_factors, 'No factors') as fraud_factors,
        COALESCE(api_quality_factors, 'No factors') as quality_factors,
        COALESCE(api_recommendation, 'unknown') as validation_recommendation,
        COALESCE(api_fraud_risk_level, 'unknown') as fraud_risk_level,
        
        -- Current status info (from when validation was done)
        -- Note: We need to join with current Salesforce data to get latest status
        raw_api_response
        
    FROM parsed_validations
    WHERE parse_error IS NULL
    AND (
        COALESCE(api_fake_lead, false) = true 
        OR COALESCE(api_fraud_score, 0) >= 7  -- High fraud threshold
        OR COALESCE(api_recommendation, '') = 'reject'
    )
),
current_salesforce_status AS (
    -- This would need to be populated by a fresh Salesforce query
    -- For now, we'll create a framework for this analysis
    SELECT 
        task_id,
        lead_id,
        first_name,
        last_name,
        email,
        phone,
        company,
        lead_source,
        created_date,
        validation_timestamp,
        was_flagged_fake,
        fraud_score,
        fraud_factors,
        quality_factors,
        validation_recommendation,
        fraud_risk_level,
        
        -- Extract current status from JSON if available
        COALESCE(
            JSON_EXTRACT_SCALAR(raw_api_response, '$.leadStatus'),
            JSON_EXTRACT_SCALAR(raw_api_response, '$.status'),
            'unknown'
        ) as status_at_validation_time,
        
        -- Anomaly detection flags
        CASE 
            WHEN was_flagged_fake = true THEN 'FLAGGED_AS_FAKE'
            WHEN fraud_score >= 8 THEN 'CRITICAL_FRAUD_RISK' 
            WHEN fraud_score >= 7 THEN 'HIGH_FRAUD_RISK'
            WHEN validation_recommendation = 'reject' THEN 'RECOMMENDED_REJECT'
            ELSE 'OTHER_ISSUE'
        END as validation_flag_type,
        
        -- Severity assessment
        CASE 
            WHEN was_flagged_fake = true AND fraud_score >= 8 THEN 'CRITICAL'
            WHEN was_flagged_fake = true OR fraud_score >= 7 THEN 'HIGH'
            WHEN fraud_score >= 5 THEN 'MEDIUM'
            ELSE 'LOW'
        END as anomaly_severity,
        
        -- Investigation priority
        CASE 
            WHEN was_flagged_fake = true THEN 1  -- Highest priority
            WHEN fraud_score >= 8 THEN 2
            WHEN validation_recommendation = 'reject' THEN 3
            ELSE 4
        END as investigation_priority,
        
        -- Time context
        DATE_TRUNC('day', created_date) as creation_date,
        DATE_TRUNC('day', validation_timestamp) as validation_date,
        CURRENT_DATE as report_date,
        
        -- Business impact assessment
        CASE 
            WHEN lead_source IN ('businessloans.com', 'Lending Tree', 'Fundera') THEN 'HIGH_VOLUME_SOURCE'
            ELSE 'STANDARD_SOURCE'
        END as source_impact_level
        
    FROM fake_lead_validations
)
SELECT 
    task_id,
    lead_id,
    first_name,
    last_name,
    email,
    phone,
    company,
    lead_source,
    creation_date,
    validation_date,
    
    -- Validation flags
    validation_flag_type,
    anomaly_severity,
    investigation_priority,
    
    -- Detailed validation info
    fraud_score,
    fraud_factors,
    quality_factors,
    validation_recommendation,
    fraud_risk_level,
    status_at_validation_time,
    
    -- Business context
    source_impact_level,
    
    -- Anomaly description
    CASE 
        WHEN validation_flag_type = 'FLAGGED_AS_FAKE' THEN 
            'Lead was explicitly flagged as fake but current status suggests otherwise'
        WHEN validation_flag_type = 'CRITICAL_FRAUD_RISK' THEN 
            'Lead had critical fraud score (8+/10) but current status suggests legitimate'
        WHEN validation_flag_type = 'HIGH_FRAUD_RISK' THEN 
            'Lead had high fraud score (7+/10) but current status suggests legitimate'
        WHEN validation_flag_type = 'RECOMMENDED_REJECT' THEN 
            'Validation system recommended rejection but lead progressed'
        ELSE 
            'Other validation concern with positive outcome'
    END as anomaly_description,
    
    -- Recommended actions
    CASE 
        WHEN anomaly_severity = 'CRITICAL' THEN 'IMMEDIATE REVIEW - Investigate validation accuracy'
        WHEN anomaly_severity = 'HIGH' THEN 'REVIEW REQUIRED - Check for false positive'
        WHEN anomaly_severity = 'MEDIUM' THEN 'MONITOR - Track validation performance'
        ELSE 'DOCUMENT - Note for validation tuning'
    END as recommended_action,
    
    -- Impact assessment
    CASE 
        WHEN validation_flag_type = 'FLAGGED_AS_FAKE' AND source_impact_level = 'HIGH_VOLUME_SOURCE' THEN 'HIGH_BUSINESS_IMPACT'
        WHEN validation_flag_type = 'FLAGGED_AS_FAKE' THEN 'MEDIUM_BUSINESS_IMPACT'
        WHEN anomaly_severity = 'CRITICAL' THEN 'MEDIUM_BUSINESS_IMPACT'
        ELSE 'LOW_BUSINESS_IMPACT'
    END as business_impact,
    
    report_date

FROM current_salesforce_status
ORDER BY 
    investigation_priority,
    anomaly_severity DESC,
    fraud_score DESC,
    validation_date DESC;
