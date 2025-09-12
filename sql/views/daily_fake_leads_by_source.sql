-- Daily Fake Leads by Lead Source Report
-- Shows fake leads per lead source for the current day (based on created_date)

CREATE OR REPLACE VIEW leads.daily_fake_leads_by_source AS
WITH today_leads AS (
    SELECT 
        COALESCE(lead_source, 'Unknown') as lead_source,
        COUNT(*) as total_leads_today,
        
        -- Fake lead counts
        COUNTIF(COALESCE(api_fake_lead, false) = true) as fake_leads_count,
        COUNTIF(COALESCE(api_fraud_score, 0) >= 8) as critical_fraud_count,
        COUNTIF(COALESCE(api_fraud_score, 0) >= 5) as high_fraud_count,
        
        -- Fake lead percentages
        ROUND((COUNTIF(COALESCE(api_fake_lead, false) = true)::DOUBLE / COUNT(*)) * 100, 2) as fake_leads_percentage,
        ROUND((COUNTIF(COALESCE(api_fraud_score, 0) >= 8)::DOUBLE / COUNT(*)) * 100, 2) as critical_fraud_percentage,
        ROUND((COUNTIF(COALESCE(api_fraud_score, 0) >= 5)::DOUBLE / COUNT(*)) * 100, 2) as high_fraud_percentage,
        
        -- Quality metrics for context
        AVG(COALESCE(api_quality_score, quality_score)) as avg_quality_score,
        AVG(COALESCE(api_fraud_score, 0)) as avg_fraud_score,
        
        -- Lead details for investigation
        STRING_AGG(
            CASE WHEN COALESCE(api_fake_lead, false) = true 
            THEN COALESCE(api_first_name, '') || ' ' || COALESCE(api_last_name, '') || ' (' || task_id || ')'
            ELSE NULL END, 
            '; '
        ) as fake_leads_list,
        
        -- Time context
        MIN(created_date) as earliest_lead_today,
        MAX(created_date) as latest_lead_today,
        CURRENT_DATE as report_date
        
    FROM parsed_validations
    WHERE parse_error IS NULL
    AND DATE_TRUNC('day', created_date) = CURRENT_DATE  -- Today only
    GROUP BY COALESCE(lead_source, 'Unknown')
),
source_rankings AS (
    SELECT 
        *,
        -- Risk level assessment for today
        CASE 
            WHEN fake_leads_percentage >= 50 THEN 'CRITICAL'
            WHEN fake_leads_percentage >= 20 THEN 'HIGH'
            WHEN fake_leads_percentage >= 10 THEN 'MEDIUM'
            WHEN fake_leads_percentage > 0 THEN 'LOW'
            ELSE 'CLEAN'
        END as daily_risk_level,
        
        -- Source performance ranking for today
        RANK() OVER (ORDER BY fake_leads_percentage DESC) as worst_source_rank,
        RANK() OVER (ORDER BY total_leads_today DESC) as volume_rank,
        RANK() OVER (ORDER BY avg_quality_score DESC) as quality_rank,
        
        -- Alert flags
        CASE WHEN fake_leads_count >= 3 THEN true ELSE false END as alert_volume,
        CASE WHEN fake_leads_percentage >= 25 THEN true ELSE false END as alert_percentage,
        CASE WHEN critical_fraud_count >= 1 THEN true ELSE false END as alert_critical_fraud
        
    FROM today_leads
)
SELECT 
    lead_source,
    total_leads_today,
    fake_leads_count,
    fake_leads_percentage,
    critical_fraud_count,
    high_fraud_count,
    daily_risk_level,
    
    -- Quality context
    ROUND(avg_quality_score, 1) as avg_quality_score,
    ROUND(avg_fraud_score, 1) as avg_fraud_score,
    
    -- Rankings
    worst_source_rank,
    volume_rank,
    quality_rank,
    
    -- Alert flags
    alert_volume,
    alert_percentage, 
    alert_critical_fraud,
    
    -- Investigation details
    fake_leads_list,
    
    -- Time context
    earliest_lead_today,
    latest_lead_today,
    report_date,
    
    -- Summary status for this source today
    CASE 
        WHEN alert_critical_fraud AND alert_percentage THEN '🚨 IMMEDIATE ACTION REQUIRED'
        WHEN alert_volume OR alert_percentage THEN '⚠️ MONITOR CLOSELY'
        WHEN fake_leads_count > 0 THEN '🔍 INVESTIGATE'
        ELSE '✅ CLEAN TODAY'
    END as daily_status,
    
    -- Recommended actions
    CASE 
        WHEN fake_leads_percentage >= 50 THEN 'PAUSE SOURCE - Review immediately'
        WHEN fake_leads_percentage >= 25 THEN 'QUARANTINE - Manual review all leads'
        WHEN fake_leads_count >= 3 THEN 'INVESTIGATE - Check lead quality processes'
        WHEN fake_leads_count >= 1 THEN 'MONITOR - Track patterns closely' 
        ELSE 'CONTINUE - Source performing well'
    END as recommended_action

FROM source_rankings
ORDER BY 
    fake_leads_percentage DESC, 
    fake_leads_count DESC, 
    total_leads_today DESC;
