-- Simplified Trends Report by Lead Source  
-- Shows Overall Data Quality Score over Time, Overall Fraud Score over Time, Each validation point over time - all by Lead Source

CREATE OR REPLACE VIEW leads.simplified_trends_by_source AS
WITH daily_source_metrics AS (
    SELECT 
        DATE_TRUNC('day', validation_timestamp) as trend_date,
        COALESCE(lead_source, 'Unknown') as lead_source,
        COUNT(*) as total_validations,
        
        -- Overall scores by source
        AVG(data_quality_score) as avg_data_quality_score,
        MEDIAN(data_quality_score) as median_data_quality_score,
        AVG(fraud_score) as avg_fraud_score,
        MEDIAN(fraud_score) as median_fraud_score,
        AVG(overall_score) as avg_overall_score,
        
        -- Individual validation point averages by source
        AVG(COALESCE(JSON_EXTRACT_SCALAR(validation_details, '$.email.score'), '0')::DOUBLE) as avg_email_score,
        AVG(COALESCE(JSON_EXTRACT_SCALAR(validation_details, '$.phone.score'), '0')::DOUBLE) as avg_phone_score,
        AVG(COALESCE(JSON_EXTRACT_SCALAR(validation_details, '$.name.score'), '0')::DOUBLE) as avg_name_score,
        AVG(COALESCE(JSON_EXTRACT_SCALAR(validation_details, '$.company.score'), '0')::DOUBLE) as avg_company_score,
        AVG(COALESCE(JSON_EXTRACT_SCALAR(validation_details, '$.completeness.score'), '0')::DOUBLE) as avg_completeness_score,
        
        -- Pass rates for each validation point by source
        (COUNTIF(COALESCE(JSON_EXTRACT_SCALAR(validation_details, '$.email.score'), '0')::DOUBLE >= 0.7)::DOUBLE / COUNT(*)) * 100 as email_pass_rate,
        (COUNTIF(COALESCE(JSON_EXTRACT_SCALAR(validation_details, '$.phone.score'), '0')::DOUBLE >= 0.7)::DOUBLE / COUNT(*)) * 100 as phone_pass_rate,
        (COUNTIF(COALESCE(JSON_EXTRACT_SCALAR(validation_details, '$.name.score'), '0')::DOUBLE >= 0.7)::DOUBLE / COUNT(*)) * 100 as name_pass_rate,
        (COUNTIF(COALESCE(JSON_EXTRACT_SCALAR(validation_details, '$.company.score'), '0')::DOUBLE >= 0.7)::DOUBLE / COUNT(*)) * 100 as company_pass_rate,
        (COUNTIF(COALESCE(JSON_EXTRACT_SCALAR(validation_details, '$.completeness.score'), '0')::DOUBLE >= 0.7)::DOUBLE / COUNT(*)) * 100 as completeness_pass_rate,
        
        -- Quality and fraud metrics by source
        (COUNTIF(data_quality_score >= 0.7)::DOUBLE / COUNT(*)) * 100 as high_quality_percentage,
        (COUNTIF(fraud_score >= 0.6)::DOUBLE / COUNT(*)) * 100 as high_fraud_risk_percentage,
        COUNTIF(fraud_score >= 0.7) as likely_fake_count
        
    FROM leads.validation_results
    WHERE validation_timestamp >= CURRENT_DATE - INTERVAL '90 days'
    GROUP BY DATE_TRUNC('day', validation_timestamp), COALESCE(lead_source, 'Unknown')
),
weekly_source_metrics AS (
    SELECT 
        DATE_TRUNC('week', validation_timestamp) as trend_date,
        COALESCE(lead_source, 'Unknown') as lead_source,
        'weekly' as period_type,
        COUNT(*) as total_validations,
        
        AVG(data_quality_score) as avg_data_quality_score,
        MEDIAN(data_quality_score) as median_data_quality_score,
        AVG(fraud_score) as avg_fraud_score,
        MEDIAN(fraud_score) as median_fraud_score,
        AVG(overall_score) as avg_overall_score,
        
        AVG(COALESCE(JSON_EXTRACT_SCALAR(validation_details, '$.email.score'), '0')::DOUBLE) as avg_email_score,
        AVG(COALESCE(JSON_EXTRACT_SCALAR(validation_details, '$.phone.score'), '0')::DOUBLE) as avg_phone_score,
        AVG(COALESCE(JSON_EXTRACT_SCALAR(validation_details, '$.name.score'), '0')::DOUBLE) as avg_name_score,
        AVG(COALESCE(JSON_EXTRACT_SCALAR(validation_details, '$.company.score'), '0')::DOUBLE) as avg_company_score,
        AVG(COALESCE(JSON_EXTRACT_SCALAR(validation_details, '$.completeness.score'), '0')::DOUBLE) as avg_completeness_score,
        
        (COUNTIF(COALESCE(JSON_EXTRACT_SCALAR(validation_details, '$.email.score'), '0')::DOUBLE >= 0.7)::DOUBLE / COUNT(*)) * 100 as email_pass_rate,
        (COUNTIF(COALESCE(JSON_EXTRACT_SCALAR(validation_details, '$.phone.score'), '0')::DOUBLE >= 0.7)::DOUBLE / COUNT(*)) * 100 as phone_pass_rate,
        (COUNTIF(COALESCE(JSON_EXTRACT_SCALAR(validation_details, '$.name.score'), '0')::DOUBLE >= 0.7)::DOUBLE / COUNT(*)) * 100 as name_pass_rate,
        (COUNTIF(COALESCE(JSON_EXTRACT_SCALAR(validation_details, '$.company.score'), '0')::DOUBLE >= 0.7)::DOUBLE / COUNT(*)) * 100 as company_pass_rate,
        (COUNTIF(COALESCE(JSON_EXTRACT_SCALAR(validation_details, '$.completeness.score'), '0')::DOUBLE >= 0.7)::DOUBLE / COUNT(*)) * 100 as completeness_pass_rate,
        
        (COUNTIF(data_quality_score >= 0.7)::DOUBLE / COUNT(*)) * 100 as high_quality_percentage,
        (COUNTIF(fraud_score >= 0.6)::DOUBLE / COUNT(*)) * 100 as high_fraud_risk_percentage,
        COUNTIF(fraud_score >= 0.7) as likely_fake_count
        
    FROM leads.validation_results
    WHERE validation_timestamp >= CURRENT_DATE - INTERVAL '90 days'
    GROUP BY DATE_TRUNC('week', validation_timestamp), COALESCE(lead_source, 'Unknown')
)

-- Daily trends by source
SELECT 
    'daily' as period_type,
    trend_date,
    lead_source,
    total_validations,
    
    -- Round to 3 decimal places for readability
    ROUND(avg_data_quality_score, 3) as avg_data_quality_score,
    ROUND(median_data_quality_score, 3) as median_data_quality_score,
    ROUND(avg_fraud_score, 3) as avg_fraud_score,
    ROUND(median_fraud_score, 3) as median_fraud_score,
    ROUND(avg_overall_score, 3) as avg_overall_score,
    
    -- Individual validation point scores
    ROUND(avg_email_score, 3) as avg_email_score,
    ROUND(avg_phone_score, 3) as avg_phone_score,
    ROUND(avg_name_score, 3) as avg_name_score,
    ROUND(avg_company_score, 3) as avg_company_score,
    ROUND(avg_completeness_score, 3) as avg_completeness_score,
    
    -- Pass rates
    ROUND(email_pass_rate, 1) as email_pass_rate_percent,
    ROUND(phone_pass_rate, 1) as phone_pass_rate_percent,
    ROUND(name_pass_rate, 1) as name_pass_rate_percent,
    ROUND(company_pass_rate, 1) as company_pass_rate_percent,
    ROUND(completeness_pass_rate, 1) as completeness_pass_rate_percent,
    
    -- Quality metrics
    ROUND(high_quality_percentage, 1) as high_quality_percentage,
    ROUND(high_fraud_risk_percentage, 1) as high_fraud_risk_percentage,
    likely_fake_count,
    
    -- Source performance trend indicators (day-over-day change)
    LAG(avg_data_quality_score) OVER (PARTITION BY lead_source ORDER BY trend_date) as prev_data_quality_score,
    ROUND(avg_data_quality_score - LAG(avg_data_quality_score) OVER (PARTITION BY lead_source ORDER BY trend_date), 4) as data_quality_change,
    LAG(avg_fraud_score) OVER (PARTITION BY lead_source ORDER BY trend_date) as prev_fraud_score,
    ROUND(avg_fraud_score - LAG(avg_fraud_score) OVER (PARTITION BY lead_source ORDER BY trend_date), 4) as fraud_score_change,
    LAG(total_validations) OVER (PARTITION BY lead_source ORDER BY trend_date) as prev_volume,
    total_validations - LAG(total_validations) OVER (PARTITION BY lead_source ORDER BY trend_date) as volume_change,
    
    -- Source rank for the day
    RANK() OVER (PARTITION BY trend_date ORDER BY avg_data_quality_score DESC) as daily_quality_rank,
    RANK() OVER (PARTITION BY trend_date ORDER BY avg_fraud_score ASC) as daily_fraud_safety_rank

FROM daily_source_metrics
WHERE total_validations >= 5  -- Only show sources with meaningful daily volume

UNION ALL

-- Weekly trends by source
SELECT 
    period_type,
    trend_date,
    lead_source,
    total_validations,
    ROUND(avg_data_quality_score, 3) as avg_data_quality_score,
    ROUND(median_data_quality_score, 3) as median_data_quality_score,
    ROUND(avg_fraud_score, 3) as avg_fraud_score,
    ROUND(median_fraud_score, 3) as median_fraud_score,
    ROUND(avg_overall_score, 3) as avg_overall_score,
    ROUND(avg_email_score, 3) as avg_email_score,
    ROUND(avg_phone_score, 3) as avg_phone_score,
    ROUND(avg_name_score, 3) as avg_name_score,
    ROUND(avg_company_score, 3) as avg_company_score,
    ROUND(avg_completeness_score, 3) as avg_completeness_score,
    ROUND(email_pass_rate, 1) as email_pass_rate_percent,
    ROUND(phone_pass_rate, 1) as phone_pass_rate_percent,
    ROUND(name_pass_rate, 1) as name_pass_rate_percent,
    ROUND(company_pass_rate, 1) as company_pass_rate_percent,
    ROUND(completeness_pass_rate, 1) as completeness_pass_rate_percent,
    ROUND(high_quality_percentage, 1) as high_quality_percentage,
    ROUND(high_fraud_risk_percentage, 1) as high_fraud_risk_percentage,
    likely_fake_count,
    LAG(avg_data_quality_score) OVER (PARTITION BY lead_source ORDER BY trend_date) as prev_data_quality_score,
    ROUND(avg_data_quality_score - LAG(avg_data_quality_score) OVER (PARTITION BY lead_source ORDER BY trend_date), 4) as data_quality_change,
    LAG(avg_fraud_score) OVER (PARTITION BY lead_source ORDER BY trend_date) as prev_fraud_score,
    ROUND(avg_fraud_score - LAG(avg_fraud_score) OVER (PARTITION BY lead_source ORDER BY trend_date), 4) as fraud_score_change,
    LAG(total_validations) OVER (PARTITION BY lead_source ORDER BY trend_date) as prev_volume,
    total_validations - LAG(total_validations) OVER (PARTITION BY lead_source ORDER BY trend_date) as volume_change,
    RANK() OVER (PARTITION BY trend_date ORDER BY avg_data_quality_score DESC) as daily_quality_rank,
    RANK() OVER (PARTITION BY trend_date ORDER BY avg_fraud_score ASC) as daily_fraud_safety_rank
    
FROM weekly_source_metrics
WHERE total_validations >= 10  -- Only show sources with meaningful weekly volume

ORDER BY period_type, trend_date DESC, lead_source;
