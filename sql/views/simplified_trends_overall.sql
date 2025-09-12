-- Simplified Overall Trends Report
-- Shows Overall Data Quality Score over Time, Overall Fraud Score over Time, Each validation point over time

CREATE OR REPLACE VIEW leads.simplified_trends_overall AS
WITH daily_metrics AS (
    SELECT 
        DATE_TRUNC('day', validation_timestamp) as trend_date,
        COUNT(*) as total_validations,
        
        -- Overall scores
        AVG(data_quality_score) as avg_data_quality_score,
        MEDIAN(data_quality_score) as median_data_quality_score,
        AVG(fraud_score) as avg_fraud_score,
        MEDIAN(fraud_score) as median_fraud_score,
        AVG(overall_score) as avg_overall_score,
        
        -- Individual validation point averages
        AVG(COALESCE(JSON_EXTRACT_SCALAR(validation_details, '$.email.score'), '0')::DOUBLE) as avg_email_score,
        AVG(COALESCE(JSON_EXTRACT_SCALAR(validation_details, '$.phone.score'), '0')::DOUBLE) as avg_phone_score,
        AVG(COALESCE(JSON_EXTRACT_SCALAR(validation_details, '$.name.score'), '0')::DOUBLE) as avg_name_score,
        AVG(COALESCE(JSON_EXTRACT_SCALAR(validation_details, '$.company.score'), '0')::DOUBLE) as avg_company_score,
        AVG(COALESCE(JSON_EXTRACT_SCALAR(validation_details, '$.completeness.score'), '0')::DOUBLE) as avg_completeness_score,
        
        -- Pass rates for each validation point
        (COUNTIF(COALESCE(JSON_EXTRACT_SCALAR(validation_details, '$.email.score'), '0')::DOUBLE >= 0.7)::DOUBLE / COUNT(*)) * 100 as email_pass_rate,
        (COUNTIF(COALESCE(JSON_EXTRACT_SCALAR(validation_details, '$.phone.score'), '0')::DOUBLE >= 0.7)::DOUBLE / COUNT(*)) * 100 as phone_pass_rate,
        (COUNTIF(COALESCE(JSON_EXTRACT_SCALAR(validation_details, '$.name.score'), '0')::DOUBLE >= 0.7)::DOUBLE / COUNT(*)) * 100 as name_pass_rate,
        (COUNTIF(COALESCE(JSON_EXTRACT_SCALAR(validation_details, '$.company.score'), '0')::DOUBLE >= 0.7)::DOUBLE / COUNT(*)) * 100 as company_pass_rate,
        (COUNTIF(COALESCE(JSON_EXTRACT_SCALAR(validation_details, '$.completeness.score'), '0')::DOUBLE >= 0.7)::DOUBLE / COUNT(*)) * 100 as completeness_pass_rate,
        
        -- Quality categories
        COUNTIF(data_quality_score >= 0.8) as high_quality_count,
        COUNTIF(data_quality_score < 0.5) as poor_quality_count,
        
        -- Fraud risk categories  
        COUNTIF(fraud_score >= 0.6) as high_fraud_risk_count,
        COUNTIF(fraud_score < 0.2) as low_fraud_risk_count,
        
        -- Percentages
        (COUNTIF(data_quality_score >= 0.7)::DOUBLE / COUNT(*)) * 100 as high_quality_percentage,
        (COUNTIF(fraud_score >= 0.6)::DOUBLE / COUNT(*)) * 100 as high_fraud_risk_percentage
        
    FROM leads.validation_results
    WHERE validation_timestamp >= CURRENT_DATE - INTERVAL '90 days'
    GROUP BY DATE_TRUNC('day', validation_timestamp)
),
weekly_metrics AS (
    SELECT 
        DATE_TRUNC('week', validation_timestamp) as trend_date,
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
        (COUNTIF(fraud_score >= 0.6)::DOUBLE / COUNT(*)) * 100 as high_fraud_risk_percentage
        
    FROM leads.validation_results
    WHERE validation_timestamp >= CURRENT_DATE - INTERVAL '90 days'
    GROUP BY DATE_TRUNC('week', validation_timestamp)
)

-- Daily trends
SELECT 
    'daily' as period_type,
    trend_date,
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
    
    -- Overall quality metrics
    ROUND(high_quality_percentage, 1) as high_quality_percentage,
    ROUND(high_fraud_risk_percentage, 1) as high_fraud_risk_percentage,
    
    -- Trend indicators (day-over-day change)
    LAG(avg_data_quality_score) OVER (ORDER BY trend_date) as prev_data_quality_score,
    ROUND(avg_data_quality_score - LAG(avg_data_quality_score) OVER (ORDER BY trend_date), 4) as data_quality_change,
    LAG(avg_fraud_score) OVER (ORDER BY trend_date) as prev_fraud_score,  
    ROUND(avg_fraud_score - LAG(avg_fraud_score) OVER (ORDER BY trend_date), 4) as fraud_score_change

FROM daily_metrics

UNION ALL

-- Weekly trends  
SELECT 
    period_type,
    trend_date,
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
    LAG(avg_data_quality_score) OVER (ORDER BY trend_date) as prev_data_quality_score,
    ROUND(avg_data_quality_score - LAG(avg_data_quality_score) OVER (ORDER BY trend_date), 4) as data_quality_change,
    LAG(avg_fraud_score) OVER (ORDER BY trend_date) as prev_fraud_score,
    ROUND(avg_fraud_score - LAG(avg_fraud_score) OVER (ORDER BY trend_date), 4) as fraud_score_change
    
FROM weekly_metrics

ORDER BY period_type, trend_date DESC;
