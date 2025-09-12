-- Lead Validation Overview View
-- Executive summary of lead validation across all sources

CREATE OR REPLACE VIEW lead_validation_overview AS
WITH overall_metrics AS (
  SELECT
    COUNT(*) as total_validations,
    COUNT(DISTINCT lead_source) as total_sources,
    COUNT(DISTINCT COALESCE(api_first_name || ' ' || api_last_name, 'Unknown')) as unique_leads,
    
    -- Quality metrics
    AVG(COALESCE(api_quality_score, quality_score)) as avg_quality_score,
    AVG(COALESCE(api_lead_score, lead_score)) as avg_lead_score,
    AVG(COALESCE(api_fraud_score, fraud_score)) as avg_fraud_score,
    AVG(COALESCE(api_data_quality_score, data_quality)) as avg_data_quality_score,
    
    -- Quality distribution
    COUNTIF(COALESCE(api_quality_score, quality_score) >= 9) as excellent_leads,
    COUNTIF(COALESCE(api_quality_score, quality_score) BETWEEN 7 AND 8.99) as good_leads,
    COUNTIF(COALESCE(api_quality_score, quality_score) BETWEEN 5 AND 6.99) as fair_leads,
    COUNTIF(COALESCE(api_quality_score, quality_score) BETWEEN 3 AND 4.99) as poor_leads,
    COUNTIF(COALESCE(api_quality_score, quality_score) < 3) as invalid_leads,
    
    -- Problem indicators
    COUNTIF(COALESCE(api_fake_lead, false)) as total_fake_leads,
    COUNTIF(COALESCE(api_fake_phone, false)) as total_fake_phones,
    COUNTIF(COALESCE(api_disposable_email, false)) as total_disposable_emails,
    COUNTIF(COALESCE(api_bounce_likely, bounce_likely, false)) as total_bounce_likely,
    
    -- Validation success rates
    AVG(CASE WHEN COALESCE(api_email_valid, email_valid) THEN 1.0 ELSE 0.0 END) as email_validation_rate,
    AVG(CASE WHEN COALESCE(api_phone_valid, phone_valid) THEN 1.0 ELSE 0.0 END) as phone_validation_rate,
    
    -- Recommendations
    COUNTIF(UPPER(COALESCE(api_recommendation, recommendation)) = 'ACCEPT') as accept_count,
    COUNTIF(UPPER(COALESCE(api_recommendation, recommendation)) = 'REJECT') as reject_count,
    COUNTIF(UPPER(COALESCE(api_recommendation, recommendation)) = 'REVIEW') as review_count,
    
    -- Temporal data
    MIN(created_date) as earliest_validation,
    MAX(created_date) as latest_validation,
    COUNT(DISTINCT DATE_TRUNC('day', created_date)) as validation_days
    
  FROM parsed_validations
  WHERE parse_error IS NULL
),
source_quality AS (
  SELECT
    COUNT(CASE WHEN quality_grade IN ('A+', 'A') THEN 1 END) as high_quality_sources,
    COUNT(CASE WHEN quality_grade IN ('B', 'C') THEN 1 END) as medium_quality_sources,
    COUNT(CASE WHEN quality_grade IN ('D', 'F') THEN 1 END) as low_quality_sources,
    COUNT(CASE WHEN risk_level = 'HIGH_RISK' THEN 1 END) as high_risk_sources,
    COUNT(CASE WHEN risk_level = 'MEDIUM_RISK' THEN 1 END) as medium_risk_sources
  FROM lead_source_quality_summary
),
worst_performers AS (
  SELECT
    COUNT(*) as sources_needing_attention
  FROM worst_lead_sources
)
SELECT 
  -- Volume metrics
  om.total_validations,
  om.total_sources,
  om.unique_leads,
  ROUND(om.total_validations::DOUBLE / om.validation_days, 1) as avg_daily_validations,
  
  -- Quality scores (rounded)
  ROUND(om.avg_quality_score, 2) as avg_quality_score,
  ROUND(om.avg_lead_score, 2) as avg_lead_score,
  ROUND(om.avg_fraud_score, 2) as avg_fraud_score,
  ROUND(om.avg_data_quality_score, 2) as avg_data_quality_score,
  
  -- Quality distribution
  om.excellent_leads,
  ROUND((om.excellent_leads::DOUBLE / om.total_validations) * 100, 2) as excellent_percentage,
  om.good_leads,
  ROUND((om.good_leads::DOUBLE / om.total_validations) * 100, 2) as good_percentage,
  om.fair_leads,
  ROUND((om.fair_leads::DOUBLE / om.total_validations) * 100, 2) as fair_percentage,
  om.poor_leads,
  ROUND((om.poor_leads::DOUBLE / om.total_validations) * 100, 2) as poor_percentage,
  om.invalid_leads,
  ROUND((om.invalid_leads::DOUBLE / om.total_validations) * 100, 2) as invalid_percentage,
  
  -- Quality summary
  (om.excellent_leads + om.good_leads) as quality_leads,
  ROUND(((om.excellent_leads + om.good_leads)::DOUBLE / om.total_validations) * 100, 2) as quality_leads_percentage,
  
  -- Problem metrics
  om.total_fake_leads,
  ROUND((om.total_fake_leads::DOUBLE / om.total_validations) * 100, 2) as fake_leads_percentage,
  om.total_fake_phones,
  ROUND((om.total_fake_phones::DOUBLE / om.total_validations) * 100, 2) as fake_phones_percentage,
  om.total_disposable_emails,
  ROUND((om.total_disposable_emails::DOUBLE / om.total_validations) * 100, 2) as disposable_emails_percentage,
  om.total_bounce_likely,
  ROUND((om.total_bounce_likely::DOUBLE / om.total_validations) * 100, 2) as bounce_likely_percentage,
  
  -- Validation success rates
  ROUND(om.email_validation_rate * 100, 2) as email_validation_percentage,
  ROUND(om.phone_validation_rate * 100, 2) as phone_validation_percentage,
  
  -- Recommendation distribution
  om.accept_count,
  ROUND((om.accept_count::DOUBLE / om.total_validations) * 100, 2) as accept_percentage,
  om.reject_count,
  ROUND((om.reject_count::DOUBLE / om.total_validations) * 100, 2) as reject_percentage,
  om.review_count,
  ROUND((om.review_count::DOUBLE / om.total_validations) * 100, 2) as review_percentage,
  
  -- Source quality breakdown
  sq.high_quality_sources,
  sq.medium_quality_sources,
  sq.low_quality_sources,
  sq.high_risk_sources,
  sq.medium_risk_sources,
  wp.sources_needing_attention,
  
  -- Overall system health indicators
  CASE 
    WHEN om.avg_quality_score >= 8 AND (om.total_fake_leads::DOUBLE / om.total_validations) < 0.05 THEN 'EXCELLENT'
    WHEN om.avg_quality_score >= 7 AND (om.total_fake_leads::DOUBLE / om.total_validations) < 0.1 THEN 'GOOD'
    WHEN om.avg_quality_score >= 6 AND (om.total_fake_leads::DOUBLE / om.total_validations) < 0.15 THEN 'FAIR'
    WHEN om.avg_quality_score >= 5 AND (om.total_fake_leads::DOUBLE / om.total_validations) < 0.25 THEN 'POOR'
    ELSE 'CRITICAL'
  END as overall_health_status,
  
  -- Data freshness
  om.earliest_validation,
  om.latest_validation,
  om.validation_days,
  DATE_DIFF('day', om.latest_validation, CURRENT_DATE) as days_since_last_validation,
  
  -- Alert flags
  CASE WHEN sq.high_risk_sources > 0 THEN true ELSE false END as has_high_risk_sources,
  CASE WHEN (om.total_fake_leads::DOUBLE / om.total_validations) > 0.15 THEN true ELSE false END as high_fraud_alert,
  CASE WHEN om.avg_quality_score < 6 THEN true ELSE false END as low_quality_alert,
  CASE WHEN DATE_DIFF('day', om.latest_validation, CURRENT_DATE) > 7 THEN true ELSE false END as stale_data_alert

FROM overall_metrics om
CROSS JOIN source_quality sq  
CROSS JOIN worst_performers wp;
