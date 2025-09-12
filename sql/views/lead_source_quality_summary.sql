-- Lead Source Quality Summary View
-- Analyzes validation results by lead source to identify worst performing sources

CREATE OR REPLACE VIEW lead_source_quality_summary AS
WITH source_metrics AS (
  SELECT
    COALESCE(lead_source, 'Unknown') as lead_source,
    COUNT(*) as total_leads,
    
    -- Primary quality metrics (using API scores as authoritative)
    AVG(COALESCE(api_quality_score, quality_score)) as avg_quality_score,
    AVG(COALESCE(api_lead_score, lead_score)) as avg_lead_score,
    AVG(COALESCE(api_fraud_score, fraud_score)) as avg_fraud_score,
    AVG(COALESCE(api_data_quality_score, data_quality)) as avg_data_quality_score,
    
    -- Quality score distribution
    COUNTIF(COALESCE(api_quality_score, quality_score) >= 9) as excellent_count,
    COUNTIF(COALESCE(api_quality_score, quality_score) BETWEEN 7 AND 8.99) as good_count,
    COUNTIF(COALESCE(api_quality_score, quality_score) BETWEEN 5 AND 6.99) as fair_count,
    COUNTIF(COALESCE(api_quality_score, quality_score) BETWEEN 3 AND 4.99) as poor_count,
    COUNTIF(COALESCE(api_quality_score, quality_score) < 3) as invalid_count,
    
    -- Fraud and quality flags
    COUNTIF(COALESCE(api_fake_lead, false)) as fake_leads,
    COUNTIF(COALESCE(api_fake_phone, false)) as fake_phones,
    COUNTIF(COALESCE(api_disposable_email, false)) as disposable_emails,
    COUNTIF(COALESCE(api_bounce_likely, bounce_likely, false)) as bounce_likely_emails,
    COUNTIF(COALESCE(api_gibberish_email, false)) as gibberish_emails,
    
    -- Email validation metrics
    AVG(CASE WHEN COALESCE(api_email_valid, email_valid) THEN 1.0 ELSE 0.0 END) as email_valid_rate,
    AVG(CASE WHEN COALESCE(api_email_sendable, email_sendable) THEN 1.0 ELSE 0.0 END) as email_sendable_rate,
    
    -- Phone validation metrics
    AVG(CASE WHEN COALESCE(api_phone_valid, phone_valid) THEN 1.0 ELSE 0.0 END) as phone_valid_rate,
    
    -- Business opportunity metrics
    AVG(COALESCE(api_business_strength_score, 0)) as avg_business_strength,
    
    -- Recommendation distribution
    COUNTIF(UPPER(COALESCE(api_recommendation, recommendation)) = 'ACCEPT') as accept_recommendations,
    COUNTIF(UPPER(COALESCE(api_recommendation, recommendation)) = 'REJECT') as reject_recommendations,
    COUNTIF(UPPER(COALESCE(api_recommendation, recommendation)) = 'REVIEW') as review_recommendations,
    
    -- Temporal metrics
    MIN(created_date) as first_validation_date,
    MAX(created_date) as last_validation_date,
    COUNT(DISTINCT DATE_TRUNC('day', created_date)) as active_days
    
  FROM parsed_validations
  WHERE parse_error IS NULL  -- Exclude parsing errors
  GROUP BY COALESCE(lead_source, 'Unknown')
),
percentages AS (
  SELECT 
    *,
    -- Quality percentages
    ROUND((excellent_count::DOUBLE / total_leads) * 100, 2) as excellent_percentage,
    ROUND((good_count::DOUBLE / total_leads) * 100, 2) as good_percentage,
    ROUND((fair_count::DOUBLE / total_leads) * 100, 2) as fair_percentage,
    ROUND((poor_count::DOUBLE / total_leads) * 100, 2) as poor_percentage,
    ROUND((invalid_count::DOUBLE / total_leads) * 100, 2) as invalid_percentage,
    
    -- Quality leads percentage (Good + Excellent)
    ROUND(((excellent_count + good_count)::DOUBLE / total_leads) * 100, 2) as quality_leads_percentage,
    
    -- Problem percentages
    ROUND((fake_leads::DOUBLE / total_leads) * 100, 2) as fake_leads_percentage,
    ROUND((fake_phones::DOUBLE / total_leads) * 100, 2) as fake_phones_percentage,
    ROUND((disposable_emails::DOUBLE / total_leads) * 100, 2) as disposable_emails_percentage,
    ROUND((bounce_likely_emails::DOUBLE / total_leads) * 100, 2) as bounce_likely_percentage,
    
    -- Recommendation percentages
    ROUND((accept_recommendations::DOUBLE / total_leads) * 100, 2) as accept_percentage,
    ROUND((reject_recommendations::DOUBLE / total_leads) * 100, 2) as reject_percentage,
    ROUND((review_recommendations::DOUBLE / total_leads) * 100, 2) as review_percentage,
    
    -- Validation percentages
    ROUND(email_valid_rate * 100, 2) as email_valid_percentage,
    ROUND(email_sendable_rate * 100, 2) as email_sendable_percentage,
    ROUND(phone_valid_rate * 100, 2) as phone_valid_percentage
    
  FROM source_metrics
)
SELECT 
  lead_source,
  total_leads,
  
  -- Quality scores (rounded for readability)
  ROUND(avg_quality_score, 2) as avg_quality_score,
  ROUND(avg_lead_score, 2) as avg_lead_score,  
  ROUND(avg_fraud_score, 2) as avg_fraud_score,
  ROUND(avg_data_quality_score, 2) as avg_data_quality_score,
  ROUND(avg_business_strength, 2) as avg_business_strength,
  
  -- Quality distribution
  excellent_count,
  excellent_percentage,
  good_count, 
  good_percentage,
  fair_count,
  fair_percentage,
  poor_count,
  poor_percentage,
  invalid_count,
  invalid_percentage,
  quality_leads_percentage,
  
  -- Problem indicators
  fake_leads,
  fake_leads_percentage,
  fake_phones,
  fake_phones_percentage,
  disposable_emails,
  disposable_emails_percentage,
  bounce_likely_emails,
  bounce_likely_percentage,
  gibberish_emails,
  
  -- Validation rates
  email_valid_percentage,
  email_sendable_percentage,
  phone_valid_percentage,
  
  -- Recommendation distribution
  accept_recommendations,
  accept_percentage,
  reject_recommendations,
  reject_percentage,
  review_recommendations,
  review_percentage,
  
  -- Temporal data
  first_validation_date,
  last_validation_date,
  active_days,
  
  -- Quality ranking (1 = best, higher = worse)
  RANK() OVER (ORDER BY avg_quality_score DESC) as quality_rank,
  RANK() OVER (ORDER BY total_leads DESC) as volume_rank,
  RANK() OVER (ORDER BY fake_leads_percentage ASC, avg_fraud_score ASC) as fraud_rank,
  
  -- Overall quality grade
  CASE 
    WHEN avg_quality_score >= 8.5 AND fake_leads_percentage <= 5 THEN 'A+'
    WHEN avg_quality_score >= 7.5 AND fake_leads_percentage <= 10 THEN 'A'
    WHEN avg_quality_score >= 6.5 AND fake_leads_percentage <= 15 THEN 'B'
    WHEN avg_quality_score >= 5.5 AND fake_leads_percentage <= 25 THEN 'C'
    WHEN avg_quality_score >= 4.0 AND fake_leads_percentage <= 35 THEN 'D'
    ELSE 'F'
  END as quality_grade,
  
  -- Red flags (sources that need immediate attention)
  CASE 
    WHEN avg_quality_score < 4 OR fake_leads_percentage > 25 OR avg_fraud_score > 5 THEN 'HIGH_RISK'
    WHEN avg_quality_score < 6 OR fake_leads_percentage > 15 OR avg_fraud_score > 3 THEN 'MEDIUM_RISK'
    ELSE 'LOW_RISK'
  END as risk_level

FROM percentages
ORDER BY avg_quality_score DESC;
