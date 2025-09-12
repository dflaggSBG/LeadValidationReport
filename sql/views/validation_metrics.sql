-- Lead Validation Metrics View
CREATE OR REPLACE VIEW leads.validation_metrics AS
WITH base_metrics AS (
  SELECT
    COUNT(*) as total_leads,
    AVG(overall_score) as avg_overall_score,
    MEDIAN(overall_score) as median_overall_score,
    
    -- Score distribution
    COUNTIF(overall_score >= 0.9) as excellent_count,
    COUNTIF(overall_score >= 0.8 AND overall_score < 0.9) as good_count,
    COUNTIF(overall_score >= 0.6 AND overall_score < 0.8) as fair_count,
    COUNTIF(overall_score >= 0.4 AND overall_score < 0.6) as poor_count,
    COUNTIF(overall_score < 0.4) as invalid_count,
    
    -- Validation status distribution
    COUNTIF(validation_status = 'Excellent') as status_excellent,
    COUNTIF(validation_status = 'Good') as status_good,
    COUNTIF(validation_status = 'Fair') as status_fair,
    COUNTIF(validation_status = 'Poor') as status_poor,
    COUNTIF(validation_status = 'Invalid') as status_invalid,
    
    -- Conversion metrics
    COUNTIF(is_converted) as converted_leads,
    COUNTIF(NOT is_converted) as not_converted_leads,
    
    -- Lead source diversity
    COUNT(DISTINCT lead_source) as unique_lead_sources,
    
    -- Temporal metrics
    MIN(created_date) as earliest_lead_date,
    MAX(created_date) as latest_lead_date,
    AVG(lead_age_days) as avg_lead_age_days,
    
    -- Validation freshness
    MIN(validation_timestamp) as earliest_validation,
    MAX(validation_timestamp) as latest_validation,
    AVG(days_since_validation) as avg_days_since_validation
    
  FROM leads.validation_summary
),
percentage_metrics AS (
  SELECT 
    *,
    -- Score distribution percentages
    ROUND((excellent_count::DOUBLE / total_leads) * 100, 2) as excellent_percentage,
    ROUND((good_count::DOUBLE / total_leads) * 100, 2) as good_percentage,
    ROUND((fair_count::DOUBLE / total_leads) * 100, 2) as fair_percentage,
    ROUND((poor_count::DOUBLE / total_leads) * 100, 2) as poor_percentage,
    ROUND((invalid_count::DOUBLE / total_leads) * 100, 2) as invalid_percentage,
    
    -- Conversion rate
    ROUND((converted_leads::DOUBLE / total_leads) * 100, 2) as conversion_rate_percentage,
    
    -- Quality score (percentage of leads with good or excellent scores)
    ROUND(((excellent_count + good_count)::DOUBLE / total_leads) * 100, 2) as quality_leads_percentage
    
  FROM base_metrics
)
SELECT 
  total_leads,
  avg_overall_score,
  median_overall_score,
  
  -- Score counts and percentages
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
  
  -- Conversion metrics
  converted_leads,
  not_converted_leads,
  conversion_rate_percentage,
  
  -- Quality metrics
  quality_leads_percentage,
  unique_lead_sources,
  
  -- Temporal metrics
  earliest_lead_date,
  latest_lead_date,
  avg_lead_age_days,
  earliest_validation,
  latest_validation,
  avg_days_since_validation,
  
  -- Data freshness indicator
  CASE 
    WHEN avg_days_since_validation <= 1 THEN 'Very Fresh'
    WHEN avg_days_since_validation <= 7 THEN 'Fresh'
    WHEN avg_days_since_validation <= 30 THEN 'Moderate'
    ELSE 'Stale'
  END as data_freshness_status
  
FROM percentage_metrics;
