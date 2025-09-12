-- Lead Validation Trends View
CREATE OR REPLACE VIEW leads.validation_trends AS
WITH daily_metrics AS (
  SELECT 
    DATE_TRUNC('day', validation_timestamp) as validation_date,
    COUNT(*) as leads_validated,
    AVG(overall_score) as avg_score,
    COUNTIF(overall_score >= 0.8) as quality_leads,
    COUNTIF(is_converted) as converted_leads,
    COUNT(DISTINCT lead_source) as unique_sources
  FROM leads.validation_results
  GROUP BY DATE_TRUNC('day', validation_timestamp)
),
weekly_metrics AS (
  SELECT 
    DATE_TRUNC('week', validation_timestamp) as validation_week,
    COUNT(*) as leads_validated,
    AVG(overall_score) as avg_score,
    COUNTIF(overall_score >= 0.8) as quality_leads,
    COUNTIF(is_converted) as converted_leads,
    COUNT(DISTINCT lead_source) as unique_sources
  FROM leads.validation_results
  GROUP BY DATE_TRUNC('week', validation_timestamp)
),
monthly_metrics AS (
  SELECT 
    DATE_TRUNC('month', validation_timestamp) as validation_month,
    COUNT(*) as leads_validated,
    AVG(overall_score) as avg_score,
    COUNTIF(overall_score >= 0.8) as quality_leads,
    COUNTIF(is_converted) as converted_leads,
    COUNT(DISTINCT lead_source) as unique_sources
  FROM leads.validation_results
  GROUP BY DATE_TRUNC('month', validation_timestamp)
)
SELECT 
  'daily' as period_type,
  validation_date as period_start,
  validation_date + INTERVAL '1 day' as period_end,
  leads_validated,
  ROUND(avg_score, 4) as avg_score,
  quality_leads,
  ROUND((quality_leads::DOUBLE / leads_validated) * 100, 2) as quality_percentage,
  converted_leads,
  ROUND((converted_leads::DOUBLE / leads_validated) * 100, 2) as conversion_rate,
  unique_sources,
  
  -- Trend indicators (compared to previous period)
  LAG(avg_score) OVER (ORDER BY validation_date) as prev_avg_score,
  ROUND(avg_score - LAG(avg_score) OVER (ORDER BY validation_date), 4) as score_change,
  LAG(leads_validated) OVER (ORDER BY validation_date) as prev_volume,
  leads_validated - LAG(leads_validated) OVER (ORDER BY validation_date) as volume_change
  
FROM daily_metrics

UNION ALL

SELECT 
  'weekly' as period_type,
  validation_week as period_start,
  validation_week + INTERVAL '1 week' as period_end,
  leads_validated,
  ROUND(avg_score, 4) as avg_score,
  quality_leads,
  ROUND((quality_leads::DOUBLE / leads_validated) * 100, 2) as quality_percentage,
  converted_leads,
  ROUND((converted_leads::DOUBLE / leads_validated) * 100, 2) as conversion_rate,
  unique_sources,
  
  -- Trend indicators
  LAG(avg_score) OVER (ORDER BY validation_week) as prev_avg_score,
  ROUND(avg_score - LAG(avg_score) OVER (ORDER BY validation_week), 4) as score_change,
  LAG(leads_validated) OVER (ORDER BY validation_week) as prev_volume,
  leads_validated - LAG(leads_validated) OVER (ORDER BY validation_week) as volume_change
  
FROM weekly_metrics

UNION ALL

SELECT 
  'monthly' as period_type,
  validation_month as period_start,
  validation_month + INTERVAL '1 month' as period_end,
  leads_validated,
  ROUND(avg_score, 4) as avg_score,
  quality_leads,
  ROUND((quality_leads::DOUBLE / leads_validated) * 100, 2) as quality_percentage,
  converted_leads,
  ROUND((converted_leads::DOUBLE / leads_validated) * 100, 2) as conversion_rate,
  unique_sources,
  
  -- Trend indicators
  LAG(avg_score) OVER (ORDER BY validation_month) as prev_avg_score,
  ROUND(avg_score - LAG(avg_score) OVER (ORDER BY validation_month), 4) as score_change,
  LAG(leads_validated) OVER (ORDER BY validation_month) as prev_volume,
  leads_validated - LAG(leads_validated) OVER (ORDER BY validation_month) as volume_change
  
FROM monthly_metrics

ORDER BY period_type, period_start DESC;
