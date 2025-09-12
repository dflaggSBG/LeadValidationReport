-- Lead Validation by Source View
CREATE OR REPLACE VIEW leads.validation_by_source AS
SELECT 
  COALESCE(lead_source, 'Unknown') as lead_source,
  COUNT(*) as total_leads,
  AVG(overall_score) as avg_score,
  MEDIAN(overall_score) as median_score,
  MIN(overall_score) as min_score,
  MAX(overall_score) as max_score,
  
  -- Score distribution
  COUNTIF(overall_score >= 0.9) as excellent_count,
  COUNTIF(overall_score >= 0.8 AND overall_score < 0.9) as good_count,
  COUNTIF(overall_score >= 0.6 AND overall_score < 0.8) as fair_count,
  COUNTIF(overall_score >= 0.4 AND overall_score < 0.6) as poor_count,
  COUNTIF(overall_score < 0.4) as invalid_count,
  
  -- Score distribution percentages
  ROUND((COUNTIF(overall_score >= 0.9)::DOUBLE / COUNT(*)) * 100, 2) as excellent_percentage,
  ROUND((COUNTIF(overall_score >= 0.8 AND overall_score < 0.9)::DOUBLE / COUNT(*)) * 100, 2) as good_percentage,
  ROUND((COUNTIF(overall_score >= 0.6 AND overall_score < 0.8)::DOUBLE / COUNT(*)) * 100, 2) as fair_percentage,
  ROUND((COUNTIF(overall_score >= 0.4 AND overall_score < 0.6)::DOUBLE / COUNT(*)) * 100, 2) as poor_percentage,
  ROUND((COUNTIF(overall_score < 0.4)::DOUBLE / COUNT(*)) * 100, 2) as invalid_percentage,
  
  -- Quality indicator
  ROUND(((COUNTIF(overall_score >= 0.8))::DOUBLE / COUNT(*)) * 100, 2) as quality_percentage,
  
  -- Conversion metrics
  COUNTIF(is_converted) as converted_leads,
  ROUND((COUNTIF(is_converted)::DOUBLE / COUNT(*)) * 100, 2) as conversion_rate,
  
  -- Lead volume trend (last 30 days vs previous 30 days)
  COUNTIF(created_date >= CURRENT_DATE - INTERVAL '30 days') as leads_last_30_days,
  COUNTIF(created_date >= CURRENT_DATE - INTERVAL '60 days' AND created_date < CURRENT_DATE - INTERVAL '30 days') as leads_previous_30_days,
  
  -- Ranking metrics
  RANK() OVER (ORDER BY AVG(overall_score) DESC) as score_rank,
  RANK() OVER (ORDER BY COUNT(*) DESC) as volume_rank,
  RANK() OVER (ORDER BY COUNTIF(is_converted)::DOUBLE / COUNT(*) DESC) as conversion_rank

FROM leads.validation_summary
GROUP BY COALESCE(lead_source, 'Unknown')
ORDER BY avg_score DESC;
