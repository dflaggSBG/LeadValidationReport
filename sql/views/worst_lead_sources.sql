-- Worst Lead Sources View
-- Identifies the worst performing lead sources that need immediate attention

CREATE OR REPLACE VIEW worst_lead_sources AS
WITH source_problems AS (
  SELECT 
    lead_source,
    total_leads,
    avg_quality_score,
    avg_fraud_score,
    quality_leads_percentage,
    fake_leads_percentage,
    quality_grade,
    risk_level,
    quality_rank,
    
    -- Calculate composite problem score (higher = worse)
    -- Weight: Quality Score (40%), Fraud (30%), Fake Leads (20%), Volume Impact (10%)
    (
      (CASE WHEN avg_quality_score IS NULL THEN 10 ELSE (10 - avg_quality_score) END * 0.4) +
      (COALESCE(avg_fraud_score, 0) * 0.3) +
      (COALESCE(fake_leads_percentage, 0) * 0.02) +  -- Convert percentage to 0-2 scale
      (CASE WHEN total_leads > 100 THEN 2 WHEN total_leads > 50 THEN 1 ELSE 0 END * 0.1)  -- Volume impact
    ) as problem_score,
    
    -- Identify specific issues (DuckDB compatible)
    CASE 
      WHEN avg_quality_score < 4 THEN 'Very Low Quality Score (' || ROUND(avg_quality_score, 1) || ')'
      WHEN avg_fraud_score > 5 THEN 'High Fraud Score (' || ROUND(avg_fraud_score, 1) || ')'
      WHEN fake_leads_percentage > 25 THEN 'High Fake Lead Rate (' || ROUND(fake_leads_percentage, 1) || '%)'
      WHEN quality_leads_percentage < 30 THEN 'Low Quality Lead Rate (' || ROUND(quality_leads_percentage, 1) || '%)'
      ELSE 'Multiple Issues'
    END as primary_issue,
    
    -- Calculate potential impact if this source were improved
    CASE 
      WHEN total_leads > 100 THEN 'HIGH_IMPACT'
      WHEN total_leads > 50 THEN 'MEDIUM_IMPACT'
      ELSE 'LOW_IMPACT'
    END as improvement_impact
    
  FROM lead_source_quality_summary
  WHERE total_leads >= 10  -- Only consider sources with meaningful volume
)
SELECT 
  lead_source,
  total_leads,
  ROUND(avg_quality_score, 2) as avg_quality_score,
  ROUND(avg_fraud_score, 2) as avg_fraud_score,
  ROUND(quality_leads_percentage, 1) as quality_leads_percentage,
  ROUND(fake_leads_percentage, 1) as fake_leads_percentage,
  quality_grade,
  risk_level,
  improvement_impact,
  ROUND(problem_score, 2) as problem_score,
  primary_issue as primary_issues,
  
  -- Priority for remediation (1 = highest priority)
  ROW_NUMBER() OVER (ORDER BY problem_score DESC, total_leads DESC) as remediation_priority,
  
  -- Specific recommendations
  CASE 
    WHEN avg_quality_score < 3 THEN 'URGENT: Suspend source immediately'
    WHEN avg_quality_score < 5 AND fake_leads_percentage > 20 THEN 'HIGH: Review source criteria and validation'
    WHEN avg_quality_score < 6 THEN 'MEDIUM: Improve source quality controls'
    WHEN fake_leads_percentage > 15 THEN 'MEDIUM: Investigate fake lead patterns'
    ELSE 'LOW: Monitor closely'
  END as recommendation,
  
  -- Estimated monthly impact if not addressed (assuming 30-day period)
  ROUND((total_leads::DOUBLE / 30) * fake_leads_percentage * 0.01 * 30, 0) as estimated_monthly_fake_leads,
  
  -- Quality trend indicator (if we had historical data, this would show trend)
  'STABLE' as trend  -- Placeholder for future trend analysis
  
FROM source_problems
WHERE 
  -- Only show sources that have significant problems
  (avg_quality_score < 7 OR fake_leads_percentage > 10 OR avg_fraud_score > 3 OR risk_level IN ('HIGH_RISK', 'MEDIUM_RISK'))
  
ORDER BY problem_score DESC, total_leads DESC;
