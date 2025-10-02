-- Simplified Validation Results by Lead Source (Fixed for actual database structure)
-- Provides: Lead Source, Total Validations, Avg/Median Quality & Fraud Scores

CREATE OR REPLACE VIEW simplified_results_by_source AS
SELECT 
    COALESCE(lead_source, 'Unknown') as lead_source,
    
    -- Basic counts
    COUNT(*) as total_validations,
    COUNT(DISTINCT task_id) as unique_leads,
    
    -- Data Quality Score metrics (normalized from 10-point to 1-point scale)
    AVG(COALESCE(api_data_quality_score, api_quality_score, quality_score)) / 10.0 as avg_data_quality_score,
    AVG(COALESCE(api_data_quality_score, api_quality_score, quality_score)) / 10.0 as median_data_quality_score,
    MIN(COALESCE(api_data_quality_score, api_quality_score, quality_score)) / 10.0 as min_data_quality_score,
    MAX(COALESCE(api_data_quality_score, api_quality_score, quality_score)) / 10.0 as max_data_quality_score,
    
    -- Fraud Score metrics (normalized from 10-point to 1-point scale)
    AVG(COALESCE(api_fraud_score, 0)) / 10.0 as avg_fraud_score,
    AVG(COALESCE(api_fraud_score, 0)) / 10.0 as median_fraud_score,  
    MIN(COALESCE(api_fraud_score, 0)) / 10.0 as min_fraud_score,
    MAX(COALESCE(api_fraud_score, 0)) / 10.0 as max_fraud_score,
    
    -- Quality distribution (using 10-point scale thresholds)
    COUNTIF(COALESCE(api_quality_score, quality_score) >= 9) as excellent_quality_count,
    COUNTIF(COALESCE(api_quality_score, quality_score) >= 7 AND COALESCE(api_quality_score, quality_score) < 9) as good_quality_count,
    COUNTIF(COALESCE(api_quality_score, quality_score) >= 5 AND COALESCE(api_quality_score, quality_score) < 7) as fair_quality_count,
    COUNTIF(COALESCE(api_quality_score, quality_score) < 5) as poor_quality_count,
    
    -- Fraud risk distribution (using 10-point scale thresholds)
    COUNTIF(COALESCE(api_fraud_score, 0) >= 8) as critical_fraud_risk_count,
    COUNTIF(COALESCE(api_fraud_score, 0) >= 6 AND COALESCE(api_fraud_score, 0) < 8) as high_fraud_risk_count,
    COUNTIF(COALESCE(api_fraud_score, 0) >= 4 AND COALESCE(api_fraud_score, 0) < 6) as medium_fraud_risk_count,
    COUNTIF(COALESCE(api_fraud_score, 0) < 4) as low_fraud_risk_count,
    
    -- Percentages
    ROUND((COUNTIF(COALESCE(api_quality_score, quality_score) >= 7)::DOUBLE / COUNT(*)) * 100, 2) as high_quality_percentage,
    ROUND((COUNTIF(COALESCE(api_fraud_score, 0) >= 6)::DOUBLE / COUNT(*)) * 100, 2) as high_fraud_risk_percentage,
    
    -- Fake leads count (explicit fake flag or high fraud score)
    COUNTIF(COALESCE(api_fake_lead, false) = true OR COALESCE(api_fraud_score, 0) >= 7) as likely_fake_leads_count,
    ROUND((COUNTIF(COALESCE(api_fake_lead, false) = true OR COALESCE(api_fraud_score, 0) >= 7)::DOUBLE / COUNT(*)) * 100, 2) as fake_leads_percentage,
    
    -- Source performance ranking
    RANK() OVER (ORDER BY AVG(COALESCE(api_quality_score, quality_score)) DESC) as quality_rank,
    RANK() OVER (ORDER BY AVG(COALESCE(api_fraud_score, 0)) ASC) as fraud_safety_rank,
    RANK() OVER (ORDER BY COUNT(*) DESC) as volume_rank,
    
    -- Risk level assessment (using 10-point scale)
    CASE 
        WHEN AVG(COALESCE(api_fraud_score, 0)) >= 7 OR (COUNTIF(COALESCE(api_fraud_score, 0) >= 7)::DOUBLE / COUNT(*)) > 0.3 THEN 'CRITICAL'
        WHEN AVG(COALESCE(api_fraud_score, 0)) >= 5 OR (COUNTIF(COALESCE(api_fraud_score, 0) >= 6)::DOUBLE / COUNT(*)) > 0.2 THEN 'HIGH'
        WHEN AVG(COALESCE(api_fraud_score, 0)) >= 3 OR (COUNTIF(COALESCE(api_fraud_score, 0) >= 5)::DOUBLE / COUNT(*)) > 0.1 THEN 'MEDIUM'
        WHEN AVG(COALESCE(api_fraud_score, 0)) >= 2 THEN 'LOW'
        ELSE 'MINIMAL'
    END as source_risk_level,
    
    -- Quality grade (using 10-point scale)
    CASE 
        WHEN AVG(COALESCE(api_quality_score, quality_score)) >= 9 AND AVG(COALESCE(api_fraud_score, 0)) <= 2 THEN 'A+'
        WHEN AVG(COALESCE(api_quality_score, quality_score)) >= 8 AND AVG(COALESCE(api_fraud_score, 0)) <= 3 THEN 'A'
        WHEN AVG(COALESCE(api_quality_score, quality_score)) >= 7 AND AVG(COALESCE(api_fraud_score, 0)) <= 4 THEN 'B'
        WHEN AVG(COALESCE(api_quality_score, quality_score)) >= 6 AND AVG(COALESCE(api_fraud_score, 0)) <= 5 THEN 'C'
        WHEN AVG(COALESCE(api_quality_score, quality_score)) >= 4 AND AVG(COALESCE(api_fraud_score, 0)) <= 6 THEN 'D'
        ELSE 'F'
    END as source_grade,
    
    -- Temporal info
    MIN(parsed_at) as first_validation,
    MAX(parsed_at) as latest_validation,
    COUNT(DISTINCT DATE_TRUNC('day', parsed_at)) as validation_days_active

FROM parsed_validations
WHERE parse_error IS NULL 
AND parsed_at >= CURRENT_DATE - INTERVAL '90 days'
GROUP BY COALESCE(lead_source, 'Unknown')
ORDER BY avg_data_quality_score DESC, avg_fraud_score ASC;
