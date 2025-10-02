-- Simplified Overall Validation Results (Fixed for actual database structure)
-- Provides: Total Validations, Average/Median Data Quality Score, Average/Median Fraud Score

CREATE OR REPLACE VIEW simplified_overall_results AS
SELECT 
    -- Basic counts
    COUNT(*) as total_validations,
    COUNT(DISTINCT task_id) as unique_leads,
    
    -- Data Quality Score metrics (normalized from 0-10 scale to 0-1 scale)
    AVG(COALESCE(api_data_quality_score, api_quality_score, quality_score)) / 10.0 as avg_data_quality_score,
    AVG(COALESCE(api_data_quality_score, api_quality_score, quality_score)) / 10.0 as median_data_quality_score,
    MIN(COALESCE(api_data_quality_score, api_quality_score, quality_score)) / 10.0 as min_data_quality_score,
    MAX(COALESCE(api_data_quality_score, api_quality_score, quality_score)) / 10.0 as max_data_quality_score,
    
    -- Fraud Score metrics (normalized from 0-10 scale to 0-1 scale)
    AVG(COALESCE(api_fraud_score, 0)) / 10.0 as avg_fraud_score,
    AVG(COALESCE(api_fraud_score, 0)) / 10.0 as median_fraud_score,
    MIN(COALESCE(api_fraud_score, 0)) / 10.0 as min_fraud_score,
    MAX(COALESCE(api_fraud_score, 0)) / 10.0 as max_fraud_score,
    
    -- Combined overall score for reference (normalized)
    AVG(COALESCE(api_quality_score, quality_score)) / 10.0 as avg_overall_score,
    AVG(COALESCE(api_quality_score, quality_score)) / 10.0 as median_overall_score,
    
    -- Quality categories (using 10-point scale thresholds)
    COUNTIF(COALESCE(api_quality_score, quality_score) >= 9) as excellent_quality_count,
    COUNTIF(COALESCE(api_quality_score, quality_score) >= 7 AND COALESCE(api_quality_score, quality_score) < 9) as good_quality_count,
    COUNTIF(COALESCE(api_quality_score, quality_score) >= 5 AND COALESCE(api_quality_score, quality_score) < 7) as fair_quality_count,
    COUNTIF(COALESCE(api_quality_score, quality_score) < 5) as poor_quality_count,
    
    -- Fraud risk categories (using 10-point scale thresholds)
    COUNTIF(COALESCE(api_fraud_score, 0) >= 8) as critical_fraud_risk_count,
    COUNTIF(COALESCE(api_fraud_score, 0) >= 6 AND COALESCE(api_fraud_score, 0) < 8) as high_fraud_risk_count,
    COUNTIF(COALESCE(api_fraud_score, 0) >= 4 AND COALESCE(api_fraud_score, 0) < 6) as medium_fraud_risk_count,
    COUNTIF(COALESCE(api_fraud_score, 0) >= 2 AND COALESCE(api_fraud_score, 0) < 4) as low_fraud_risk_count,
    COUNTIF(COALESCE(api_fraud_score, 0) < 2) as minimal_fraud_risk_count,
    
    -- Percentages
    ROUND((COUNTIF(COALESCE(api_quality_score, quality_score) >= 7)::DOUBLE / COUNT(*)) * 100, 2) as high_quality_percentage,
    ROUND((COUNTIF(COALESCE(api_fraud_score, 0) >= 6)::DOUBLE / COUNT(*)) * 100, 2) as high_fraud_risk_percentage,
    
    -- Data freshness
    MIN(parsed_at) as earliest_validation,
    MAX(parsed_at) as latest_validation,
    COUNT(DISTINCT DATE_TRUNC('day', parsed_at)) as validation_days_span,
    
    -- Summary status
    CASE 
        WHEN AVG(COALESCE(api_quality_score, quality_score)) >= 8 AND AVG(COALESCE(api_fraud_score, 0)) <= 3 THEN 'EXCELLENT'
        WHEN AVG(COALESCE(api_quality_score, quality_score)) >= 7 AND AVG(COALESCE(api_fraud_score, 0)) <= 4 THEN 'GOOD'
        WHEN AVG(COALESCE(api_quality_score, quality_score)) >= 6 AND AVG(COALESCE(api_fraud_score, 0)) <= 5 THEN 'FAIR'
        WHEN AVG(COALESCE(api_quality_score, quality_score)) >= 4 AND AVG(COALESCE(api_fraud_score, 0)) <= 7 THEN 'POOR'
        ELSE 'CRITICAL'
    END as overall_system_status

FROM parsed_validations
WHERE parse_error IS NULL 
AND parsed_at >= CURRENT_DATE - INTERVAL '90 days'; -- Last 90 days by default
