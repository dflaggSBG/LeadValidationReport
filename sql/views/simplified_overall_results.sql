-- Simplified Overall Validation Results
-- Provides: Total Validations, Average/Median Data Quality Score, Average/Median Fraud Score

CREATE OR REPLACE VIEW leads.simplified_overall_results AS
SELECT 
    -- Basic counts
    COUNT(*) as total_validations,
    COUNT(DISTINCT lead_id) as unique_leads,
    
    -- Data Quality Score metrics (0-1 scale, higher = better)
    AVG(data_quality_score) as avg_data_quality_score,
    MEDIAN(data_quality_score) as median_data_quality_score,
    MIN(data_quality_score) as min_data_quality_score,
    MAX(data_quality_score) as max_data_quality_score,
    
    -- Fraud Score metrics (0-1 scale, higher = more fraudulent) 
    AVG(fraud_score) as avg_fraud_score,
    MEDIAN(fraud_score) as median_fraud_score,
    MIN(fraud_score) as min_fraud_score,
    MAX(fraud_score) as max_fraud_score,
    
    -- Combined overall score for reference
    AVG(overall_score) as avg_overall_score,
    MEDIAN(overall_score) as median_overall_score,
    
    -- Quality categories
    COUNTIF(data_quality_score >= 0.9) as excellent_quality_count,
    COUNTIF(data_quality_score >= 0.7 AND data_quality_score < 0.9) as good_quality_count,
    COUNTIF(data_quality_score >= 0.5 AND data_quality_score < 0.7) as fair_quality_count,
    COUNTIF(data_quality_score < 0.5) as poor_quality_count,
    
    -- Fraud risk categories
    COUNTIF(fraud_score >= 0.8) as critical_fraud_risk_count,
    COUNTIF(fraud_score >= 0.6 AND fraud_score < 0.8) as high_fraud_risk_count,
    COUNTIF(fraud_score >= 0.4 AND fraud_score < 0.6) as medium_fraud_risk_count,
    COUNTIF(fraud_score >= 0.2 AND fraud_score < 0.4) as low_fraud_risk_count,
    COUNTIF(fraud_score < 0.2) as minimal_fraud_risk_count,
    
    -- Percentages
    ROUND((COUNTIF(data_quality_score >= 0.7)::DOUBLE / COUNT(*)) * 100, 2) as high_quality_percentage,
    ROUND((COUNTIF(fraud_score >= 0.6)::DOUBLE / COUNT(*)) * 100, 2) as high_fraud_risk_percentage,
    
    -- Data freshness
    MIN(validation_timestamp) as earliest_validation,
    MAX(validation_timestamp) as latest_validation,
    COUNT(DISTINCT DATE_TRUNC('day', validation_timestamp)) as validation_days_span,
    
    -- Summary status
    CASE 
        WHEN AVG(data_quality_score) >= 0.8 AND AVG(fraud_score) <= 0.3 THEN 'EXCELLENT'
        WHEN AVG(data_quality_score) >= 0.7 AND AVG(fraud_score) <= 0.4 THEN 'GOOD'
        WHEN AVG(data_quality_score) >= 0.6 AND AVG(fraud_score) <= 0.5 THEN 'FAIR'
        WHEN AVG(data_quality_score) >= 0.4 AND AVG(fraud_score) <= 0.7 THEN 'POOR'
        ELSE 'CRITICAL'
    END as overall_system_status

FROM leads.validation_results
WHERE validation_timestamp >= CURRENT_DATE - INTERVAL '90 days'; -- Last 90 days by default
