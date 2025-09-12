-- Simplified Validation Results by Lead Source
-- Provides: Lead Source, Total Validations, Avg/Median Quality & Fraud Scores

CREATE OR REPLACE VIEW leads.simplified_results_by_source AS
SELECT 
    COALESCE(lead_source, 'Unknown') as lead_source,
    
    -- Basic counts
    COUNT(*) as total_validations,
    COUNT(DISTINCT lead_id) as unique_leads,
    
    -- Data Quality Score metrics
    AVG(data_quality_score) as avg_data_quality_score,
    MEDIAN(data_quality_score) as median_data_quality_score,
    MIN(data_quality_score) as min_data_quality_score,
    MAX(data_quality_score) as max_data_quality_score,
    
    -- Fraud Score metrics
    AVG(fraud_score) as avg_fraud_score,
    MEDIAN(fraud_score) as median_fraud_score,  
    MIN(fraud_score) as min_fraud_score,
    MAX(fraud_score) as max_fraud_score,
    
    -- Quality distribution
    COUNTIF(data_quality_score >= 0.9) as excellent_quality_count,
    COUNTIF(data_quality_score >= 0.7 AND data_quality_score < 0.9) as good_quality_count,
    COUNTIF(data_quality_score >= 0.5 AND data_quality_score < 0.7) as fair_quality_count,
    COUNTIF(data_quality_score < 0.5) as poor_quality_count,
    
    -- Fraud risk distribution
    COUNTIF(fraud_score >= 0.8) as critical_fraud_risk_count,
    COUNTIF(fraud_score >= 0.6 AND fraud_score < 0.8) as high_fraud_risk_count,
    COUNTIF(fraud_score >= 0.4 AND fraud_score < 0.6) as medium_fraud_risk_count,
    COUNTIF(fraud_score < 0.4) as low_fraud_risk_count,
    
    -- Percentages
    ROUND((COUNTIF(data_quality_score >= 0.7)::DOUBLE / COUNT(*)) * 100, 2) as high_quality_percentage,
    ROUND((COUNTIF(fraud_score >= 0.6)::DOUBLE / COUNT(*)) * 100, 2) as high_fraud_risk_percentage,
    
    -- Fake leads count (high fraud score)
    COUNTIF(fraud_score >= 0.7) as likely_fake_leads_count,
    ROUND((COUNTIF(fraud_score >= 0.7)::DOUBLE / COUNT(*)) * 100, 2) as fake_leads_percentage,
    
    -- Source performance ranking
    RANK() OVER (ORDER BY AVG(data_quality_score) DESC) as quality_rank,
    RANK() OVER (ORDER BY AVG(fraud_score) ASC) as fraud_safety_rank,
    RANK() OVER (ORDER BY COUNT(*) DESC) as volume_rank,
    
    -- Risk level assessment
    CASE 
        WHEN AVG(fraud_score) >= 0.7 OR (COUNTIF(fraud_score >= 0.7)::DOUBLE / COUNT(*)) > 0.3 THEN 'CRITICAL'
        WHEN AVG(fraud_score) >= 0.5 OR (COUNTIF(fraud_score >= 0.6)::DOUBLE / COUNT(*)) > 0.2 THEN 'HIGH'
        WHEN AVG(fraud_score) >= 0.3 OR (COUNTIF(fraud_score >= 0.5)::DOUBLE / COUNT(*)) > 0.1 THEN 'MEDIUM'
        WHEN AVG(fraud_score) >= 0.2 THEN 'LOW'
        ELSE 'MINIMAL'
    END as source_risk_level,
    
    -- Quality grade
    CASE 
        WHEN AVG(data_quality_score) >= 0.9 AND AVG(fraud_score) <= 0.2 THEN 'A+'
        WHEN AVG(data_quality_score) >= 0.8 AND AVG(fraud_score) <= 0.3 THEN 'A'
        WHEN AVG(data_quality_score) >= 0.7 AND AVG(fraud_score) <= 0.4 THEN 'B'
        WHEN AVG(data_quality_score) >= 0.6 AND AVG(fraud_score) <= 0.5 THEN 'C'
        WHEN AVG(data_quality_score) >= 0.4 AND AVG(fraud_score) <= 0.6 THEN 'D'
        ELSE 'F'
    END as source_grade,
    
    -- Temporal info
    MIN(validation_timestamp) as first_validation,
    MAX(validation_timestamp) as latest_validation,
    COUNT(DISTINCT DATE_TRUNC('day', validation_timestamp)) as validation_days_active

FROM leads.validation_results 
WHERE validation_timestamp >= CURRENT_DATE - INTERVAL '90 days'
GROUP BY COALESCE(lead_source, 'Unknown')
ORDER BY avg_data_quality_score DESC, avg_fraud_score ASC;
