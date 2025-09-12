-- Lead Validation Summary View
CREATE OR REPLACE VIEW leads.validation_summary AS
WITH latest_validations AS (
  SELECT 
    lead_id,
    validation_timestamp,
    overall_score,
    validation_status,
    first_name,
    last_name,
    email,
    phone,
    company,
    status as lead_status,
    lead_source,
    created_date,
    last_modified_date,
    is_converted,
    converted_date,
    ROW_NUMBER() OVER (PARTITION BY lead_id ORDER BY validation_timestamp DESC) as rn
  FROM leads.validation_results
)
SELECT 
  lead_id,
  validation_timestamp,
  overall_score,
  validation_status,
  first_name,
  last_name,
  email,
  phone,
  company,
  lead_status,
  lead_source,
  created_date,
  last_modified_date,
  is_converted,
  converted_date,
  
  -- Score categorization
  CASE 
    WHEN overall_score >= 0.9 THEN 'Excellent'
    WHEN overall_score >= 0.8 THEN 'Good'
    WHEN overall_score >= 0.6 THEN 'Fair'
    WHEN overall_score >= 0.4 THEN 'Poor'
    ELSE 'Invalid'
  END as score_category,
  
  -- Lead age in days
  DATE_DIFF('day', created_date, CURRENT_DATE) as lead_age_days,
  
  -- Days since last validation
  DATE_DIFF('day', validation_timestamp, CURRENT_TIMESTAMP) as days_since_validation,
  
  -- Conversion indicators
  CASE WHEN is_converted THEN 'Converted' ELSE 'Not Converted' END as conversion_status,
  CASE 
    WHEN is_converted AND converted_date IS NOT NULL 
    THEN DATE_DIFF('day', created_date, converted_date)
    ELSE NULL 
  END as days_to_conversion
  
FROM latest_validations 
WHERE rn = 1;
