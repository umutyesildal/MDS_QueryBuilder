-- ===============================================
-- Silver Layer Analysis Queries
-- ===============================================
-- Example SQL queries for analyzing standardized Silver layer data
-- QueryBuilder for Medical Data Science (Übungsblatt 3.2)

-- ===============================================
-- 1. BASIC SILVER LAYER EXPLORATION
-- ===============================================

-- Count total standardized records
SELECT COUNT(*) as total_standardized_records 
FROM silver.collection_disease_std;

-- Records by OMOP concept
SELECT 
    concept_id,
    concept_name,
    parameter_type,
    COUNT(*) as record_count,
    COUNT(DISTINCT subject_id) as unique_patients
FROM silver.collection_disease_std 
GROUP BY concept_id, concept_name, parameter_type
ORDER BY record_count DESC;

-- Data quality overview
SELECT 
    parameter_type,
    COUNT(*) as total_records,
    SUM(CASE WHEN is_outlier THEN 1 ELSE 0 END) as outliers,
    SUM(CASE WHEN error_flag THEN 1 ELSE 0 END) as errors,
    ROUND(AVG(CASE WHEN is_outlier THEN 0 ELSE 1 END) * 100, 2) as quality_percentage
FROM silver.collection_disease_std
GROUP BY parameter_type;

-- ===============================================
-- 2. OMOP STANDARDIZATION ANALYSIS
-- ===============================================

-- OMOP concept coverage by patient
SELECT 
    subject_id,
    COUNT(DISTINCT concept_id) as unique_concepts,
    COUNT(*) as total_measurements
FROM silver.collection_disease_std
GROUP BY subject_id
ORDER BY unique_concepts DESC
LIMIT 20;

-- Unit standardization verification
SELECT 
    concept_name,
    valueuom,
    COUNT(*) as count,
    MIN(value) as min_value,
    MAX(value) as max_value,
    ROUND(AVG(value), 2) as avg_value
FROM silver.collection_disease_std
WHERE value IS NOT NULL
GROUP BY concept_name, valueuom
ORDER BY concept_name, count DESC;

-- Transformation summary
SELECT 
    CASE 
        WHEN transformation_log LIKE '%Unit converted%' THEN 'Unit Conversion'
        WHEN transformation_log LIKE '%Outlier%' THEN 'Outlier Detection'
        WHEN transformation_log LIKE '%Duplicate%' THEN 'Duplicate Resolution'
        ELSE 'Other'
    END as transformation_type,
    COUNT(*) as count
FROM silver.collection_disease_std
WHERE transformation_log != ''
GROUP BY transformation_type
ORDER BY count DESC;

-- ===============================================
-- 3. CLINICAL PARAMETER ANALYSIS
-- ===============================================

-- Respiratory parameters analysis
SELECT 
    subject_id,
    charttime,
    concept_name,
    value,
    valueuom,
    is_outlier
FROM silver.collection_disease_std
WHERE concept_id = 3027018  -- Respiratory Rate
    AND charttime >= '2180-01-01'
    AND charttime < '2181-01-01'
ORDER BY subject_id, charttime
LIMIT 50;

-- Heart rate trends with outlier analysis
SELECT 
    DATE(charttime) as measurement_date,
    COUNT(*) as total_measurements,
    ROUND(AVG(value), 1) as avg_heart_rate,
    ROUND(STDDEV(value), 1) as std_heart_rate,
    MIN(value) as min_heart_rate,
    MAX(value) as max_heart_rate,
    SUM(CASE WHEN is_outlier THEN 1 ELSE 0 END) as outliers
FROM silver.collection_disease_std
WHERE concept_id = 3027017  -- Heart Rate
GROUP BY DATE(charttime)
ORDER BY measurement_date
LIMIT 30;

-- Laboratory parameters comparison
SELECT 
    s.concept_name,
    COUNT(*) as total_tests,
    COUNT(DISTINCT s.subject_id) as patients_tested,
    ROUND(AVG(s.value), 3) as mean_value,
    ROUND(STDDEV(s.value), 3) as std_value,
    MIN(s.value) as min_value,
    MAX(s.value) as max_value,
    s.valueuom
FROM silver.collection_disease_std s
WHERE s.parameter_type = 'lab'
    AND s.value IS NOT NULL
    AND NOT s.error_flag
GROUP BY s.concept_name, s.valueuom
ORDER BY total_tests DESC;

-- ===============================================
-- 4. PATIENT COHORT ANALYSIS
-- ===============================================

-- Patient respiratory profile
WITH patient_resp_profile AS (
    SELECT 
        subject_id,
        AVG(CASE WHEN concept_id = 3027018 THEN value END) as avg_resp_rate,
        AVG(CASE WHEN concept_id = 3027017 THEN value END) as avg_heart_rate,
        AVG(CASE WHEN concept_id = 40764520 THEN value END) as avg_spo2,
        COUNT(DISTINCT DATE(charttime)) as monitoring_days
    FROM silver.collection_disease_std
    WHERE parameter_type = 'vital'
        AND NOT error_flag
    GROUP BY subject_id
    HAVING COUNT(*) >= 10  -- At least 10 measurements
)
SELECT 
    subject_id,
    ROUND(avg_resp_rate, 1) as avg_respiratory_rate,
    ROUND(avg_heart_rate, 1) as avg_heart_rate,
    ROUND(avg_spo2, 1) as avg_oxygen_saturation,
    monitoring_days
FROM patient_resp_profile
ORDER BY avg_resp_rate DESC
LIMIT 20;

-- Critical values detection
SELECT 
    subject_id,
    hadm_id,
    charttime,
    concept_name,
    value,
    valueuom,
    CASE 
        WHEN concept_name = 'Heart Rate' AND (value < 50 OR value > 150) THEN 'Critical HR'
        WHEN concept_name = 'Respiratory Rate' AND (value < 12 OR value > 30) THEN 'Critical RR'
        WHEN concept_name = 'Oxygen Saturation' AND value < 90 THEN 'Critical SpO2'
        WHEN concept_name = 'pH' AND (value < 7.25 OR value > 7.55) THEN 'Critical pH'
        ELSE 'Normal'
    END as clinical_status
FROM silver.collection_disease_std
WHERE (
    (concept_name = 'Heart Rate' AND (value < 50 OR value > 150)) OR
    (concept_name = 'Respiratory Rate' AND (value < 12 OR value > 30)) OR
    (concept_name = 'Oxygen Saturation' AND value < 90) OR
    (concept_name = 'pH' AND (value < 7.25 OR value > 7.55))
)
AND NOT error_flag
ORDER BY charttime DESC
LIMIT 50;

-- ===============================================
-- 5. TEMPORAL ANALYSIS
-- ===============================================

-- Daily measurement frequency by parameter type
SELECT 
    DATE(charttime) as measurement_date,
    parameter_type,
    COUNT(*) as measurements,
    COUNT(DISTINCT subject_id) as unique_patients,
    COUNT(DISTINCT concept_id) as unique_concepts
FROM silver.collection_disease_std
WHERE charttime >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY DATE(charttime), parameter_type
ORDER BY measurement_date DESC, parameter_type;

-- Hourly pattern analysis for vital signs
SELECT 
    EXTRACT(HOUR FROM charttime) as hour_of_day,
    concept_name,
    COUNT(*) as measurement_count,
    ROUND(AVG(value), 2) as avg_value,
    ROUND(STDDEV(value), 2) as std_value
FROM silver.collection_disease_std
WHERE parameter_type = 'vital'
    AND NOT error_flag
    AND charttime >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY EXTRACT(HOUR FROM charttime), concept_name
ORDER BY hour_of_day, concept_name;

-- ===============================================
-- 6. DATA QUALITY ASSESSMENT
-- ===============================================

-- Quality metrics by concept
SELECT 
    concept_name,
    COUNT(*) as total_records,
    SUM(CASE WHEN error_flag THEN 1 ELSE 0 END) as errors,
    SUM(CASE WHEN is_outlier THEN 1 ELSE 0 END) as outliers,
    SUM(CASE WHEN transformation_log != '' THEN 1 ELSE 0 END) as transformed,
    ROUND(
        (COUNT(*) - SUM(CASE WHEN error_flag THEN 1 ELSE 0 END)) * 100.0 / COUNT(*), 
        2
    ) as quality_percentage
FROM silver.collection_disease_std
GROUP BY concept_name
ORDER BY total_records DESC;

-- Outlier analysis by parameter
SELECT 
    concept_name,
    parameter_type,
    valueuom,
    COUNT(*) as total_measurements,
    SUM(CASE WHEN is_outlier THEN 1 ELSE 0 END) as outliers,
    ROUND(
        SUM(CASE WHEN is_outlier THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 
        2
    ) as outlier_percentage,
    MIN(value) as min_value,
    MAX(value) as max_value
FROM silver.collection_disease_std
WHERE value IS NOT NULL
GROUP BY concept_name, parameter_type, valueuom
HAVING SUM(CASE WHEN is_outlier THEN 1 ELSE 0 END) > 0
ORDER BY outlier_percentage DESC;

-- ===============================================
-- 7. COMPARATIVE ANALYSIS (Bronze vs Silver)
-- ===============================================

-- Data volume comparison
SELECT 
    'Bronze Layer' as layer,
    COUNT(*) as record_count,
    COUNT(DISTINCT subject_id) as unique_patients
FROM bronze.collection_disease
UNION ALL
SELECT 
    'Silver Layer' as layer,
    COUNT(*) as record_count,
    COUNT(DISTINCT subject_id) as unique_patients
FROM silver.collection_disease_std;

-- Unit standardization impact
WITH bronze_units AS (
    SELECT 
        itemid,
        valueuom,
        COUNT(*) as bronze_count
    FROM bronze.collection_disease
    WHERE valueuom IS NOT NULL
    GROUP BY itemid, valueuom
),
silver_units AS (
    SELECT 
        itemid,
        valueuom,
        COUNT(*) as silver_count
    FROM silver.collection_disease_std
    WHERE valueuom IS NOT NULL
    GROUP BY itemid, valueuom
)
SELECT 
    'Bronze unique units' as metric,
    COUNT(DISTINCT valueuom) as count
FROM bronze_units
UNION ALL
SELECT 
    'Silver unique units' as metric,
    COUNT(DISTINCT valueuom) as count
FROM silver_units;

-- ===============================================
-- 8. EXPORT QUERIES FOR ANALYSIS
-- ===============================================

-- Export standardized vital signs for analysis
SELECT 
    subject_id,
    hadm_id,
    stay_id,
    charttime,
    concept_id,
    concept_name,
    value,
    valueuom,
    is_outlier
FROM silver.collection_disease_std
WHERE parameter_type = 'vital'
    AND NOT error_flag
    AND charttime >= '2180-01-01'
ORDER BY subject_id, charttime;

-- Export lab results for cohort analysis
SELECT 
    subject_id,
    hadm_id,
    charttime,
    concept_id,
    concept_name,
    value,
    valueuom,
    is_outlier
FROM silver.collection_disease_std
WHERE parameter_type = 'lab'
    AND NOT error_flag
ORDER BY subject_id, charttime;

-- ===============================================
-- END OF SILVER LAYER ANALYSIS QUERIES
-- ===============================================
