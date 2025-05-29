-- ===============================================
-- Example SQL Queries for Bronze Layer Analysis
-- ===============================================
-- QueryBuilder for Medical Data Science (Übungsblatt 3.2)
-- Use these queries to explore and analyze extracted data

-- ===============================================
-- 1. BASIC DATA EXPLORATION
-- ===============================================

-- Count total extracted records
SELECT COUNT(*) as total_records 
FROM bronze.collection_disease;

-- Records by source table
SELECT 
    source_table,
    COUNT(*) as record_count,
    COUNT(DISTINCT subject_id) as unique_patients,
    COUNT(DISTINCT itemid) as unique_parameters
FROM bronze.collection_disease 
GROUP BY source_table;

-- Data time range
SELECT 
    MIN(charttime) as earliest_record,
    MAX(charttime) as latest_record,
    MAX(charttime) - MIN(charttime) as time_span
FROM bronze.collection_disease
WHERE charttime IS NOT NULL;

-- ===============================================
-- 2. PARAMETER ANALYSIS
-- ===============================================

-- Top 20 most frequent parameters (Chart Events)
SELECT 
    cd.itemid,
    di.label,
    di.unitname,
    COUNT(*) as frequency
FROM bronze.collection_disease cd
JOIN mimiciv_icu.d_items di ON cd.itemid = di.itemid
WHERE cd.source_table = 'chartevents'
GROUP BY cd.itemid, di.label, di.unitname
ORDER BY frequency DESC
LIMIT 20;

-- Top 20 most frequent parameters (Lab Events)
SELECT 
    cd.itemid,
    dl.label,
    dl.category,
    COUNT(*) as frequency
FROM bronze.collection_disease cd
JOIN mimiciv_hosp.d_labitems dl ON cd.itemid = dl.itemid
WHERE cd.source_table = 'labevents'
GROUP BY cd.itemid, dl.label, dl.category
ORDER BY frequency DESC
LIMIT 20;

-- ===============================================
-- 3. PATIENT ANALYSIS
-- ===============================================

-- Patients with most measurements
SELECT 
    subject_id,
    COUNT(*) as total_measurements,
    COUNT(DISTINCT itemid) as unique_parameters,
    MIN(charttime) as first_measurement,
    MAX(charttime) as last_measurement
FROM bronze.collection_disease
GROUP BY subject_id
ORDER BY total_measurements DESC
LIMIT 20;

-- Patient ICU stays with respiratory measurements
SELECT 
    cd.subject_id,
    cd.stay_id,
    COUNT(DISTINCT cd.itemid) as unique_respiratory_params,
    COUNT(*) as total_measurements
FROM bronze.collection_disease cd
JOIN mimiciv_icu.d_items di ON cd.itemid = di.itemid
WHERE cd.source_table = 'chartevents'
AND (LOWER(di.label) LIKE '%respiratory%' OR LOWER(di.label) LIKE '%spo2%')
GROUP BY cd.subject_id, cd.stay_id
HAVING COUNT(*) > 50
ORDER BY total_measurements DESC;

-- ===============================================
-- 4. CLINICAL PARAMETER QUERIES
-- ===============================================

-- SpO2 measurements for specific patient
SELECT 
    cd.charttime,
    cd.valuenum,
    cd.valueuom,
    di.label
FROM bronze.collection_disease cd
JOIN mimiciv_icu.d_items di ON cd.itemid = di.itemid
WHERE cd.subject_id = 10000032  -- Replace with actual subject_id
AND LOWER(di.label) LIKE '%spo2%'
ORDER BY cd.charttime;

-- Heart Rate trends over time
SELECT 
    DATE(cd.charttime) as measurement_date,
    AVG(cd.valuenum) as avg_heart_rate,
    MIN(cd.valuenum) as min_heart_rate,
    MAX(cd.valuenum) as max_heart_rate,
    COUNT(*) as measurements
FROM bronze.collection_disease cd
JOIN mimiciv_icu.d_items di ON cd.itemid = di.itemid
WHERE LOWER(di.label) LIKE '%heart rate%'
AND cd.valuenum BETWEEN 30 AND 200  -- Reasonable heart rate range
GROUP BY DATE(cd.charttime)
ORDER BY measurement_date;

-- Lab values: pH analysis
SELECT 
    cd.subject_id,
    cd.hadm_id,
    cd.charttime,
    cd.valuenum as ph_value,
    CASE 
        WHEN cd.valuenum < 7.35 THEN 'Acidic'
        WHEN cd.valuenum > 7.45 THEN 'Basic'
        ELSE 'Normal'
    END as ph_status
FROM bronze.collection_disease cd
JOIN mimiciv_hosp.d_labitems dl ON cd.itemid = dl.itemid
WHERE LOWER(dl.label) LIKE '%ph%'
AND cd.valuenum BETWEEN 6.8 AND 7.8  -- Reasonable pH range
ORDER BY cd.charttime;

-- ===============================================
-- 5. DATA QUALITY ANALYSIS
-- ===============================================

-- Check for missing values
SELECT 
    source_table,
    COUNT(*) as total_records,
    COUNT(value) as non_null_values,
    COUNT(valuenum) as non_null_numeric,
    ROUND(100.0 * COUNT(valuenum) / COUNT(*), 2) as numeric_completeness_pct
FROM bronze.collection_disease
GROUP BY source_table;

-- Value distribution analysis
SELECT 
    cd.itemid,
    di.label,
    MIN(cd.valuenum) as min_value,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY cd.valuenum) as q1,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY cd.valuenum) as median,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY cd.valuenum) as q3,
    MAX(cd.valuenum) as max_value,
    COUNT(*) as count
FROM bronze.collection_disease cd
JOIN mimiciv_icu.d_items di ON cd.itemid = di.itemid
WHERE cd.source_table = 'chartevents'
AND cd.valuenum IS NOT NULL
GROUP BY cd.itemid, di.label
ORDER BY count DESC;

-- Detect potential outliers
SELECT 
    cd.itemid,
    di.label,
    cd.valuenum,
    cd.charttime,
    cd.subject_id
FROM bronze.collection_disease cd
JOIN mimiciv_icu.d_items di ON cd.itemid = di.itemid
WHERE cd.source_table = 'chartevents'
AND (
    (LOWER(di.label) LIKE '%heart rate%' AND (cd.valuenum < 20 OR cd.valuenum > 250))
    OR (LOWER(di.label) LIKE '%respiratory%' AND (cd.valuenum < 5 OR cd.valuenum > 60))
    OR (LOWER(di.label) LIKE '%spo2%' AND (cd.valuenum < 70 OR cd.valuenum > 100))
)
ORDER BY cd.itemid, cd.valuenum;

-- ===============================================
-- 6. RESPIRATORY FAILURE ANALYSIS
-- ===============================================

-- Patients with both SpO2 and Respiratory Rate measurements
WITH spo2_patients AS (
    SELECT DISTINCT cd.subject_id, cd.stay_id
    FROM bronze.collection_disease cd
    JOIN mimiciv_icu.d_items di ON cd.itemid = di.itemid
    WHERE LOWER(di.label) LIKE '%spo2%'
),
rr_patients AS (
    SELECT DISTINCT cd.subject_id, cd.stay_id
    FROM bronze.collection_disease cd
    JOIN mimiciv_icu.d_items di ON cd.itemid = di.itemid
    WHERE LOWER(di.label) LIKE '%respiratory rate%'
)
SELECT 
    sp.subject_id,
    sp.stay_id,
    'Both SpO2 and RR available' as measurement_status
FROM spo2_patients sp
INNER JOIN rr_patients rr ON sp.subject_id = rr.subject_id AND sp.stay_id = rr.stay_id;

-- Hypoxemia detection (SpO2 < 90%)
SELECT 
    cd.subject_id,
    cd.stay_id,
    cd.charttime,
    cd.valuenum as spo2_value,
    di.label
FROM bronze.collection_disease cd
JOIN mimiciv_icu.d_items di ON cd.itemid = di.itemid
WHERE LOWER(di.label) LIKE '%spo2%'
AND cd.valuenum < 90
AND cd.valuenum > 70  -- Exclude unrealistic values
ORDER BY cd.subject_id, cd.charttime;

-- ===============================================
-- 7. INFLAMMATORY MARKERS ANALYSIS
-- ===============================================

-- Patients with inflammatory markers (IL-6, IL-8, IL-10)
SELECT 
    cd.subject_id,
    dl.label as inflammatory_marker,
    AVG(cd.valuenum) as avg_value,
    MIN(cd.valuenum) as min_value,
    MAX(cd.valuenum) as max_value,
    COUNT(*) as measurements
FROM bronze.collection_disease cd
JOIN mimiciv_hosp.d_labitems dl ON cd.itemid = dl.itemid
WHERE LOWER(dl.label) LIKE '%il-%'
OR LOWER(dl.label) LIKE '%interleukin%'
GROUP BY cd.subject_id, dl.label
HAVING COUNT(*) >= 2  -- At least 2 measurements
ORDER BY cd.subject_id, avg_value DESC;

-- Procalcitonin levels (infection marker)
SELECT 
    cd.subject_id,
    cd.hadm_id,
    cd.charttime,
    cd.valuenum as procalcitonin_level,
    CASE 
        WHEN cd.valuenum < 0.1 THEN 'Low risk'
        WHEN cd.valuenum BETWEEN 0.1 AND 0.25 THEN 'Moderate risk'
        WHEN cd.valuenum > 0.25 THEN 'High risk'
        ELSE 'Unknown'
    END as infection_risk
FROM bronze.collection_disease cd
JOIN mimiciv_hosp.d_labitems dl ON cd.itemid = dl.itemid
WHERE LOWER(dl.label) LIKE '%procalcitonin%'
AND cd.valuenum IS NOT NULL
ORDER BY cd.subject_id, cd.charttime;

-- ===============================================
-- 8. TEMPORAL ANALYSIS
-- ===============================================

-- Measurements per hour of day
SELECT 
    EXTRACT(HOUR FROM charttime) as hour_of_day,
    COUNT(*) as measurement_count
FROM bronze.collection_disease
WHERE charttime IS NOT NULL
GROUP BY EXTRACT(HOUR FROM charttime)
ORDER BY hour_of_day;

-- Daily measurement trends
SELECT 
    DATE(charttime) as measurement_date,
    COUNT(*) as daily_measurements,
    COUNT(DISTINCT subject_id) as unique_patients
FROM bronze.collection_disease
WHERE charttime IS NOT NULL
GROUP BY DATE(charttime)
ORDER BY measurement_date;

-- ===============================================
-- 9. CROSS-TABLE CORRELATION ANALYSIS
-- ===============================================

-- Patients with both vital signs and lab values
WITH vital_patients AS (
    SELECT DISTINCT subject_id, hadm_id
    FROM bronze.collection_disease
    WHERE source_table = 'chartevents'
),
lab_patients AS (
    SELECT DISTINCT subject_id, hadm_id
    FROM bronze.collection_disease
    WHERE source_table = 'labevents'
)
SELECT 
    COUNT(DISTINCT vp.subject_id) as patients_with_both,
    COUNT(DISTINCT vp.hadm_id) as admissions_with_both
FROM vital_patients vp
INNER JOIN lab_patients lp ON vp.subject_id = lp.subject_id AND vp.hadm_id = lp.hadm_id;

-- ===============================================
-- 10. EXPORT QUERIES FOR FURTHER ANALYSIS
-- ===============================================

-- Export respiratory parameters for machine learning
SELECT 
    cd.subject_id,
    cd.hadm_id,
    cd.stay_id,
    cd.charttime,
    di.label as parameter_name,
    cd.valuenum as value,
    cd.valueuom as unit
FROM bronze.collection_disease cd
JOIN mimiciv_icu.d_items di ON cd.itemid = di.itemid
WHERE cd.source_table = 'chartevents'
AND (
    LOWER(di.label) LIKE '%spo2%' 
    OR LOWER(di.label) LIKE '%respiratory rate%'
    OR LOWER(di.label) LIKE '%heart rate%'
)
AND cd.valuenum IS NOT NULL
ORDER BY cd.subject_id, cd.charttime;

-- Export lab values for biomarker analysis
SELECT 
    cd.subject_id,
    cd.hadm_id,
    cd.charttime,
    dl.label as biomarker_name,
    cd.valuenum as value,
    cd.valueuom as unit,
    dl.category
FROM bronze.collection_disease cd
JOIN mimiciv_hosp.d_labitems dl ON cd.itemid = dl.itemid
WHERE cd.source_table = 'labevents'
AND cd.valuenum IS NOT NULL
ORDER BY cd.subject_id, cd.charttime;
