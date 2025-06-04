CREATE MATERIALIZED VIEW IF NOT EXISTS silver.cohort_disease AS
SELECT DISTINCT s.stay_id
FROM mimiciv_hosp.diagnoses_icd d
JOIN mimiciv_hosp.transfers t ON d.subject_id = t.subject_id AND d.hadm_id = t.hadm_id
JOIN mimiciv_icu.icustays s ON t.subject_id = s.subject_id AND t.hadm_id = s.hadm_id
WHERE d.icd_code IN ('51881', 'J960', 'J9600', 'J9601', 'J9602', 'J961', 'J9610', 'J9611', 'J9612', 'J969');
