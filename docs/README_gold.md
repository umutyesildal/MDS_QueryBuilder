# 🥇 Gold Layer SOFA Score Calculation

**Medical Data Science - Gold Layer ETL for SOFA Scoring**  
*Sequential Organ Failure Assessment (SOFA) Score Calculator for MIMIC-IV ARI Patients*

---

## 📋 **Overview**

The Gold Layer implements a comprehensive SOFA score calculation pipeline that transforms standardized Silver layer data into analysis-ready SOFA scores for ICU patients. The system focuses on Acute Respiratory Failure (ARI) patients but can process all ICU admissions with disease type classification.

### 🎯 **Key Features**
- ✅ **24-Hour Windowing** - Consecutive non-overlapping windows from ICU admission
- ✅ **Complete SOFA Scoring** - All 6 organ systems (Respiratory, Cardiovascular, Coagulation, Liver, CNS, Renal)
- ✅ **ARI Patient Identification** - ICD-10/ICD-9 code based patient classification
- ✅ **Intelligent Imputation** - LOCF, population medians, and SpO2/FiO2 surrogate
- ✅ **Clinical Validation** - Adherence to official SOFA scoring guidelines
- ✅ **Comprehensive Logging** - Detailed audit trails and quality metrics

---

## 🏗️ **Architecture Overview**

```
Silver Layer (Standardized Data)
├── silver.collection_disease_std  → Standardized measurements
├── mimiciv_icu.icustays           → ICU stay information  
└── mimiciv_hosp.diagnoses_icd     → Diagnosis codes
                ↓
        🥇 Gold Layer Pipeline
                ↓
    ┌─ ARI Patient Identification (ICD codes)
    ├─ 24h Time Windowing (from ICU admission)
    ├─ SOFA Parameter Aggregation (min/max/sum per window)
    ├─ Intelligent Imputation (LOCF → SpO2 surrogate → Population median)
    ├─ SOFA Score Calculation (6 subscores + total)
    └─ Quality Validation & Audit
                ↓
        gold.sofa_scores  → Analysis-ready SOFA scores
```

---

## 🚀 **Quick Start**

### **Prerequisites**
- Completed Bronze and Silver layers
- Python 3.8+ with required dependencies
- PostgreSQL with MIMIC-IV and processed Silver data

### **1. Run SOFA Calculation**
```bash
# Activate virtual environment
source venv/bin/activate

# Run Gold layer SOFA calculation
python calculate_sofa_gold.py
```

### **2. Check Results**
```sql
-- View SOFA score distribution
SELECT sofa_total, COUNT(*) as frequency
FROM gold.sofa_scores
GROUP BY sofa_total
ORDER BY sofa_total;

-- ARI vs Other patients SOFA comparison
SELECT disease_type, 
       AVG(sofa_total) as avg_sofa,
       COUNT(*) as total_windows
FROM gold.sofa_scores
GROUP BY disease_type;
```

---

## 📊 **SOFA Scoring System**

### **Organ Systems & Scoring (0-4 points each)**

| **System** | **Parameter** | **Score 0** | **Score 1** | **Score 2** | **Score 3** | **Score 4** |
|------------|---------------|-------------|-------------|-------------|-------------|-------------|
| **Respiratory** | PaO₂/FiO₂ [mmHg] | ≥ 400 | < 400 | < 300 | < 200 + MV | < 100 + MV |
| **Cardiovascular** | MAP [mmHg] / Vasopressors | MAP ≥ 70 | MAP < 70 | Low dose pressors | Medium dose pressors | High dose pressors |
| **Coagulation** | Platelets [×10³/μL] | ≥ 150 | < 150 | < 100 | < 50 | < 20 |
| **Liver** | Bilirubin [mg/dL] | < 1.2 | 1.2-1.9 | 2.0-5.9 | 6.0-11.9 | ≥ 12.0 |
| **CNS** | Glasgow Coma Scale | 15 | 13-14 | 10-12 | 6-9 | < 6 |
| **Renal** | Creatinine [mg/dL] / Urine | < 1.2 | 1.2-1.9 | 2.0-3.4 | 3.5-4.9 / <500mL | ≥5.0 / <200mL |

**Total SOFA Score:** Sum of all subscores (0-24)
**Severity Categories:** Normal (0), Mild (1-6), Moderate (7-12), Severe (13-18), Critical (19-24)

---

## ⏰ **Time Windowing Strategy**

### **Window Generation**
- **Duration:** 24 consecutive hours
- **Reference:** ICU admission time (`icustays.intime`)
- **Type:** Non-overlapping consecutive windows
- **Numbering:** Window 1 = ICU Day 1, Window 2 = ICU Day 2, etc.

### **Example Windowing**
```
ICU Admission: 2023-01-15 08:30:00
├─ Window 1: 2023-01-15 08:30:00 → 2023-01-16 08:30:00 (ICU Day 1)
├─ Window 2: 2023-01-16 08:30:00 → 2023-01-17 08:30:00 (ICU Day 2)
└─ Window 3: 2023-01-17 08:30:00 → 2023-01-18 08:30:00 (ICU Day 3)
```

---

## 🔧 **Parameter Aggregation Rules**

### **Within Each 24-Hour Window:**

| **SOFA Component** | **Aggregation Rule** | **Clinical Rationale** |
|-------------------|---------------------|----------------------|
| **PaO₂/FiO₂ Ratio** | **Minimum** (worst) | SOFA uses worst respiratory function |
| **MAP** | **Minimum** (worst) | Lowest blood pressure indicates hemodynamic compromise |
| **Platelets** | **Minimum** (worst) | Lowest count reflects coagulation dysfunction |
| **Bilirubin** | **Maximum** (worst) | Highest level indicates liver dysfunction |
| **GCS** | **Minimum** (worst) | Lowest consciousness level for neurological assessment |
| **Creatinine** | **Maximum** (worst) | Highest level indicates renal dysfunction |
| **Urine Output** | **Sum** (total 24h) | Total daily output for renal assessment |
| **Vasopressors** | **Maximum** (highest dose) | Peak vasopressor requirement |

---

## 🔄 **Imputation Strategy**

### **Hierarchical Imputation Approach:**

1. **SpO₂/FiO₂ Surrogate** (for missing PaO₂/FiO₂)
   - Use SpO₂/FiO₂ ratio as substitute
   - Flag as `spo2_fio2_surrogate = TRUE`

2. **Last Observation Carried Forward (LOCF)**
   - Look back up to 48 hours within same ICU stay
   - Use most recent valid measurement
   - Flag as `parameter_imputed = TRUE`

3. **Population Median**
   - Use median values from ARI patient population
   - Only for parameters with ≥10 observations
   - Flag as `parameter_imputed = TRUE`

4. **Skip if Insufficient Data**
   - Skip windows with >3/6 SOFA components missing
   - Log as insufficient data for reliable SOFA calculation

---

## 🏥 **ARI Patient Identification**

### **ICD Code Classification:**

**ICD-10 Codes:**
- `J96.0*` - Acute respiratory failure (various subtypes)
- `J80` - Acute respiratory distress syndrome (ARDS)
- `J44.0/J44.1` - COPD with acute exacerbation
- `J12-J18` - Various pneumonia types

**ICD-9 Codes:**
- `518.81/518.82` - Acute respiratory failure
- `518.5` - Pulmonary insufficiency

### **Disease Type Classification:**
- **ARI Patients:** `disease_type = 'ARI'`
- **Other Patients:** `disease_type = 'OTHER'`

---

## 📁 **File Structure**

```
kod/
├── calculate_sofa_gold.py          # 🥇 Main SOFA calculation pipeline
├── config_gold.py                  # Gold layer configuration
├── sofa_mappings.py                # SOFA scoring tables and mappings
├── README_gold.md                  # This documentation
├── gold_sofa_calculation.log       # Pipeline execution logs (generated)
├── gold_validation_report.txt      # Quality validation report (generated)
└── gold_analysis_queries.sql       # Example analysis queries (generated)
```

---

## 📊 **Output Schema: `gold.sofa_scores`**

### **Patient & Window Information**
| Column | Type | Description |
|--------|------|-------------|
| `subject_id` | INTEGER | Patient identifier |
| `hadm_id` | INTEGER | Hospital admission ID |
| `stay_id` | INTEGER | ICU stay ID |
| `disease_type` | VARCHAR(10) | 'ARI' or 'OTHER' |
| `window_start` | TIMESTAMP | 24h window start time |
| `window_end` | TIMESTAMP | 24h window end time |
| `window_number` | INTEGER | Window sequence (1, 2, 3...) |
| `icu_day` | INTEGER | ICU day (1-based) |

### **Raw SOFA Parameter Values**
| Column | Type | Description |
|--------|------|-------------|
| `pao2` | NUMERIC | PaO₂ value (mmHg) |
| `fio2` | NUMERIC | FiO₂ (%) |
| `pao2_fio2_ratio` | NUMERIC | PaO₂/FiO₂ ratio |
| `spo2` | NUMERIC | SpO₂ (%) |
| `spo2_fio2_ratio` | NUMERIC | SpO₂/FiO₂ ratio |
| `is_mechanically_ventilated` | BOOLEAN | Mechanical ventilation status |
| `map` | NUMERIC | Mean arterial pressure (mmHg) |
| `platelets` | NUMERIC | Platelet count (×10³/μL) |
| `bilirubin` | NUMERIC | Bilirubin (mg/dL) |
| `gcs_total` | INTEGER | Glasgow Coma Scale total |
| `creatinine` | NUMERIC | Creatinine (mg/dL) |
| `urine_output_24h` | NUMERIC | 24-hour urine output (mL) |

### **Imputation Flags**
| Column | Type | Description |
|--------|------|-------------|
| `pao2_fio2_imputed` | BOOLEAN | PaO₂/FiO₂ imputed via LOCF/median |
| `spo2_fio2_surrogate` | BOOLEAN | Used SpO₂/FiO₂ as PaO₂/FiO₂ surrogate |
| `map_imputed` | BOOLEAN | MAP imputed |
| `platelets_imputed` | BOOLEAN | Platelets imputed |
| `bilirubin_imputed` | BOOLEAN | Bilirubin imputed |
| `gcs_imputed` | BOOLEAN | GCS imputed |
| `creatinine_imputed` | BOOLEAN | Creatinine imputed |
| `urine_output_imputed` | BOOLEAN | Urine output imputed |

### **SOFA Scores & Metadata**
| Column | Type | Description |
|--------|------|-------------|
| `sofa_respiratory_subscore` | INTEGER | Respiratory SOFA (0-4) |
| `sofa_cardiovascular_subscore` | INTEGER | Cardiovascular SOFA (0-4) |
| `sofa_coagulation_subscore` | INTEGER | Coagulation SOFA (0-4) |
| `sofa_liver_subscore` | INTEGER | Liver SOFA (0-4) |
| `sofa_cns_subscore` | INTEGER | CNS SOFA (0-4) |
| `sofa_renal_subscore` | INTEGER | Renal SOFA (0-4) |
| `sofa_total` | INTEGER | Total SOFA score (0-24) |
| `sofa_severity` | VARCHAR(20) | Severity category |
| `missing_components` | TEXT[] | List of missing SOFA components |
| `calculation_notes` | TEXT | Detailed calculation explanations |

---

## 📈 **Analysis Examples**

### **Patient SOFA Trajectory**
```sql
-- Track SOFA score progression for a specific patient
SELECT subject_id, stay_id, icu_day, window_start,
       sofa_total, sofa_severity,
       sofa_respiratory_subscore,
       sofa_cardiovascular_subscore,
       sofa_coagulation_subscore,
       sofa_liver_subscore,
       sofa_cns_subscore,
       sofa_renal_subscore
FROM gold.sofa_scores
WHERE subject_id = 12345678
ORDER BY window_start;
```

### **SOFA Score Distribution**
```sql
-- Overall SOFA score distribution
SELECT sofa_total, 
       COUNT(*) as frequency,
       ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
FROM gold.sofa_scores
GROUP BY sofa_total
ORDER BY sofa_total;
```

### **ARI vs Non-ARI Comparison**
```sql
-- Compare SOFA scores between ARI and other patients
SELECT disease_type,
       COUNT(*) as total_windows,
       AVG(sofa_total) as avg_sofa_score,
       STDDEV(sofa_total) as std_sofa_score,
       MIN(sofa_total) as min_sofa,
       MAX(sofa_total) as max_sofa,
       PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY sofa_total) as median_sofa
FROM gold.sofa_scores
GROUP BY disease_type;
```

### **Imputation Rate Analysis**
```sql
-- Analyze imputation rates by parameter
SELECT 
    AVG(CASE WHEN pao2_fio2_imputed THEN 1.0 ELSE 0.0 END) * 100 as pao2_imputation_rate,
    AVG(CASE WHEN spo2_fio2_surrogate THEN 1.0 ELSE 0.0 END) * 100 as spo2_surrogate_rate,
    AVG(CASE WHEN map_imputed THEN 1.0 ELSE 0.0 END) * 100 as map_imputation_rate,
    AVG(CASE WHEN platelets_imputed THEN 1.0 ELSE 0.0 END) * 100 as platelets_imputation_rate,
    AVG(CASE WHEN bilirubin_imputed THEN 1.0 ELSE 0.0 END) * 100 as bilirubin_imputation_rate,
    AVG(CASE WHEN gcs_imputed THEN 1.0 ELSE 0.0 END) * 100 as gcs_imputation_rate,
    AVG(CASE WHEN creatinine_imputed THEN 1.0 ELSE 0.0 END) * 100 as creatinine_imputation_rate,
    COUNT(*) as total_records
FROM gold.sofa_scores;
```

### **Daily SOFA Trends**
```sql
-- Average daily SOFA scores for ARI patients
SELECT icu_day,
       COUNT(*) as patients,
       AVG(sofa_total) as avg_sofa,
       AVG(sofa_respiratory_subscore) as avg_respiratory,
       AVG(sofa_cardiovascular_subscore) as avg_cardiovascular
FROM gold.sofa_scores
WHERE disease_type = 'ARI'
  AND icu_day <= 14  -- First 14 days
GROUP BY icu_day
ORDER BY icu_day;
```

---

## 📋 **Configuration**

### **Key Configuration Files:**

**`config_gold.py`** - Main configuration:
- Database connections and schema settings
- Time windowing parameters
- Aggregation rules for SOFA parameters
- Imputation strategies and thresholds
- Data quality filters and validation rules

**`sofa_mappings.py`** - SOFA scoring:
- Official SOFA scoring tables for all 6 organ systems
- OMOP concept mappings for clinical parameters
- ARI patient identification ICD codes
- Utility functions for score calculation

### **Customizable Settings:**

```python
# Time windowing
WINDOWING_CONFIG = {
    'window_duration_hours': 24,        # 24-hour windows
    'max_windows_per_stay': 30,         # Maximum 30 days
}

# Imputation thresholds
IMPUTATION_CONFIG = {
    'max_missing_components': 3,        # Skip if >3/6 missing
    'locf_max_lookback_hours': 48,      # 48h lookback for LOCF
}

# Data quality filters
QUALITY_FILTERS = {
    'min_stay_duration_hours': 6,       # Minimum ICU stay
    'exclude_outliers': True,           # Skip outlier records
}
```

---

## 🔍 **Validation & Quality Checks**

### **Automated Validation:**
- ✅ SOFA scores within valid ranges (0-24 total, 0-4 subscores)
- ✅ Temporal consistency of windows
- ✅ Missing data rate reporting
- ✅ Imputation rate analysis
- ✅ Data completeness assessment

### **Quality Metrics Generated:**
- Patient coverage rates
- Parameter availability by organ system
- Imputation success rates
- SOFA score distribution validation
- ARI vs non-ARI population comparison

---

## 📚 **Clinical References**

1. **Vincent JL, et al.** The SOFA (Sepsis-related Organ Failure Assessment) score to describe organ dysfunction/failure. *Intensive Care Med.* 1996;22(7):707-10.

2. **Ferreira FL, et al.** Serial evaluation of the SOFA score to predict outcome in critically ill patients. *JAMA.* 2001;286(14):1754-8.

3. **Brinton et al.** Development and validation of the qSOFA score for sepsis screening. *Critical Care Medicine.* Various implementations.

4. **Jeong et al.** SpO₂/FiO₂ ratio as surrogate for PaO₂/FiO₂ in ARDS assessment. *Respiratory Care.* Multiple validation studies.

---

## 🔧 **Troubleshooting**

### **Common Issues:**

| **Issue** | **Symptom** | **Solution** |
|-----------|-------------|--------------|
| **No ARI Patients Found** | `Found 0 ARI patients` | Check ICD code mappings in `diagnoses_icd` table |
| **Missing Silver Data** | `No measurements for window` | Verify Silver layer completed successfully |
| **High Missing Data** | `Skipping window with X/6 missing` | Review imputation thresholds in config |
| **Invalid SOFA Scores** | Scores outside 0-24 range | Check SOFA mapping tables |

### **Log File Analysis:**
Check `gold_sofa_calculation.log` for:
- ARI patient identification results
- Window generation statistics
- Imputation success rates
- SOFA calculation warnings
- Final pipeline summary

---

## ✅ **Validation Checklist**

After running Gold Layer SOFA calculation:

- [ ] `gold_sofa_calculation.log` shows successful completion
- [ ] `gold.sofa_scores` table created with expected record count
- [ ] SOFA scores are within valid ranges (0-24)
- [ ] ARI patients properly identified and classified
- [ ] Imputation rates are reasonable (<50% for most parameters)
- [ ] Sample queries return clinically meaningful results
- [ ] No critical errors or warnings in logs

---

## 🎯 **Next Steps: Business Intelligence**

The Gold Layer SOFA scores enable various downstream analyses:

1. **Predictive Modeling** - ICU outcome prediction using SOFA trajectories
2. **Quality Improvement** - ICU performance benchmarking
3. **Clinical Research** - ARI patient outcome studies
4. **Real-time Dashboards** - SOFA score monitoring systems
5. **Sepsis Detection** - Early warning system development

---

**🥇 Analysis-ready SOFA scores for critical care research and quality improvement!**

*Medical Data Science - Gold Layer SOFA Score Calculator | Complete Medallion Architecture Healthcare Data Pipeline*
