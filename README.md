# 🏥 Bronze QueryBuilder for Acute Respiratory Failure Data

**Medical Data Science - Übungsblatt 3.2**  
*Dynamic SQL Query Generator for MIMIC-IV Clinical Parameters*

---

## 📋 **Overview**

This QueryBuilder extracts clinical parameters associated with **Acute Respiratory Failure (ARI)** from the MIMIC-IV database and structures them into a Bronze-level schema for further analysis. The system dynamically generates SQL queries based on OMOP concepts and parameter mappings from previous analyses.

### 🎯 **Key Features**
- ✅ **Dynamic SQL Generation** - Automatically builds queries based on parameter definitions
- ✅ **OMOP Concept Integration** - Maps parameters to standardized medical concepts
- ✅ **Comprehensive Logging** - Detailed logs for debugging and audit trails
- ✅ **Data Quality Filtering** - Excludes erroneous and incomplete records
- ✅ **Validation Tools** - Built-in data quality checks and reporting
- ✅ **Secure Configuration** - Database credentials separated and gitignored

---

## 🚀 **Quick Setup**

### **Automated Setup (Recommended)**
```bash
# Clone the repository
git clone https://github.com/yourusername/MDS_Schema.git
cd MDS_Schema

# Run the automated setup script
./setup_test.sh
```

The setup script will:
1. ✅ Create and configure virtual environment
2. ✅ Install Python dependencies
3. ✅ Set up database configuration
4. ✅ Test database connection
5. ✅ Extract medical data (if needed)
6. ✅ Validate data quality
7. ✅ Run comprehensive tests

### **Manual Setup**
If you prefer manual setup:

```bash
# 1. Create virtual environment
python3 -m venv venv
source venv/bin/activate

# 2. Install dependencies
pip install -r requirements.txt

# 3. Configure database (see Database Configuration section)
cp config_template.py config_local.py
# Edit config_local.py with your credentials

# 4. Test connection and run extraction
python test_db.py
python querybuilder.py
```

---

## 🔧 **Database Configuration**

### **⚠️ Important: Configure Your Database**

The project uses a secure configuration system that keeps your database credentials separate from the code:

1. **Copy the template:**
   ```bash
   cp config_template.py config_local.py
   ```

2. **Edit `config_local.py` with your database settings:**
   ```python
   DB_CONFIG = {
       'host': 'localhost',           # Your PostgreSQL host
       'port': 5432,                  # Your PostgreSQL port
       'database': 'mimiciv',         # Your MIMIC-IV database name
       'user': 'your_username',       # Your PostgreSQL username
       'password': 'your_password'    # Your password (or None for OS auth)
   }
   ```

3. **Common configurations:**

   **Local PostgreSQL with password:**
   ```python
   DB_CONFIG = {
       'host': 'localhost',
       'port': 5432, 
       'database': 'mimiciv',
       'user': 'postgres',
       'password': 'your_password'
   }
   ```

   **OS Authentication (no password):**
   ```python
   DB_CONFIG = {
       'host': 'localhost',
       'port': 5432,
       'database': 'mimiciv', 
       'user': 'your_username',
       'password': None
   }
   ```

   **Using Environment Variables (production):**
   ```python
   import os
   DB_CONFIG = {
       'host': os.getenv('DB_HOST', 'localhost'),
       'port': int(os.getenv('DB_PORT', 5432)),
       'database': os.getenv('DB_NAME', 'mimiciv'),
       'user': os.getenv('DB_USER'),
       'password': os.getenv('DB_PASSWORD')
   }
   ```

4. **Test your configuration:**
   ```bash
   python test_db.py
   ```

**🔒 Security Note:** The `config_local.py` file is automatically gitignored to protect your credentials.

---

## 🏗️ **Architecture**

```
MIMIC-IV Database (Source)
├── mimiciv_hosp.labevents     → Lab measurements 
├── mimiciv_icu.chartevents    → Vital signs & monitoring
├── mimiciv_hosp.d_labitems    → Lab item metadata
└── mimiciv_icu.d_items        → Chart item metadata
                ↓
        QueryBuilder.py
                ↓
    bronze.collection_disease   → Structured output
```

---

## 📊 **Extracted Parameters**

### **Chart Events** (mimiciv_icu.chartevents)
| Parameter | OMOP Concept ID | Description |
|-----------|-----------------|-------------|
| SpO₂ | 40764520 | Oxygen saturation |
| Respiratory Rate | 3027018 | Breathing frequency |
| Heart Rate | 3027017 | Pulse rate |
| Tidal Volume | 3024289 | Lung capacity measurement |
| Minute Ventilation | 3024328 | Ventilation rate |

### **Lab Events** (mimiciv_hosp.labevents)
| Parameter | OMOP Concept ID | Description |
|-----------|-----------------|-------------|
| pH | 3014605 | Blood acidity |
| PaCO₂ | 3020656 | Carbon dioxide pressure |
| Creatinine | 3016723 | Kidney function marker |
| Albumin | 3013705 | Protein level |
| D-Dimer | 3003737 | Clotting indicator |
| Procalcitonin | 3013682 | Infection marker |
| IL-6, IL-8, IL-10 | 3015039, 3015040, 3015037 | Inflammatory markers |
| NT-proBNP | 3016728 | Heart failure marker |

*Full parameter list available in `config.py`*

---

## 🚀 **Quick Start**

### **Prerequisites**
- Python 3.8+
- PostgreSQL with MIMIC-IV database
- Database access credentials

### **1. Installation**
```bash
# Clone/navigate to project directory
cd /path/to/querybuilder

# Install dependencies
pip install -r requirements.txt

# Alternative: Install manually
pip install sqlalchemy psycopg2-binary pandas
```

### **2. Configuration**
Edit database connection in `querybuilder.py` (line 248):
```python
connection_string = "postgresql://username@localhost:5432/mimiciv"
```

### **3. Run Extraction**
```bash
python querybuilder.py
```

### **4. Validate Results**
```bash
python validate_data.py
```

---

## 📁 **File Structure**

```
kod/
├── querybuilder.py          # Main extraction script
├── validate_data.py         # Data validation & quality checks
├── config.py               # Configuration & parameter mappings
├── requirements.txt        # Python dependencies
├── README.md              # This documentation
├── querybuilder.log       # Execution logs (generated)
└── examples/              # SQL query examples (optional)
```

---

## 🔧 **Configuration Options**

### **Database Settings** (`config.py`)
```python
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'mimiciv',
    'user': 'your_username'
}
```

### **Parameter Customization**
Add new parameters to extract:
```python
CHART_PARAMETERS = [
    'spo2', 'respiratory rate', 'heart rate',
    'your_new_parameter'  # Add here
]
```

### **Data Quality Filters**
```python
QUALITY_FILTERS = {
    'chartevents': {
        'exclude_error': True,
        'require_valuenum': True
    },
    'labevents': {
        'exclude_flags': ['abnormal', 'error'],
        'require_valuenum': True
    }
}
```

---

## 📊 **Output Schema**

### **bronze.collection_disease**
| Column | Type | Description |
|--------|------|-------------|
| `id` | SERIAL | Primary key |
| `subject_id` | INTEGER | Patient identifier |
| `hadm_id` | INTEGER | Hospital admission ID |
| `stay_id` | INTEGER | ICU stay ID |
| `charttime` | TIMESTAMP | Measurement time |
| `storetime` | TIMESTAMP | Storage time |
| `itemid` | INTEGER | Parameter identifier |
| `value` | TEXT | Raw measurement value |
| `valuenum` | NUMERIC | Numeric measurement |
| `valueuom` | TEXT | Unit of measurement |
| `source_table` | VARCHAR(50) | Source table (chartevents/labevents) |
| `omop_concept_id` | INTEGER | OMOP concept mapping |
| `extraction_timestamp` | TIMESTAMP | When record was extracted |

---

## 📈 **Usage Examples**

### **Basic Data Exploration**
```sql
-- Count total extracted records
SELECT COUNT(*) FROM bronze.collection_disease;

-- Records by source table
SELECT source_table, COUNT(*) 
FROM bronze.collection_disease 
GROUP BY source_table;

-- Top 10 most frequent parameters
SELECT itemid, COUNT(*) as frequency
FROM bronze.collection_disease 
GROUP BY itemid 
ORDER BY frequency DESC 
LIMIT 10;
```

### **Clinical Analysis**
```sql
-- SpO2 values for specific patient
SELECT charttime, valuenum, valueuom
FROM bronze.collection_disease cd
JOIN mimiciv_icu.d_items di ON cd.itemid = di.itemid
WHERE cd.subject_id = 10001234 
AND LOWER(di.label) LIKE '%spo2%'
ORDER BY charttime;

-- Average lab values by parameter
SELECT 
    dl.label,
    AVG(cd.valuenum) as avg_value,
    COUNT(*) as measurements
FROM bronze.collection_disease cd
JOIN mimiciv_hosp.d_labitems dl ON cd.itemid = dl.itemid
WHERE cd.source_table = 'labevents'
GROUP BY dl.label;
```

---

## 📋 **Logging & Monitoring**

### **Log File Location**
- **File:** `querybuilder.log`
- **Format:** Timestamp - Logger - Level - Message
- **Levels:** INFO, WARNING, ERROR

### **Key Log Messages**
```
✅ Connected to database. Found schemas: ['mimiciv_hosp', 'mimiciv_icu']
📊 Found 15 chart itemids: 220045: Heart Rate, 220210: Respiratory Rate...
💾 Inserted 125,483 rows from chartevents with itemids: [220045, 220210...]
🎯 EXTRACTION SUMMARY: Duration: 0:03:45, Total rows: 250,967
```

### **Error Handling**
- Connection failures → Check database credentials
- Missing schemas → Verify MIMIC-IV installation  
- No itemids found → Check parameter definitions
- Insertion errors → Review data quality filters

---

## 🔍 **Troubleshooting**

### **Common Issues**

| Issue | Symptom | Solution |
|-------|---------|----------|
| **Connection Failed** | `Database connection failed` | Check PostgreSQL service, credentials, database name |
| **No Tables Found** | `Required MIMIC-IV schemas not found` | Verify schema names: `mimiciv_hosp`, `mimiciv_icu` |
| **No ItemIDs** | `No itemids found, aborting extraction` | Check parameter spelling in `config.py` |
| **Permission Denied** | `permission denied for schema` | Grant access: `GRANT USAGE ON SCHEMA bronze TO username;` |
| **Memory Issues** | Process killed/timeout | Add LIMIT clauses for testing, increase system memory |

### **Debug Steps**
1. **Test Connection:**
   ```bash
   psql -h localhost -U username -d mimiciv -c "\dt mimiciv_hosp.*"
   ```

2. **Check Schemas:**
   ```sql
   SELECT schema_name FROM information_schema.schemata;
   ```

3. **Verify Parameters:**
   ```sql
   SELECT itemid, label FROM mimiciv_icu.d_items WHERE LOWER(label) LIKE '%spo2%';
   ```

4. **Test Small Extraction:**
   Modify `querybuilder.py` to add `LIMIT 1000` for testing.

---

## 📚 **Advanced Usage**

### **Batch Processing**
For large datasets, consider:
```python
# Add to insert queries
LIMIT 10000 OFFSET {batch_number * 10000}
```

### **Custom OMOP Mapping**
Update extracted records with OMOP concepts:
```sql
UPDATE bronze.collection_disease 
SET omop_concept_id = 3027018 
WHERE itemid IN (SELECT itemid FROM mimiciv_icu.d_items WHERE LOWER(label) LIKE '%respiratory rate%');
```

### **Data Quality Assessment**
```sql
-- Check value distributions
SELECT 
    itemid,
    MIN(valuenum) as min_val,
    MAX(valuenum) as max_val,
    AVG(valuenum) as avg_val,
    STDDEV(valuenum) as std_dev
FROM bronze.collection_disease 
WHERE valuenum IS NOT NULL 
GROUP BY itemid;
```

---

## 🤝 **Contributing**

### **Adding New Parameters**
1. Update `CHART_PARAMETERS` or `LAB_PARAMETERS` in `config.py`
2. Add OMOP concept ID to `OMOP_CONCEPTS`
3. Test with validation script
4. Update documentation

### **Extending Functionality**
- Add new source tables in `querybuilder.py`
- Implement additional quality filters
- Create specialized validation reports
- Add data export functions

---

## 📞 **Support**

### **Documentation**
- **MIMIC-IV:** https://mimic.mit.edu/docs/iv/
- **OMOP Concepts:** https://athena.ohdsi.org/
- **SQLAlchemy:** https://docs.sqlalchemy.org/

### **Logs Analysis**
Check `querybuilder.log` for detailed execution information:
- Connection status
- ItemID discovery results  
- Insertion statistics
- Error details with stack traces

---

## ✅ **Validation Checklist**

After running the QueryBuilder:

- [ ] `querybuilder.log` shows successful completion
- [ ] `bronze.collection_disease` table exists and contains data
- [ ] Both `chartevents` and `labevents` source tables represented
- [ ] No critical errors in log file
- [ ] Validation script reports expected parameter coverage
- [ ] Sample queries return reasonable results

---

## 🏗️ **Medallion Architecture Pipeline**

This project implements a complete **Bronze → Silver → Gold** medallion architecture for medical data processing:

### **🥉 Bronze Layer** (Raw Data)
- **Script**: `querybuilder.py`
- **Schema**: `bronze.collection_disease` 
- **Purpose**: Extract raw clinical data from MIMIC-IV
- **Data**: Unprocessed chartevents and labevents
- **Runner**: `./run_bronze.sh`

### **🥈 Silver Layer** (Standardized Data)
- **Script**: `standardize_data.py`
- **Schema**: `silver.collection_disease_std`
- **Purpose**: Clean, standardize, and validate Bronze data
- **Features**:
  - ✅ Unit standardization with OMOP concepts
  - ✅ Outlier detection and flagging
  - ✅ Duplicate resolution
  - ✅ Data quality validation
- **Runner**: `./run_silver.sh`

### **🥇 Gold Layer** (Analytics & BI)
- **Script**: `gold_analytics.py`
- **Schema**: `gold.*` (multiple analytical views)
- **Purpose**: Create business intelligence and analytical aggregations
- **Views Created**:
  - 📊 `gold.patient_summaries` - Patient-level metrics
  - 📈 `gold.clinical_indicators` - Parameter statistics & quality
  - 📅 `gold.daily_trends` - Daily aggregated trends
  - ⏰ `gold.hourly_patterns` - Hourly measurement patterns
  - 🔍 `gold.data_quality_summary` - Quality dashboard
- **Runner**: `./run_gold.sh`

### **🚀 Complete Pipeline**
Run the entire medallion architecture:
```bash
# Complete pipeline (Bronze → Silver → Gold)
./run_pipeline.sh

# Or run individual layers
./run_bronze.sh    # Extract raw data
./run_silver.sh    # Standardize data  
./run_gold.sh      # Create analytics
```

### **📊 Example Gold Layer Queries**
```sql
-- Top patients by measurement volume
SELECT subject_id, total_measurements, avg_heart_rate, avg_spo2 
FROM gold.patient_summaries 
ORDER BY total_measurements DESC LIMIT 10;

-- Parameter quality overview
SELECT concept_name, total_measurements, outlier_percentage, conversion_percentage
FROM gold.clinical_indicators
ORDER BY total_measurements DESC;

-- Recent trends
SELECT measurement_date, concept_name, daily_avg, daily_outliers
FROM gold.daily_trends 
WHERE measurement_date >= CURRENT_DATE - 7
ORDER BY measurement_date DESC;
```

---

**🎯 Ready to extract medical data for Acute Respiratory Failure analysis!**

*For questions or issues, check the log files and troubleshooting section above.*
