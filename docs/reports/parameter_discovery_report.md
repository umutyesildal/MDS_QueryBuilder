# MIMIC-IV SOFA Parameter Discovery Report
Generated: 2025-06-20 16:29:50

## Database Overview
- mimiciv_icu.chartevents: 668,862 records
- mimiciv_hosp.labevents: 107,727 records
- mimiciv_icu.outputevents: 9,362 records
- mimiciv_icu.inputevents: 20,404 records
- mimiciv_icu.icustays: 140 records

## SOFA Parameter Coverage by System
### RESPIRATORY (15 parameters found)
#### chartevents (13 items)
- **220210**: Respiratory Rate (13,913 measurements, 100 patients)
- **220339**: PEEP set (1,447 measurements, 56 patients)
- **224690**: Respiratory Rate (Total) (1,331 measurements, 56 patients)
- **224689**: Respiratory Rate (spontaneous) (1,314 measurements, 55 patients)
- **226253**: SpO2 Desat Limit (1,176 measurements, 100 patients)
- **223849**: Ventilator Mode (1,011 measurements, 43 patients)
- **223848**: Ventilator Type (906 measurements, 42 patients)
- **224688**: Respiratory Rate (Set) (801 measurements, 53 patients)
- **224700**: Total PEEP Level (490 measurements, 47 patients)
- **227566**: Ventilator Tank #2 (409 measurements, 48 patients)
- **227565**: Ventilator Tank #1 (408 measurements, 48 patients)
- **229314**: Ventilator Mode (Hamilton) (402 measurements, 17 patients)
- **228232**: PAR-Oxygen saturation (45 measurements, 15 patients)
#### labevents (2 items)
- **50817**: Oxygen Saturation (223 measurements, 61 patients)
- **50819**: PEEP (130 measurements, 26 patients)

### CARDIOVASCULAR (33 parameters found)
#### chartevents (26 items)
- **220180**: Non Invasive Blood Pressure diastolic (8,349 measurements, 100 patients)
- **220179**: Non Invasive Blood Pressure systolic (8,347 measurements, 100 patients)
- **220181**: Non Invasive Blood Pressure mean (8,342 measurements, 99 patients)
- **220052**: Arterial Blood Pressure mean (5,560 measurements, 56 patients)
- **220050**: Arterial Blood Pressure systolic (5,525 measurements, 56 patients)
- **220051**: Arterial Blood Pressure diastolic (5,524 measurements, 56 patients)
- **223752**: Non-Invasive Blood Pressure Alarm - Low (876 measurements, 96 patients)
- **223751**: Non-Invasive Blood Pressure Alarm - High (876 measurements, 96 patients)
- **225312**: ART BP Mean (488 measurements, 9 patients)
- **225309**: ART BP Systolic (486 measurements, 9 patients)
- **225310**: ART BP Diastolic (486 measurements, 9 patients)
- **228005**: PBP (Prefilter) Replacement Rate (440 measurements, 4 patients)
- **220058**: Arterial Blood Pressure Alarm - High (436 measurements, 56 patients)
- **220056**: Arterial Blood Pressure Alarm - Low (433 measurements, 56 patients)
- **220059**: Pulmonary Artery Pressure systolic (325 measurements, 12 patients)
- **220060**: Pulmonary Artery Pressure diastolic (324 measurements, 12 patients)
- **224366**: Epidural Infusion Rate (mL/hr) (83 measurements, 4 patients)
- **227537**: ART Blood Pressure Alarm - High (55 measurements, 20 patients)
- **227538**: ART Blood Pressure Alarm - Low (55 measurements, 20 patients)
- **224322**: IABP Mean (38 measurements, 1 patients)
- **228866**: Plateau Pressure (IABP) (33 measurements, 1 patients)
- **224367**: Epidural Total Dose (mL) (16 measurements, 3 patients)
- **227355**: IABP Dressing Occlusive (11 measurements, 1 patients)
- **225981**: IABP Alarms Activated (11 measurements, 1 patients)
- **225342**: IABP Zero/Calibrate (11 measurements, 1 patients)
- **226110**: IABP placed in outside facility (11 measurements, 1 patients)
#### inputevents (7 items)
- **221906**: Norepinephrine (947 measurements, 26 patients)
- **221749**: Phenylephrine (625 measurements, 30 patients)
- **225851**: Cefepime (144 measurements, 24 patients)
- **222315**: Vasopressin (55 measurements, 8 patients)
- **221653**: Dobutamine (44 measurements, 2 patients)
- **221289**: Epinephrine (36 measurements, 4 patients)
- **221662**: Dopamine (28 measurements, 3 patients)

### COAGULATION (1 parameters found)
#### labevents (1 items)
- **51265**: Platelet Count (2,820 measurements, 100 patients)

### LIVER (4 parameters found)
#### labevents (4 items)
- **50885**: Bilirubin, Total (1,146 measurements, 80 patients)
- **51514**: Urobilinogen (61 measurements, 34 patients)
- **50883**: Bilirubin, Direct (53 measurements, 18 patients)
- **50884**: Bilirubin, Indirect (45 measurements, 18 patients)

### CNS (5 parameters found)
#### chartevents (5 items)
- **220739**: GCS - Eye Opening (3,274 measurements, 100 patients)
- **223900**: GCS - Verbal Response (3,266 measurements, 100 patients)
- **223901**: GCS - Motor Response (3,251 measurements, 100 patients)
- **227346**: Mental status (1,155 measurements, 99 patients)
- **228231**: PAR-Consciousness (46 measurements, 16 patients)

### RENAL (10 parameters found)
#### chartevents (2 items)
- **220615**: Creatinine (serum) (928 measurements, 98 patients)
- **225110**: Recreational drug use (143 measurements, 53 patients)
#### labevents (6 items)
- **50912**: Creatinine (3,003 measurements, 100 patients)
- **50910**: Creatine Kinase (CK) (200 measurements, 59 patients)
- **50911**: Creatine Kinase, MB Isoenzyme (179 measurements, 51 patients)
- **51082**: Creatinine, Urine (106 measurements, 35 patients)
- **51070**: Albumin/Creatinine, Urine (20 measurements, 8 patients)
- **51099**: Protein/Creatinine Ratio (15 measurements, 9 patients)
#### outputevents (2 items)
- **226559**: Foley (6,637 measurements, 85 patients)
- **227489**: GU Irrigant/Urine Volume Out (31 measurements, 3 patients)

## Summary
- **Total SOFA parameters discovered**: 68
- **Systems with parameters**: 6/6