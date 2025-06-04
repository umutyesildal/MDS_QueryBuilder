#!/usr/bin/env python3
"""
SOFA Score Mapping Tables
========================

Sequential Organ Failure Assessment (SOFA) score mapping tables based on
official SOFA scoring criteria. Used for converting raw clinical values
to SOFA subscores (0-4 points per organ system).

Reference: Vincent JL, et al. The SOFA (Sepsis-related Organ Failure Assessment) 
score to describe organ dysfunction/failure. Intensive Care Med. 1996;22(7):707-10.

Author: Medical Data Science Team
Date: 2025-06-04
"""

# =============================================================================
# SOFA SCORING MAPPINGS
# =============================================================================

class SOFAMappings:
    """
    SOFA score mapping tables for all six organ systems.
    Each mapping function returns a tuple of (score, description).
    """
    
    @staticmethod
    def respiratory_score(pao2_fio2_ratio, is_mechanically_ventilated=False):
        """
        Respiratory SOFA Score based on PaO2/FiO2 ratio
        
        Args:
            pao2_fio2_ratio (float): PaO2/FiO2 ratio in mmHg
            is_mechanically_ventilated (bool): Whether patient is on mechanical ventilation
            
        Returns:
            tuple: (score, description)
        """
        if pao2_fio2_ratio is None:
            return (None, "Missing PaO2/FiO2 ratio")
        
        if pao2_fio2_ratio >= 400:
            return (0, "PaO2/FiO2 ≥ 400")
        elif pao2_fio2_ratio < 400 and pao2_fio2_ratio >= 300:
            return (1, "PaO2/FiO2 < 400")
        elif pao2_fio2_ratio < 300 and pao2_fio2_ratio >= 200:
            return (2, "PaO2/FiO2 < 300")
        elif pao2_fio2_ratio < 200 and pao2_fio2_ratio >= 100:
            if is_mechanically_ventilated:
                return (3, "PaO2/FiO2 < 200 with mechanical ventilation")
            else:
                return (2, "PaO2/FiO2 < 200 without mechanical ventilation")
        elif pao2_fio2_ratio < 100:
            if is_mechanically_ventilated:
                return (4, "PaO2/FiO2 < 100 with mechanical ventilation")
            else:
                return (3, "PaO2/FiO2 < 100 without mechanical ventilation")
        else:
            return (0, "Normal respiratory function")
    
    @staticmethod
    def cardiovascular_score(map_mmhg, vasopressor_doses=None):
        """
        Cardiovascular SOFA Score based on MAP and vasopressor use
        
        Args:
            map_mmhg (float): Mean Arterial Pressure in mmHg
            vasopressor_doses (dict): Dictionary with vasopressor doses
                                    {'dopamine': dose, 'epinephrine': dose, 'norepinephrine': dose}
                                    
        Returns:
            tuple: (score, description)
        """
        if map_mmhg is None and (vasopressor_doses is None or not any(vasopressor_doses.values())):
            return (None, "Missing MAP and vasopressor data")
        
        # Check vasopressor use first (higher priority)
        if vasopressor_doses:
            dopamine = vasopressor_doses.get('dopamine', 0) or 0
            epinephrine = vasopressor_doses.get('epinephrine', 0) or 0 
            norepinephrine = vasopressor_doses.get('norepinephrine', 0) or 0
            dobutamine = vasopressor_doses.get('dobutamine', 0) or 0
            
            # Score 4: High dose vasopressors
            if (dopamine > 15 or epinephrine > 0.1 or norepinephrine > 0.1):
                return (4, "High dose vasopressors")
            
            # Score 3: Medium dose vasopressors  
            elif (dopamine > 5 or epinephrine > 0 and epinephrine <= 0.1 or 
                  norepinephrine > 0 and norepinephrine <= 0.1):
                return (3, "Medium dose vasopressors")
                
            # Score 2: Low dose dopamine or any dobutamine
            elif (dopamine > 0 and dopamine <= 5) or dobutamine > 0:
                return (2, "Low dose dopamine or dobutamine")
        
        # If no significant vasopressors, use MAP
        if map_mmhg is not None:
            if map_mmhg >= 70:
                return (0, "MAP ≥ 70 mmHg")
            else:
                return (1, "MAP < 70 mmHg")
        
        return (0, "Normal cardiovascular function")
    
    @staticmethod
    def coagulation_score(platelets_k_ul):
        """
        Coagulation SOFA Score based on platelet count
        
        Args:
            platelets_k_ul (float): Platelet count in thousands/μL (×10³/μL)
            
        Returns:
            tuple: (score, description)
        """
        if platelets_k_ul is None:
            return (None, "Missing platelet count")
        
        if platelets_k_ul >= 150:
            return (0, "Platelets ≥ 150 × 10³/μL")
        elif platelets_k_ul < 150 and platelets_k_ul >= 100:
            return (1, "Platelets < 150 × 10³/μL")
        elif platelets_k_ul < 100 and platelets_k_ul >= 50:
            return (2, "Platelets < 100 × 10³/μL")
        elif platelets_k_ul < 50 and platelets_k_ul >= 20:
            return (3, "Platelets < 50 × 10³/μL")
        elif platelets_k_ul < 20:
            return (4, "Platelets < 20 × 10³/μL")
        else:
            return (0, "Normal coagulation")
    
    @staticmethod
    def liver_score(bilirubin_mg_dl):
        """
        Liver SOFA Score based on bilirubin level
        
        Args:
            bilirubin_mg_dl (float): Bilirubin level in mg/dL
            
        Returns:
            tuple: (score, description)
        """
        if bilirubin_mg_dl is None:
            return (None, "Missing bilirubin level")
        
        if bilirubin_mg_dl < 1.2:
            return (0, "Bilirubin < 1.2 mg/dL")
        elif bilirubin_mg_dl >= 1.2 and bilirubin_mg_dl <= 1.9:
            return (1, "Bilirubin 1.2-1.9 mg/dL")
        elif bilirubin_mg_dl >= 2.0 and bilirubin_mg_dl <= 5.9:
            return (2, "Bilirubin 2.0-5.9 mg/dL")
        elif bilirubin_mg_dl >= 6.0 and bilirubin_mg_dl <= 11.9:
            return (3, "Bilirubin 6.0-11.9 mg/dL")
        elif bilirubin_mg_dl >= 12.0:
            return (4, "Bilirubin ≥ 12.0 mg/dL")
        else:
            return (0, "Normal liver function")
    
    @staticmethod
    def cns_score(gcs_total):
        """
        Central Nervous System SOFA Score based on Glasgow Coma Scale
        
        Args:
            gcs_total (int): Total Glasgow Coma Scale score (3-15)
            
        Returns:
            tuple: (score, description)
        """
        if gcs_total is None:
            return (None, "Missing GCS score")
        
        if gcs_total == 15:
            return (0, "GCS = 15")
        elif gcs_total >= 13 and gcs_total <= 14:
            return (1, "GCS 13-14")
        elif gcs_total >= 10 and gcs_total <= 12:
            return (2, "GCS 10-12")
        elif gcs_total >= 6 and gcs_total <= 9:
            return (3, "GCS 6-9")
        elif gcs_total < 6:
            return (4, "GCS < 6")
        else:
            return (0, "Normal neurological function")
    
    @staticmethod
    def renal_score(creatinine_mg_dl, urine_output_ml_24h=None):
        """
        Renal SOFA Score based on creatinine level and/or urine output
        
        Args:
            creatinine_mg_dl (float): Creatinine level in mg/dL
            urine_output_ml_24h (float): Urine output in mL per 24 hours
            
        Returns:
            tuple: (score, description)
        """
        if creatinine_mg_dl is None and urine_output_ml_24h is None:
            return (None, "Missing creatinine and urine output")
        
        # Determine score based on creatinine
        creat_score = 0
        if creatinine_mg_dl is not None:
            if creatinine_mg_dl < 1.2:
                creat_score = 0
            elif creatinine_mg_dl >= 1.2 and creatinine_mg_dl <= 1.9:
                creat_score = 1
            elif creatinine_mg_dl >= 2.0 and creatinine_mg_dl <= 3.4:
                creat_score = 2
            elif creatinine_mg_dl >= 3.5 and creatinine_mg_dl <= 4.9:
                creat_score = 3
            elif creatinine_mg_dl >= 5.0:
                creat_score = 4
        
        # Determine score based on urine output
        urine_score = 0
        if urine_output_ml_24h is not None:
            if urine_output_ml_24h >= 500:
                urine_score = 0
            elif urine_output_ml_24h < 500 and urine_output_ml_24h >= 200:
                urine_score = 3
            elif urine_output_ml_24h < 200:
                urine_score = 4
        
        # Take the higher score
        final_score = max(creat_score, urine_score)
        
        # Generate description
        descriptions = {
            0: "Normal renal function",
            1: "Creatinine 1.2-1.9 mg/dL",
            2: "Creatinine 2.0-3.4 mg/dL", 
            3: "Creatinine 3.5-4.9 mg/dL or urine output < 500 mL/day",
            4: "Creatinine ≥ 5.0 mg/dL or urine output < 200 mL/day"
        }
        
        return (final_score, descriptions[final_score])

# =============================================================================
# OMOP CONCEPT MAPPINGS FOR SOFA PARAMETERS
# =============================================================================

SOFA_OMOP_CONCEPTS = {
    # Respiratory system
    'PaO2': 40762499,           # Partial pressure of oxygen in arterial blood
    'SpO2': 40764520,           # Oxygen saturation
    'FiO2': 4353936,            # Fraction of inspired oxygen
    
    # Cardiovascular system  
    'MAP': 3004249,             # Mean arterial pressure
    'dopamine': 1307046,        # Dopamine
    'epinephrine': 1343916,     # Epinephrine 
    'norepinephrine': 1344965,  # Norepinephrine
    'dobutamine': 1307863,      # Dobutamine
    
    # Coagulation
    'platelets': 3013650,       # Platelet count
    
    # Liver
    'bilirubin': 3017044,       # Bilirubin total
    
    # Central nervous system
    'gcs': 3012386,             # Glasgow coma scale total
    
    # Renal
    'creatinine': 3016723,      # Creatinine
    'urine_output': 3012110     # Urine output
}

# =============================================================================
# ARI PATIENT IDENTIFICATION 
# =============================================================================

ARI_ICD10_CODES = [
    'J96.0',    # Acute respiratory failure
    'J96.00',   # Acute respiratory failure, unspecified whether with hypoxia or hypercapnia
    'J96.01',   # Acute respiratory failure with hypoxia
    'J96.02',   # Acute respiratory failure with hypercapnia
    'J80',      # Acute respiratory distress syndrome
    'J44.0',    # Chronic obstructive pulmonary disease with acute lower respiratory infection
    'J44.1',    # Chronic obstructive pulmonary disease with acute exacerbation
    'J12',      # Viral pneumonia, not elsewhere classified
    'J13',      # Pneumonia due to Streptococcus pneumoniae
    'J14',      # Pneumonia due to Haemophilus influenzae
    'J15',      # Bacterial pneumonia, not elsewhere classified
    'J18',      # Pneumonia, unspecified organism
]

# =============================================================================
# IMPUTATION STRATEGIES
# =============================================================================

IMPUTATION_CONFIG = {
    'locf_max_hours': 24,       # Maximum hours to look back for LOCF
    'population_percentile': 50, # Use median for population imputation
    'min_data_threshold': 0.5,   # Skip patient if < 50% of SOFA components available
    'spo2_to_pao2_conversion': True,  # Use SpO2/FiO2 as surrogate for PaO2/FiO2
}

# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def calculate_total_sofa(subscores):
    """
    Calculate total SOFA score from individual subscores
    
    Args:
        subscores (dict): Dictionary with subscore values
        
    Returns:
        tuple: (total_score, missing_components)
    """
    score_components = ['respiratory', 'cardiovascular', 'coagulation', 
                       'liver', 'cns', 'renal']
    
    total = 0
    missing = []
    
    for component in score_components:
        if component in subscores and subscores[component] is not None:
            total += subscores[component]
        else:
            missing.append(component)
    
    return total, missing

def get_sofa_severity_category(total_score):
    """
    Categorize SOFA score severity
    
    Args:
        total_score (int): Total SOFA score
        
    Returns:
        str: Severity category
    """
    if total_score is None:
        return "Unknown"
    elif total_score == 0:
        return "Normal"
    elif total_score <= 6:
        return "Mild"
    elif total_score <= 12:
        return "Moderate" 
    elif total_score <= 18:
        return "Severe"
    else:
        return "Critical"

if __name__ == "__main__":
    # Test the SOFA mappings
    print("🧪 Testing SOFA Mappings...")
    
    # Test respiratory
    resp_score, resp_desc = SOFAMappings.respiratory_score(150, True)
    print(f"Respiratory: PaO2/FiO2=150, Ventilated=True → Score: {resp_score}, {resp_desc}")
    
    # Test cardiovascular
    cardio_score, cardio_desc = SOFAMappings.cardiovascular_score(60, {'dopamine': 10})
    print(f"Cardiovascular: MAP=60, Dopamine=10 → Score: {cardio_score}, {cardio_desc}")
    
    # Test total calculation
    subscores = {'respiratory': 3, 'cardiovascular': 2, 'coagulation': 1, 
                'liver': 0, 'cns': 1, 'renal': 2}
    total, missing = calculate_total_sofa(subscores)
    severity = get_sofa_severity_category(total)
    print(f"Total SOFA: {total}, Missing: {missing}, Severity: {severity}")
    
    print("✅ SOFA mappings initialized successfully!")
