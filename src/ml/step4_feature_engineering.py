#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Uebung 5: ML Implementation - Step 4: Feature Engineering & Temporal Splitting
Medical Data Science - ICU Mortality Prediction using SOFA Scores

This script performs advanced feature engineering and implements proper temporal
splitting for time-series mortality prediction, ensuring no data leakage.
"""

import sys
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.model_selection import train_test_split
import pickle

# Project imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.file_paths import get_log_path, get_report_path

# Setup project root
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

class FeatureEngineer:
    """Advanced feature engineering for ICU mortality prediction"""
    
    def __init__(self):
        self.setup_logging()
        self.df = None
        self.feature_cols = []
        self.target_col = 'target_mortality_48h'
        self.scalers = {}
        self.encoders = {}
        
    def setup_logging(self):
        """Setup logging configuration"""
        log_path = get_log_path('ml_feature_engineering.log')
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_path),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger('FeatureEngineer')
        
    def load_dataset(self):
        """Load the processed mortality prediction dataset"""
        self.logger.info("📂 Loading processed dataset...")
        
        csv_path = os.path.join(project_root, 'data', 'ml_dataset_48h_mortality.csv')
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"Dataset not found: {csv_path}")
            
        self.df = pd.read_csv(csv_path)
        
        # Convert datetime columns
        datetime_cols = ['charttime', 'window_start', 'window_end', 'icu_intime', 'icu_outtime', 
                        'prediction_timepoint', 'deathtime']
        for col in datetime_cols:
            if col in self.df.columns:
                self.df[col] = pd.to_datetime(self.df[col])
                
        self.logger.info(f"✅ Dataset loaded: {len(self.df):,} records, {len(self.df.columns)} features")
        
    def create_temporal_features(self):
        """Create temporal features from datetime columns"""
        self.logger.info("⏰ Creating temporal features...")
        
        # Time-based features from charttime
        if 'charttime' in self.df.columns:
            self.df['hour_of_day'] = self.df['charttime'].dt.hour
            self.df['day_of_week'] = self.df['charttime'].dt.dayofweek
            self.df['is_weekend'] = (self.df['day_of_week'].isin([5, 6])).astype(int)
            self.df['is_night_shift'] = ((self.df['hour_of_day'] >= 19) | (self.df['hour_of_day'] <= 7)).astype(int)
            
        # ICU length of stay features
        if 'icu_intime' in self.df.columns and 'icu_outtime' in self.df.columns:
            self.df['icu_los_hours'] = (self.df['icu_outtime'] - self.df['icu_intime']).dt.total_seconds() / 3600
            self.df['remaining_icu_hours'] = (self.df['icu_outtime'] - self.df['charttime']).dt.total_seconds() / 3600
            
        # Measurement timing features
        if 'hours_from_admission' in self.df.columns:
            self.df['is_early_measurement'] = (self.df['hours_from_admission'] <= 24).astype(int)
            self.df['is_late_measurement'] = (self.df['hours_from_admission'] >= 72).astype(int)
            self.df['admission_phase'] = pd.cut(self.df['hours_from_admission'], 
                                             bins=[0, 24, 48, 72, float('inf')], 
                                             labels=['0-24h', '24-48h', '48-72h', '>72h'])
            
        self.logger.info("✅ Temporal features created")
        
    def create_sofa_derived_features(self):
        """Create derived features from SOFA scores"""
        self.logger.info("📊 Creating SOFA-derived features...")
        
        sofa_components = ['respiratory_score', 'cardiovascular_score', 'hepatic_score', 
                          'coagulation_score', 'renal_score', 'neurological_score']
        
        # Count non-zero SOFA components
        available_sofa = [col for col in sofa_components if col in self.df.columns]
        if available_sofa:
            self.df['sofa_components_affected'] = (self.df[available_sofa] > 0).sum(axis=1)
            self.df['sofa_severity_high'] = (self.df['total_sofa_score'] >= 10).astype(int)
            self.df['sofa_severity_moderate'] = ((self.df['total_sofa_score'] >= 5) & 
                                               (self.df['total_sofa_score'] < 10)).astype(int)
            self.df['sofa_severity_mild'] = ((self.df['total_sofa_score'] >= 1) & 
                                           (self.df['total_sofa_score'] < 5)).astype(int)
            
            # Most critical organ system
            sofa_nonzero = self.df[available_sofa].copy()
            sofa_nonzero[sofa_nonzero == 0] = np.nan
            self.df['dominant_organ_failure'] = sofa_nonzero.idxmax(axis=1)
            
        self.logger.info("✅ SOFA-derived features created")
        
    def create_clinical_features(self):
        """Create derived clinical features"""
        self.logger.info("🔬 Creating clinical features...")
        
        # Blood pressure categories
        if 'mean_arterial_pressure' in self.df.columns:
            self.df['hypotensive'] = (self.df['mean_arterial_pressure'] < 65).astype(int)
            self.df['hypertensive'] = (self.df['mean_arterial_pressure'] > 100).astype(int)
            
        # Kidney function
        if 'creatinine_level' in self.df.columns:
            self.df['aki_stage'] = pd.cut(self.df['creatinine_level'],
                                        bins=[0, 1.2, 2.0, 3.0, float('inf')],
                                        labels=['normal', 'mild_aki', 'moderate_aki', 'severe_aki'])
            
        # Liver function
        if 'bilirubin_level' in self.df.columns:
            self.df['hyperbilirubinemia'] = (self.df['bilirubin_level'] > 2.0).astype(int)
            
        # Coagulation
        if 'platelet_count' in self.df.columns:
            self.df['thrombocytopenia'] = (self.df['platelet_count'] < 100).astype(int)
            self.df['severe_thrombocytopenia'] = (self.df['platelet_count'] < 50).astype(int)
            
        # Neurological
        if 'gcs_total' in self.df.columns:
            self.df['altered_consciousness'] = (self.df['gcs_total'] < 15).astype(int)
            self.df['severe_altered_consciousness'] = (self.df['gcs_total'] <= 8).astype(int)
            
        # Urine output
        if 'urine_output_24h' in self.df.columns:
            self.df['oliguria'] = (self.df['urine_output_24h'] < 400).astype(int)
            self.df['anuria'] = (self.df['urine_output_24h'] < 100).astype(int)
            
        self.logger.info("✅ Clinical features created")
        
    def create_interaction_features(self):
        """Create interaction features between important variables"""
        self.logger.info("🔗 Creating interaction features...")
        
        # SOFA score interactions with time
        if 'total_sofa_score' in self.df.columns and 'hours_from_admission' in self.df.columns:
            self.df['sofa_time_interaction'] = self.df['total_sofa_score'] * self.df['hours_from_admission']
            
        # Data completeness interactions
        if 'data_completeness_score' in self.df.columns:
            self.df['completeness_sofa_interaction'] = self.df['data_completeness_score'] * self.df['total_sofa_score']
            
        # Cardiovascular-renal interaction (common in ICU)
        if 'cardiovascular_score' in self.df.columns and 'renal_score' in self.df.columns:
            self.df['cardio_renal_interaction'] = self.df['cardiovascular_score'] * self.df['renal_score']
            
        self.logger.info("✅ Interaction features created")
        
    def handle_missing_values(self):
        """Handle missing values with domain-specific strategies"""
        self.logger.info("🔧 Handling missing values...")
        
        # Strategy 1: Clinical normal values
        clinical_normal_values = {
            'pao2_fio2_ratio': 400,  # Normal PaO2/FiO2 ratio
            'mean_arterial_pressure': 85,  # Normal MAP
            'bilirubin_level': 1.0,  # Normal bilirubin
            'platelet_count': 250,  # Normal platelet count
            'creatinine_level': 1.0,  # Normal creatinine
            'urine_output_24h': 1500,  # Normal urine output
            'gcs_total': 15  # Normal GCS
        }
        
        # Strategy 2: Forward fill for temporal data (within patient)
        temporal_cols = ['mean_arterial_pressure', 'creatinine_level', 'platelet_count']
        for col in temporal_cols:
            if col in self.df.columns:
                self.df[col] = self.df.groupby('stay_id')[col].fillna(method='ffill')
                
        # Strategy 3: Fill remaining with clinical normal values
        for col, normal_val in clinical_normal_values.items():
            if col in self.df.columns:
                self.df[col] = self.df[col].fillna(normal_val)
                
        # Strategy 4: Create missingness indicators for important features
        important_features = ['bilirubin_level', 'gcs_total', 'urine_output_24h']
        for col in important_features:
            if col in self.df.columns:
                self.df[f'{col}_missing'] = self.df[col].isnull().astype(int)
                
        self.logger.info("✅ Missing values handled")
        
    def encode_categorical_features(self):
        """Encode categorical features"""
        self.logger.info("🏷️ Encoding categorical features...")
        
        # Label encode ordinal features
        ordinal_features = ['admission_phase', 'aki_stage']
        for col in ordinal_features:
            if col in self.df.columns and self.df[col].notna().any():
                le = LabelEncoder()
                non_null_mask = self.df[col].notna()
                self.df.loc[non_null_mask, f'{col}_encoded'] = le.fit_transform(self.df.loc[non_null_mask, col])
                self.encoders[col] = le
                
        # One-hot encode nominal features
        nominal_features = ['dominant_organ_failure']
        for col in nominal_features:
            if col in self.df.columns and self.df[col].notna().any():
                dummies = pd.get_dummies(self.df[col], prefix=col, dummy_na=True)
                self.df = pd.concat([self.df, dummies], axis=1)
                
        self.logger.info("✅ Categorical features encoded")
        
    def select_features(self):
        """Select features for modeling"""
        self.logger.info("🎯 Selecting features for modeling...")
        
        # Base SOFA features
        sofa_features = ['respiratory_score', 'cardiovascular_score', 'hepatic_score', 
                        'coagulation_score', 'renal_score', 'neurological_score', 'total_sofa_score']
        
        # Clinical features
        clinical_features = ['mean_arterial_pressure', 'bilirubin_level', 'platelet_count', 
                           'creatinine_level', 'urine_output_24h', 'gcs_total']
        
        # Temporal features
        temporal_features = ['hours_from_admission', 'hour_of_day', 'day_of_week', 'is_weekend', 
                           'is_night_shift', 'is_early_measurement', 'is_late_measurement']
        
        # Derived features
        derived_features = ['sofa_components_affected', 'sofa_severity_high', 'sofa_severity_moderate',
                          'hypotensive', 'hypertensive', 'thrombocytopenia', 'altered_consciousness',
                          'oliguria', 'data_completeness_score']
        
        # Interaction features
        interaction_features = ['sofa_time_interaction', 'completeness_sofa_interaction', 
                              'cardio_renal_interaction']
        
        # Missingness indicators
        missingness_features = [col for col in self.df.columns if col.endswith('_missing')]
        
        # Encoded categorical features
        encoded_features = [col for col in self.df.columns if col.endswith('_encoded')]
        
        # One-hot encoded features
        onehot_features = [col for col in self.df.columns if col.startswith('dominant_organ_failure_')]
        
        # Combine all feature categories
        all_potential_features = (sofa_features + clinical_features + temporal_features + 
                                derived_features + interaction_features + missingness_features + 
                                encoded_features + onehot_features)
        
        # Filter to only existing columns
        self.feature_cols = [col for col in all_potential_features if col in self.df.columns]
        
        self.logger.info(f"✅ Selected {len(self.feature_cols)} features for modeling")
        return self.feature_cols
        
    def temporal_train_test_split(self, test_size=0.2, validation_size=0.1):
        """Perform temporal split to prevent data leakage"""
        self.logger.info("📅 Performing temporal train/validation/test split...")
        
        # Sort by prediction timepoint to ensure temporal order
        self.df = self.df.sort_values('prediction_timepoint')
        
        # Calculate split indices
        n_total = len(self.df)
        n_test = int(n_total * test_size)
        n_val = int(n_total * validation_size)
        n_train = n_total - n_test - n_val
        
        # Temporal split points
        train_end_idx = n_train
        val_end_idx = n_train + n_val
        
        # Create splits
        train_df = self.df.iloc[:train_end_idx].copy()
        val_df = self.df.iloc[train_end_idx:val_end_idx].copy()
        test_df = self.df.iloc[val_end_idx:].copy()
        
        # Extract features and targets
        X_train = train_df[self.feature_cols]
        y_train = train_df[self.target_col]
        
        X_val = val_df[self.feature_cols]
        y_val = val_df[self.target_col]
        
        X_test = test_df[self.feature_cols]
        y_test = test_df[self.target_col]
        
        # Log split information
        self.logger.info(f"📊 Train set: {len(X_train):,} samples ({y_train.mean()*100:.2f}% positive)")
        self.logger.info(f"📊 Validation set: {len(X_val):,} samples ({y_val.mean()*100:.2f}% positive)")
        self.logger.info(f"📊 Test set: {len(X_test):,} samples ({y_test.mean()*100:.2f}% positive)")
        
        # Temporal boundaries
        train_time_range = (train_df['prediction_timepoint'].min(), train_df['prediction_timepoint'].max())
        val_time_range = (val_df['prediction_timepoint'].min(), val_df['prediction_timepoint'].max())
        test_time_range = (test_df['prediction_timepoint'].min(), test_df['prediction_timepoint'].max())
        
        self.logger.info(f"📅 Train time range: {train_time_range[0]} to {train_time_range[1]}")
        self.logger.info(f"📅 Validation time range: {val_time_range[0]} to {val_time_range[1]}")
        self.logger.info(f"📅 Test time range: {test_time_range[0]} to {test_time_range[1]}")
        
        return (X_train, X_val, X_test, y_train, y_val, y_test, 
                train_df, val_df, test_df)
        
    def scale_features(self, X_train, X_val, X_test):
        """Scale features using training set statistics"""
        self.logger.info("⚖️ Scaling features...")
        
        # Identify numeric features to scale
        numeric_features = X_train.select_dtypes(include=[np.number]).columns
        
        # Fit scaler on training data only
        scaler = StandardScaler()
        X_train_scaled = X_train.copy()
        X_val_scaled = X_val.copy()
        X_test_scaled = X_test.copy()
        
        X_train_scaled[numeric_features] = scaler.fit_transform(X_train[numeric_features])
        X_val_scaled[numeric_features] = scaler.transform(X_val[numeric_features])
        X_test_scaled[numeric_features] = scaler.transform(X_test[numeric_features])
        
        self.scalers['standard_scaler'] = scaler
        
        self.logger.info(f"✅ Scaled {len(numeric_features)} numeric features")
        
        return X_train_scaled, X_val_scaled, X_test_scaled
        
    def save_processed_data(self, splits, feature_info):
        """Save processed data and metadata"""
        self.logger.info("💾 Saving processed data...")
        
        X_train, X_val, X_test, y_train, y_val, y_test, train_df, val_df, test_df = splits
        
        # Create processed data directory
        processed_dir = os.path.join(project_root, 'data', 'processed')
        os.makedirs(processed_dir, exist_ok=True)
        
        # Save splits
        splits_to_save = {
            'X_train': X_train, 'X_val': X_val, 'X_test': X_test,
            'y_train': y_train, 'y_val': y_val, 'y_test': y_test
        }
        
        for name, data in splits_to_save.items():
            if isinstance(data, pd.DataFrame):
                data.to_csv(os.path.join(processed_dir, f'{name}.csv'), index=False)
            else:  # Series
                data.to_csv(os.path.join(processed_dir, f'{name}.csv'), index=False, header=[self.target_col])
                
        # Save metadata
        metadata = {
            'feature_columns': self.feature_cols,
            'target_column': self.target_col,
            'n_features': len(self.feature_cols),
            'train_size': len(X_train),
            'val_size': len(X_val),
            'test_size': len(X_test),
            'feature_info': feature_info
        }
        
        with open(os.path.join(processed_dir, 'metadata.pickle'), 'wb') as f:
            pickle.dump(metadata, f)
            
        # Save scalers and encoders
        with open(os.path.join(processed_dir, 'scalers.pickle'), 'wb') as f:
            pickle.dump(self.scalers, f)
            
        with open(os.path.join(processed_dir, 'encoders.pickle'), 'wb') as f:
            pickle.dump(self.encoders, f)
            
        self.logger.info(f"✅ Processed data saved to: {processed_dir}")
        
        return processed_dir
        
    def generate_feature_report(self, splits, feature_info):
        """Generate comprehensive feature engineering report"""
        X_train, X_val, X_test, y_train, y_val, y_test, train_df, val_df, test_df = splits
        
        report_path = get_report_path('ml_feature_engineering_report.md')
        
        with open(report_path, 'w') as f:
            f.write("# Übung 5 - Feature Engineering & Temporal Splitting Report\n\n")
            f.write(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            f.write("## Feature Engineering Summary\n\n")
            f.write(f"- **Original Features:** {len(self.df.columns)}\n")
            f.write(f"- **Final Features:** {len(self.feature_cols)}\n")
            f.write(f"- **Feature Categories:** {len(feature_info)} types\n\n")
            
            f.write("### Feature Categories\n")
            for category, features in feature_info.items():
                f.write(f"- **{category}:** {len(features)} features\n")
                
            f.write("\n## Temporal Split Results\n\n")
            f.write(f"- **Training Set:** {len(X_train):,} samples ({y_train.mean()*100:.2f}% positive)\n")
            f.write(f"- **Validation Set:** {len(X_val):,} samples ({y_val.mean()*100:.2f}% positive)\n")
            f.write(f"- **Test Set:** {len(X_test):,} samples ({y_test.mean()*100:.2f}% positive)\n\n")
            
            f.write("### Temporal Boundaries\n")
            f.write(f"- **Train Period:** {train_df['prediction_timepoint'].min()} to {train_df['prediction_timepoint'].max()}\n")
            f.write(f"- **Validation Period:** {val_df['prediction_timepoint'].min()} to {val_df['prediction_timepoint'].max()}\n")
            f.write(f"- **Test Period:** {test_df['prediction_timepoint'].min()} to {test_df['prediction_timepoint'].max()}\n\n")
            
            f.write("## Data Quality\n\n")
            missing_after = X_train.isnull().sum().sum()
            f.write(f"- **Missing Values After Processing:** {missing_after}\n")
            f.write(f"- **Feature Scaling:** StandardScaler applied to numeric features\n")
            f.write(f"- **Categorical Encoding:** Label and One-hot encoding applied\n\n")
            
            f.write("## Next Steps\n\n")
            f.write("1. **Step 5:** Handle Class Imbalance (SMOTE, Class Weights)\n")
            f.write("2. **Step 6:** Baseline Model Implementation\n")
            f.write("3. **Step 7:** Advanced Model Development\n")
            f.write("4. **Step 8:** Model Evaluation & Validation\n")
            f.write("5. **Step 9:** Explainable AI (SHAP)\n")
            
        self.logger.info(f"📄 Feature engineering report saved: {report_path}")

def main():
    """Main execution function"""
    print("🚀 Starting Step 4: Feature Engineering & Temporal Splitting")
    print("=" * 60)
    
    try:
        # Initialize feature engineer
        engineer = FeatureEngineer()
        
        # Load and process data
        engineer.load_dataset()
        engineer.create_temporal_features()
        engineer.create_sofa_derived_features()
        engineer.create_clinical_features()
        engineer.create_interaction_features()
        engineer.handle_missing_values()
        engineer.encode_categorical_features()
        
        # Select features
        feature_cols = engineer.select_features()
        
        # Create feature info for reporting
        feature_info = {
            'SOFA Features': [col for col in feature_cols if 'score' in col],
            'Clinical Features': [col for col in feature_cols if col in ['mean_arterial_pressure', 'bilirubin_level', 'platelet_count', 'creatinine_level', 'urine_output_24h', 'gcs_total']],
            'Temporal Features': [col for col in feature_cols if any(x in col for x in ['hour', 'day', 'time', 'admission'])],
            'Derived Features': [col for col in feature_cols if any(x in col for x in ['hypotensive', 'thrombocytopenia', 'oliguria', 'severity'])],
            'Interaction Features': [col for col in feature_cols if 'interaction' in col],
            'Missingness Indicators': [col for col in feature_cols if col.endswith('_missing')]
        }
        
        # Temporal split
        splits = engineer.temporal_train_test_split()
        X_train, X_val, X_test, y_train, y_val, y_test, train_df, val_df, test_df = splits
        
        # Scale features
        X_train_scaled, X_val_scaled, X_test_scaled = engineer.scale_features(X_train, X_val, X_test)
        
        # Update splits with scaled features
        scaled_splits = (X_train_scaled, X_val_scaled, X_test_scaled, 
                        y_train, y_val, y_test, train_df, val_df, test_df)
        
        # Save processed data
        processed_dir = engineer.save_processed_data(scaled_splits, feature_info)
        
        # Generate report
        engineer.generate_feature_report(scaled_splits, feature_info)
        
        print("\n✅ Step 4 completed successfully!")
        print(f"📊 {len(feature_cols)} features engineered")
        print(f"💾 Processed data saved to: {processed_dir}")
        print("📋 Check docs/reports/ml_feature_engineering_report.md for details")
        print("\n🔄 Ready to proceed to Step 5: Class Imbalance Handling")
        
        return 0
        
    except Exception as e:
        print(f"❌ Feature engineering failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit(main())
