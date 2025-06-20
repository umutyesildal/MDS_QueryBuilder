#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Uebung 5: ML Implementation - Step 5: Baseline Model Implementation
Medical Data Science - ICU Mortality Prediction using SOFA Scores

This script implements baseline models with class imbalance handling for 
48-hour ICU mortality prediction using only professor-recommended libraries.
"""

import sys
import os
import pandas as pd
import numpy as np
import pickle
import logging
from datetime import datetime

# Scikit-learn imports (professor recommended)
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (classification_report, confusion_matrix, 
                           roc_auc_score, average_precision_score, roc_curve, 
                           precision_recall_curve, f1_score)
from sklearn.utils.class_weight import compute_class_weight

# Imbalanced-learn for handling class imbalance
from imblearn.over_sampling import SMOTE
from imblearn.combine import SMOTETomek

import matplotlib.pyplot as plt
import seaborn as sns

# SHAP for explainability (professor recommended)
import shap

# Project imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.file_paths import get_log_path, get_report_path

# Setup project root
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

class BaselineModelTrainer:
    """Baseline model training with class imbalance handling"""
    
    def __init__(self):
        self.setup_logging()
        self.models = {}
        self.results = {}
        self.processed_dir = os.path.join(project_root, 'data', 'processed')
        self.models_dir = os.path.join(project_root, 'models')
        self.figures_dir = os.path.join(project_root, 'docs', 'visualizations', 'models')
        
        # Create directories
        os.makedirs(self.models_dir, exist_ok=True)
        os.makedirs(self.figures_dir, exist_ok=True)
        
        # Set plotting style
        plt.style.use('seaborn-v0_8')
        sns.set_palette("husl")
        
    def setup_logging(self):
        """Setup logging configuration"""
        log_path = get_log_path('ml_baseline_models.log')
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_path),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger('BaselineTrainer')
        
    def load_processed_data(self):
        """Load processed train/validation/test data"""
        self.logger.info("📂 Loading processed data...")
        
        # Load datasets
        X_train = pd.read_csv(os.path.join(self.processed_dir, 'X_train.csv'))
        X_val = pd.read_csv(os.path.join(self.processed_dir, 'X_val.csv'))
        X_test = pd.read_csv(os.path.join(self.processed_dir, 'X_test.csv'))
        
        y_train = pd.read_csv(os.path.join(self.processed_dir, 'y_train.csv')).iloc[:, 0]
        y_val = pd.read_csv(os.path.join(self.processed_dir, 'y_val.csv')).iloc[:, 0]
        y_test = pd.read_csv(os.path.join(self.processed_dir, 'y_test.csv')).iloc[:, 0]
        
        # Handle any remaining missing values
        X_train = X_train.fillna(0)
        X_val = X_val.fillna(0)
        X_test = X_test.fillna(0)
        
        # Load metadata
        with open(os.path.join(self.processed_dir, 'metadata.pickle'), 'rb') as f:
            self.metadata = pickle.load(f)
            
        self.logger.info(f"✅ Data loaded - Train: {len(X_train)}, Val: {len(X_val)}, Test: {len(X_test)}")
        
        return X_train, X_val, X_test, y_train, y_val, y_test
        
    def handle_class_imbalance(self, X_train, y_train):
        """Handle class imbalance using multiple strategies"""
        self.logger.info("⚖️ Handling class imbalance...")
        
        strategies = {}
        
        # Strategy 1: Class weights
        class_weights = compute_class_weight('balanced', classes=np.unique(y_train), y=y_train)
        class_weight_dict = {0: class_weights[0], 1: class_weights[1]}
        strategies['class_weights'] = class_weight_dict
        
        # Strategy 2: SMOTE oversampling
        smote = SMOTE(random_state=42, k_neighbors=3)  # Use fewer neighbors due to small dataset
        X_smote, y_smote = smote.fit_resample(X_train, y_train)
        strategies['smote'] = (X_smote, y_smote)
        
        # Strategy 3: SMOTE + Tomek Links (hybrid)
        smote_tomek = SMOTETomek(random_state=42, smote=SMOTE(k_neighbors=3))
        X_smote_tomek, y_smote_tomek = smote_tomek.fit_resample(X_train, y_train)
        strategies['smote_tomek'] = (X_smote_tomek, y_smote_tomek)
        
        # Log results
        self.logger.info(f"📊 Original distribution: {np.bincount(y_train)}")
        self.logger.info(f"📊 SMOTE distribution: {np.bincount(y_smote)}")
        self.logger.info(f"📊 SMOTE+Tomek distribution: {np.bincount(y_smote_tomek)}")
        self.logger.info(f"📊 Class weights: {class_weight_dict}")
        
        return strategies
        
    def train_baseline_models(self, strategies, X_train, X_val, y_train, y_val):
        """Train baseline models with different imbalance handling strategies"""
        self.logger.info("🤖 Training baseline models...")
        
        # Model configurations
        model_configs = {
            'logistic_regression': LogisticRegression(random_state=42, max_iter=1000),
            'random_forest': RandomForestClassifier(random_state=42, n_estimators=100)
        }
        
        results = {}
        
        for model_name, base_model in model_configs.items():
            self.logger.info(f"🔧 Training {model_name}...")
            
            model_results = {}
            
            # 1. No imbalance handling (baseline)
            model_no_balance = type(base_model)(**base_model.get_params())
            model_no_balance.fit(X_train, y_train)
            val_pred_no_balance = model_no_balance.predict_proba(X_val)[:, 1]
            model_results['no_balance'] = {
                'model': model_no_balance,
                'val_predictions': val_pred_no_balance,
                'strategy': 'none'
            }
            
            # 2. Class weights
            if hasattr(base_model, 'class_weight'):
                model_weights = type(base_model)(**{**base_model.get_params(), 'class_weight': strategies['class_weights']})
                model_weights.fit(X_train, y_train)
                val_pred_weights = model_weights.predict_proba(X_val)[:, 1]
                model_results['class_weights'] = {
                    'model': model_weights,
                    'val_predictions': val_pred_weights,
                    'strategy': 'class_weights'
                }
            
            # 3. SMOTE
            X_smote, y_smote = strategies['smote']
            model_smote = type(base_model)(**base_model.get_params())
            model_smote.fit(X_smote, y_smote)
            val_pred_smote = model_smote.predict_proba(X_val)[:, 1]
            model_results['smote'] = {
                'model': model_smote,
                'val_predictions': val_pred_smote,
                'strategy': 'smote'
            }
            
            # 4. SMOTE + Tomek
            X_smote_tomek, y_smote_tomek = strategies['smote_tomek']
            model_smote_tomek = type(base_model)(**base_model.get_params())
            model_smote_tomek.fit(X_smote_tomek, y_smote_tomek)
            val_pred_smote_tomek = model_smote_tomek.predict_proba(X_val)[:, 1]
            model_results['smote_tomek'] = {
                'model': model_smote_tomek,
                'val_predictions': val_pred_smote_tomek,
                'strategy': 'smote_tomek'
            }
            
            results[model_name] = model_results
            
        self.models = results
        return results
        
    def evaluate_models(self, models, X_val, y_val, X_test, y_test):
        """Comprehensive model evaluation"""
        self.logger.info("📊 Evaluating models...")
        
        evaluation_results = {}
        
        for model_name, model_variants in models.items():
            self.logger.info(f"📈 Evaluating {model_name}...")
            
            variant_results = {}
            
            for variant_name, variant_info in model_variants.items():
                model = variant_info['model']
                strategy = variant_info['strategy']
                
                # Validation predictions
                val_pred_proba = model.predict_proba(X_val)[:, 1]
                val_pred_binary = (val_pred_proba >= 0.5).astype(int)
                
                # Test predictions
                test_pred_proba = model.predict_proba(X_test)[:, 1]
                test_pred_binary = (test_pred_proba >= 0.5).astype(int)
                
                # Calculate metrics
                val_metrics = self.calculate_metrics(y_val, val_pred_binary, val_pred_proba)
                test_metrics = self.calculate_metrics(y_test, test_pred_binary, test_pred_proba)
                
                variant_results[variant_name] = {
                    'model': model,
                    'strategy': strategy,
                    'val_metrics': val_metrics,
                    'test_metrics': test_metrics,
                    'val_pred_proba': val_pred_proba,
                    'test_pred_proba': test_pred_proba
                }
                
                # Log key metrics
                self.logger.info(f"  {variant_name} - Val AUC: {val_metrics['roc_auc']:.3f}, Test AUC: {test_metrics['roc_auc']:.3f}")
                
            evaluation_results[model_name] = variant_results
            
        self.results = evaluation_results
        return evaluation_results
        
    def calculate_metrics(self, y_true, y_pred_binary, y_pred_proba):
        """Calculate comprehensive evaluation metrics"""
        metrics = {
            'accuracy': (y_pred_binary == y_true).mean(),
            'precision': np.sum((y_pred_binary == 1) & (y_true == 1)) / max(np.sum(y_pred_binary == 1), 1),
            'recall': np.sum((y_pred_binary == 1) & (y_true == 1)) / max(np.sum(y_true == 1), 1),
            'f1': f1_score(y_true, y_pred_binary, zero_division=0),
            'roc_auc': roc_auc_score(y_true, y_pred_proba) if len(np.unique(y_true)) > 1 else 0,
            'pr_auc': average_precision_score(y_true, y_pred_proba) if len(np.unique(y_true)) > 1 else 0
        }
        
        # Handle edge cases
        if np.sum(y_true == 1) == 0:  # No positive cases
            metrics['recall'] = 0
            metrics['pr_auc'] = 0
            
        return metrics
        
    def select_best_models(self):
        """Select best performing models based on validation metrics"""
        self.logger.info("🏆 Selecting best models...")
        
        best_models = {}
        
        for model_name, variants in self.results.items():
            # Select best variant based on validation ROC-AUC
            best_variant = max(variants.items(), 
                             key=lambda x: x[1]['val_metrics']['roc_auc'])
            
            best_models[model_name] = {
                'variant_name': best_variant[0],
                'model': best_variant[1]['model'],
                'strategy': best_variant[1]['strategy'],
                'val_auc': best_variant[1]['val_metrics']['roc_auc'],
                'test_auc': best_variant[1]['test_metrics']['roc_auc']
            }
            
            self.logger.info(f"🏆 Best {model_name}: {best_variant[0]} (Val AUC: {best_variant[1]['val_metrics']['roc_auc']:.3f})")
            
        # Overall best model
        overall_best = max(best_models.items(), 
                          key=lambda x: x[1]['val_auc'])
        
        self.logger.info(f"🥇 Overall best model: {overall_best[0]} - {overall_best[1]['variant_name']}")
        
        return best_models, overall_best

def main():
    """Main execution function"""
    print("🚀 Starting Step 5: Baseline Model Implementation")
    print("=" * 60)
    
    try:
        # Initialize trainer
        trainer = BaselineModelTrainer()
        
        # Load processed data
        X_train, X_val, X_test, y_train, y_val, y_test = trainer.load_processed_data()
        
        # Handle class imbalance
        strategies = trainer.handle_class_imbalance(X_train, y_train)
        
        # Train models
        models = trainer.train_baseline_models(strategies, X_train, X_val, y_train, y_val)
        
        # Evaluate models
        results = trainer.evaluate_models(models, X_val, y_val, X_test, y_test)
        
        # Select best models
        best_models, overall_best = trainer.select_best_models()
        
        print("\n✅ Step 5 completed successfully!")
        print(f"🏆 Best model: {overall_best[0]} ({overall_best[1]['variant_name']})")
        print(f"📊 Test AUC: {overall_best[1]['test_auc']:.3f}")
        print("\n🎉 Übung 5 ML Implementation Complete!")
        
        return 0
        
    except Exception as e:
        print(f"❌ Baseline modeling failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit(main())
