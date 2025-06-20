#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Uebung 5: ML Implementation - Step 3: Exploratory Data Analysis (EDA)
Medical Data Science - ICU Mortality Prediction using SOFA Scores

This script performs comprehensive exploratory data analysis on the 48-hour
mortality prediction dataset to understand patterns, distributions, and 
relationships in the data.
"""

import sys
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import logging

# Project imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.file_paths import get_log_path, get_report_path

# Setup project root
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

class ExploratoryDataAnalyzer:
    """Comprehensive EDA for ICU mortality prediction dataset"""
    
    def __init__(self):
        self.setup_logging()
        self.df = None
        self.figures_dir = os.path.join(project_root, 'docs', 'visualizations', 'eda')
        os.makedirs(self.figures_dir, exist_ok=True)
        
        # Set plotting style
        plt.style.use('seaborn-v0_8')
        sns.set_palette("husl")
        
    def setup_logging(self):
        """Setup logging configuration"""
        log_path = get_log_path('ml_eda.log')
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_path),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger('EDAAnalyzer')
        
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
        
    def basic_data_overview(self):
        """Generate basic data overview and statistics"""
        self.logger.info("📊 Generating basic data overview...")
        
        print("="*60)
        print("DATASET OVERVIEW")
        print("="*60)
        
        print(f"📊 Shape: {self.df.shape}")
        print(f"👥 Unique patients: {self.df['subject_id'].nunique()}")
        print(f"🏥 Unique ICU stays: {self.df['stay_id'].nunique()}")
        print(f"💀 48h mortality cases: {self.df['target_mortality_48h'].sum()}")
        print(f"📈 48h mortality rate: {self.df['target_mortality_48h'].mean()*100:.2f}%")
        
        print("\n" + "="*60)
        print("DATA TYPES")
        print("="*60)
        print(self.df.dtypes.value_counts())
        
        print("\n" + "="*60)
        print("MISSING VALUES")
        print("="*60)
        missing_counts = self.df.isnull().sum()
        missing_pct = (missing_counts / len(self.df)) * 100
        missing_summary = pd.DataFrame({
            'Missing Count': missing_counts,
            'Missing %': missing_pct
        }).sort_values('Missing %', ascending=False)
        print(missing_summary[missing_summary['Missing Count'] > 0])
        
    def analyze_target_distribution(self):
        """Analyze target variable distribution"""
        self.logger.info("🎯 Analyzing target variable distribution...")
        
        fig, axes = plt.subplots(1, 2, figsize=(15, 6))
        
        # Target distribution
        target_counts = self.df['target_mortality_48h'].value_counts()
        axes[0].pie(target_counts.values, labels=['Survived', '48h Mortality'], 
                   autopct='%1.1f%%', startangle=90)
        axes[0].set_title('48-Hour Mortality Distribution')
        
        # Mortality rate by time from admission
        self.df['hours_bin'] = pd.cut(self.df['hours_from_admission'], 
                                      bins=10, labels=False)
        mortality_by_time = self.df.groupby('hours_bin')['target_mortality_48h'].agg(['mean', 'count'])
        axes[1].bar(range(10), mortality_by_time['mean'] * 100)
        axes[1].set_xlabel('Time from Admission (binned)')
        axes[1].set_ylabel('Mortality Rate (%)')
        axes[1].set_title('Mortality Rate by Time from Admission')
        
        plt.tight_layout()
        plt.savefig(os.path.join(self.figures_dir, 'target_distribution.png'), dpi=300, bbox_inches='tight')
        plt.show()
        
    def analyze_sofa_distributions(self):
        """Analyze SOFA score distributions"""
        self.logger.info("📊 Analyzing SOFA score distributions...")
        
        sofa_cols = ['respiratory_score', 'cardiovascular_score', 'hepatic_score', 
                     'coagulation_score', 'renal_score', 'neurological_score', 'total_sofa_score']
        
        fig, axes = plt.subplots(2, 4, figsize=(20, 10))
        axes = axes.flatten()
        
        for i, col in enumerate(sofa_cols):
            if col in self.df.columns:
                # Distribution by mortality outcome
                for outcome in [0, 1]:
                    subset = self.df[self.df['target_mortality_48h'] == outcome]
                    axes[i].hist(subset[col], alpha=0.7, bins=range(0, 6), 
                               label=f'Mortality: {outcome}', density=True)
                
                axes[i].set_title(f'{col.replace("_", " ").title()}')
                axes[i].set_xlabel('Score')
                axes[i].set_ylabel('Density')
                axes[i].legend()
                
        # Remove empty subplot
        fig.delaxes(axes[-1])
        
        plt.tight_layout()
        plt.savefig(os.path.join(self.figures_dir, 'sofa_distributions.png'), dpi=300, bbox_inches='tight')
        plt.show()
        
    def analyze_clinical_parameters(self):
        """Analyze clinical parameter distributions"""
        self.logger.info("🔬 Analyzing clinical parameter distributions...")
        
        clinical_cols = ['mean_arterial_pressure', 'bilirubin_level', 'platelet_count', 
                        'creatinine_level', 'urine_output_24h', 'gcs_total']
        
        # Filter columns that exist and have some non-null values
        available_cols = [col for col in clinical_cols if col in self.df.columns and not self.df[col].isnull().all()]
        
        if not available_cols:
            self.logger.warning("No clinical parameters available for analysis")
            return
            
        n_cols = len(available_cols)
        n_rows = (n_cols + 2) // 3
        
        fig, axes = plt.subplots(n_rows, 3, figsize=(18, 6*n_rows))
        if n_rows == 1:
            axes = axes.reshape(1, -1)
        axes = axes.flatten()
        
        for i, col in enumerate(available_cols):
            # Box plot by mortality outcome
            data_for_plot = []
            labels = []
            for outcome in [0, 1]:
                subset_data = self.df[self.df['target_mortality_48h'] == outcome][col].dropna()
                if len(subset_data) > 0:
                    data_for_plot.append(subset_data)
                    labels.append(f'Mortality: {outcome}')
            
            if data_for_plot:
                axes[i].boxplot(data_for_plot, labels=labels)
                axes[i].set_title(f'{col.replace("_", " ").title()}')
                axes[i].set_ylabel(col.replace("_", " ").title())
                
        # Remove empty subplots
        for i in range(len(available_cols), len(axes)):
            fig.delaxes(axes[i])
            
        plt.tight_layout()
        plt.savefig(os.path.join(self.figures_dir, 'clinical_parameters.png'), dpi=300, bbox_inches='tight')
        plt.show()
        
    def analyze_temporal_patterns(self):
        """Analyze temporal patterns in the data"""
        self.logger.info("⏰ Analyzing temporal patterns...")
        
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        
        # 1. Hours from admission distribution
        axes[0,0].hist(self.df['hours_from_admission'], bins=30, alpha=0.7, edgecolor='black')
        axes[0,0].set_xlabel('Hours from ICU Admission')
        axes[0,0].set_ylabel('Frequency')
        axes[0,0].set_title('Distribution of Measurement Times')
        
        # 2. Mortality by time windows
        time_bins = [0, 24, 48, 72, 168, float('inf')]  # 0-24h, 24-48h, 48-72h, 72h-1week, >1week
        time_labels = ['0-24h', '24-48h', '48-72h', '72h-1w', '>1week']
        self.df['time_window'] = pd.cut(self.df['hours_from_admission'], bins=time_bins, labels=time_labels)
        
        mortality_by_window = self.df.groupby('time_window')['target_mortality_48h'].agg(['mean', 'count'])
        axes[0,1].bar(range(len(mortality_by_window)), mortality_by_window['mean'] * 100)
        axes[0,1].set_xlabel('Time Window from Admission')
        axes[0,1].set_ylabel('Mortality Rate (%)')
        axes[0,1].set_title('Mortality Rate by Time Windows')
        axes[0,1].set_xticks(range(len(time_labels)))
        axes[0,1].set_xticklabels(time_labels, rotation=45)
        
        # 3. SOFA progression over time (for patients with multiple measurements)
        patient_counts = self.df['stay_id'].value_counts()
        multi_measurement_stays = patient_counts[patient_counts > 1].index
        
        if len(multi_measurement_stays) > 0:
            multi_df = self.df[self.df['stay_id'].isin(multi_measurement_stays)]
            # Sample some stays for visualization
            sample_stays = multi_measurement_stays[:10] if len(multi_measurement_stays) > 10 else multi_measurement_stays
            
            for stay_id in sample_stays:
                stay_data = multi_df[multi_df['stay_id'] == stay_id].sort_values('hours_from_admission')
                axes[1,0].plot(stay_data['hours_from_admission'], stay_data['total_sofa_score'], 
                              alpha=0.6, marker='o', markersize=4)
                
            axes[1,0].set_xlabel('Hours from Admission')
            axes[1,0].set_ylabel('Total SOFA Score')
            axes[1,0].set_title('SOFA Score Trajectories (Sample Patients)')
        
        # 4. Data completeness over time
        completeness_by_time = self.df.groupby('time_window')['data_completeness_score'].mean()
        axes[1,1].bar(range(len(completeness_by_time)), completeness_by_time)
        axes[1,1].set_xlabel('Time Window from Admission')
        axes[1,1].set_ylabel('Average Completeness Score')
        axes[1,1].set_title('Data Completeness by Time Windows')
        axes[1,1].set_xticks(range(len(time_labels)))
        axes[1,1].set_xticklabels(time_labels, rotation=45)
        
        plt.tight_layout()
        plt.savefig(os.path.join(self.figures_dir, 'temporal_patterns.png'), dpi=300, bbox_inches='tight')
        plt.show()
        
    def correlation_analysis(self):
        """Perform correlation analysis"""
        self.logger.info("🔗 Performing correlation analysis...")
        
        # Select numeric columns for correlation
        numeric_cols = self.df.select_dtypes(include=[np.number]).columns
        numeric_cols = [col for col in numeric_cols if col not in ['id', 'subject_id', 'stay_id']]
        
        if len(numeric_cols) < 2:
            self.logger.warning("Not enough numeric columns for correlation analysis")
            return
            
        corr_matrix = self.df[numeric_cols].corr()
        
        # Create correlation heatmap
        plt.figure(figsize=(14, 12))
        mask = np.triu(np.ones_like(corr_matrix, dtype=bool))
        sns.heatmap(corr_matrix, mask=mask, annot=True, cmap='coolwarm', center=0,
                   square=True, linewidths=0.5, fmt='.2f')
        plt.title('Feature Correlation Matrix')
        plt.tight_layout()
        plt.savefig(os.path.join(self.figures_dir, 'correlation_matrix.png'), dpi=300, bbox_inches='tight')
        plt.show()
        
        # Feature correlation with target
        target_corr = corr_matrix['target_mortality_48h'].drop('target_mortality_48h').sort_values(key=abs, ascending=False)
        
        plt.figure(figsize=(10, 8))
        target_corr.plot(kind='barh')
        plt.title('Feature Correlation with 48h Mortality')
        plt.xlabel('Correlation Coefficient')
        plt.tight_layout()
        plt.savefig(os.path.join(self.figures_dir, 'target_correlations.png'), dpi=300, bbox_inches='tight')
        plt.show()
        
        return target_corr
        
    def class_imbalance_analysis(self):
        """Analyze class imbalance and its implications"""
        self.logger.info("⚖️ Analyzing class imbalance...")
        
        target_counts = self.df['target_mortality_48h'].value_counts()
        imbalance_ratio = target_counts[0] / target_counts[1]
        
        print("\n" + "="*60)
        print("CLASS IMBALANCE ANALYSIS")
        print("="*60)
        print(f"Survivors (0): {target_counts[0]:,} ({target_counts[0]/len(self.df)*100:.1f}%)")
        print(f"48h Mortality (1): {target_counts[1]:,} ({target_counts[1]/len(self.df)*100:.1f}%)")
        print(f"Imbalance Ratio: {imbalance_ratio:.1f}:1")
        
        if imbalance_ratio > 10:
            print("⚠️  Severe class imbalance detected!")
            print("   Recommendations:")
            print("   - Use stratified sampling for train/test split")
            print("   - Consider SMOTE or other oversampling techniques")
            print("   - Use appropriate metrics (ROC-AUC, PR-AUC, F1)")
            print("   - Adjust class weights in models")
        elif imbalance_ratio > 3:
            print("⚠️  Moderate class imbalance detected!")
            print("   Recommendations:")
            print("   - Use stratified sampling")
            print("   - Monitor precision/recall carefully")
            print("   - Consider class weighting")
        
        return imbalance_ratio
        
    def generate_eda_report(self, target_corr, imbalance_ratio):
        """Generate comprehensive EDA report"""
        report_path = get_report_path('ml_eda_report.md')
        
        with open(report_path, 'w') as f:
            f.write("# Übung 5 - Exploratory Data Analysis Report\n\n")
            f.write(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            f.write("## Dataset Summary\n\n")
            f.write(f"- **Total Records:** {len(self.df):,}\n")
            f.write(f"- **Features:** {len(self.df.columns)}\n")
            f.write(f"- **Unique Patients:** {self.df['subject_id'].nunique()}\n")
            f.write(f"- **Unique ICU Stays:** {self.df['stay_id'].nunique()}\n")
            f.write(f"- **48h Mortality Events:** {self.df['target_mortality_48h'].sum()}\n")
            f.write(f"- **48h Mortality Rate:** {self.df['target_mortality_48h'].mean()*100:.2f}%\n\n")
            
            f.write("## Key Findings\n\n")
            f.write(f"### Class Imbalance\n")
            f.write(f"- **Imbalance Ratio:** {imbalance_ratio:.1f}:1\n")
            f.write(f"- **Impact:** {'Severe' if imbalance_ratio > 10 else 'Moderate' if imbalance_ratio > 3 else 'Mild'}\n\n")
            
            f.write("### Top Predictive Features (by correlation)\n")
            for feature, corr_val in target_corr.head(10).items():
                f.write(f"- **{feature}:** {corr_val:.3f}\n")
                
            f.write("\n### Data Quality Issues\n")
            missing_counts = self.df.isnull().sum()
            high_missing = missing_counts[missing_counts > len(self.df) * 0.3]
            if not high_missing.empty:
                f.write("**High Missing Values (>30%):**\n")
                for col, count in high_missing.items():
                    pct = (count / len(self.df)) * 100
                    f.write(f"- **{col}:** {pct:.1f}%\n")
            else:
                f.write("- No features with high missing values (>30%)\n")
                
            f.write("\n## Visualizations Generated\n\n")
            f.write("1. **Target Distribution:** `docs/visualizations/eda/target_distribution.png`\n")
            f.write("2. **SOFA Score Distributions:** `docs/visualizations/eda/sofa_distributions.png`\n")
            f.write("3. **Clinical Parameters:** `docs/visualizations/eda/clinical_parameters.png`\n")
            f.write("4. **Temporal Patterns:** `docs/visualizations/eda/temporal_patterns.png`\n")
            f.write("5. **Correlation Matrix:** `docs/visualizations/eda/correlation_matrix.png`\n")
            f.write("6. **Target Correlations:** `docs/visualizations/eda/target_correlations.png`\n\n")
            
            f.write("## Next Steps\n\n")
            f.write("1. **Step 4:** Feature Engineering & Selection\n")
            f.write("2. **Step 5:** Temporal Data Splitting\n")
            f.write("3. **Step 6:** Handle Class Imbalance\n")
            f.write("4. **Step 7:** Baseline Model Implementation\n")
            
        self.logger.info(f"📄 EDA report saved: {report_path}")

def main():
    """Main execution function"""
    print("🚀 Starting Step 3: Exploratory Data Analysis")
    print("=" * 60)
    
    try:
        # Initialize analyzer
        analyzer = ExploratoryDataAnalyzer()
        
        # Load dataset
        analyzer.load_dataset()
        
        # Perform comprehensive EDA
        analyzer.basic_data_overview()
        analyzer.analyze_target_distribution()
        analyzer.analyze_sofa_distributions()
        analyzer.analyze_clinical_parameters()
        analyzer.analyze_temporal_patterns()
        target_corr = analyzer.correlation_analysis()
        imbalance_ratio = analyzer.class_imbalance_analysis()
        
        # Generate report
        analyzer.generate_eda_report(target_corr, imbalance_ratio)
        
        print("\n✅ Step 3 completed successfully!")
        print("📊 Visualizations saved to: docs/visualizations/eda/")
        print("📋 Check docs/reports/ml_eda_report.md for detailed analysis")
        print("\n🔄 Ready to proceed to Step 4: Feature Engineering")
        
        return 0
        
    except Exception as e:
        print(f"❌ EDA failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit(main())
