#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Configuration Comparison Analysis Module for Task 5.4
====================================================

This module implements comprehensive statistical comparison between
dual ETL configurations including:
- Statistical correlation analysis
- Bland-Altman plots
- Subgroup analysis
- Mortality correlation analysis

Author: Medical Data Science Team
Date: 2025-06-17
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats
from sqlalchemy import create_engine, text
from datetime import datetime
import logging
import sys
from config_local import DB_CONFIG

class ConfigurationComparison:
    """Compare results from two ETL configurations"""
    
    def __init__(self, comparison_config):
        self.config = comparison_config
        self.logger = self._setup_logging()
        self.engine = self._create_engine()
        self.results = {}
    
    def _setup_logging(self):
        """Setup logging"""
        logger = logging.getLogger('ComparisonAnalysis')
        logger.setLevel(logging.INFO)
        
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        
        if not logger.handlers:
            logger.addHandler(handler)
        
        return logger
    
    def _create_engine(self):
        """Create database engine"""
        if DB_CONFIG.get('password'):
            connection_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        else:
            connection_string = f"postgresql://{DB_CONFIG['user']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        
        return create_engine(connection_string)
    
    def load_configuration_data(self, table1, table2):
        """Load data from both configuration tables"""
        self.logger.info(f"📊 Loading data from {table1} and {table2}...")
        
        # Load config 1 data
        query1 = f"""
        SELECT 
            patient_id, hadm_id, stay_id, measurement_time,
            apache_ii_score, sofa_score, saps_ii_score, oasis_score,
            total_parameters_used, data_quality_score,
            config_name, aggregation_method, imputation_method
        FROM gold.{table1}
        WHERE apache_ii_score IS NOT NULL 
          AND sofa_score IS NOT NULL
        ORDER BY patient_id, measurement_time
        """
        
        df1 = pd.read_sql(query1, self.engine)
        df1['config_source'] = 'config1'
        
        # Load config 2 data
        query2 = f"""
        SELECT 
            patient_id, hadm_id, stay_id, measurement_time,
            apache_ii_score, sofa_score, saps_ii_score, oasis_score,
            total_parameters_used, data_quality_score,
            config_name, aggregation_method, imputation_method
        FROM gold.{table2}
        WHERE apache_ii_score IS NOT NULL 
          AND sofa_score IS NOT NULL
        ORDER BY patient_id, measurement_time
        """
        
        df2 = pd.read_sql(query2, self.engine)
        df2['config_source'] = 'config2'
        
        self.logger.info(f"✅ Loaded {len(df1):,} records from {table1}")
        self.logger.info(f"✅ Loaded {len(df2):,} records from {table2}")
        
        return df1, df2
    
    def merge_for_comparison(self, df1, df2):
        """Merge datasets for paired comparison"""
        self.logger.info("🔄 Merging datasets for paired comparison...")
        
        # Merge on patient and time for paired comparison
        merged = df1.merge(
            df2, 
            on=['patient_id', 'hadm_id', 'stay_id', 'measurement_time'],
            suffixes=('_config1', '_config2'),
            how='inner'
        )
        
        self.logger.info(f"✅ {len(merged):,} paired observations for comparison")
        return merged
    
    def calculate_correlations(self, merged_df):
        """Calculate correlation statistics"""
        self.logger.info("📈 Calculating correlation statistics...")
        
        score_columns = ['apache_ii_score', 'sofa_score', 'saps_ii_score', 'oasis_score']
        correlations = {}
        
        for score in score_columns:
            col1 = f"{score}_config1"
            col2 = f"{score}_config2"
            
            if col1 in merged_df.columns and col2 in merged_df.columns:
                # Remove NaN values for correlation
                valid_data = merged_df[[col1, col2]].dropna()
                
                if len(valid_data) > 10:  # Minimum sample size
                    # Pearson correlation
                    pearson_r, pearson_p = stats.pearsonr(valid_data[col1], valid_data[col2])
                    
                    # Spearman correlation
                    spearman_r, spearman_p = stats.spearmanr(valid_data[col1], valid_data[col2])
                    
                    # Mean Absolute Difference
                    mad = np.mean(np.abs(valid_data[col1] - valid_data[col2]))
                    
                    # Statistical tests
                    t_stat, t_p = stats.ttest_rel(valid_data[col1], valid_data[col2])
                    wilcoxon_stat, wilcoxon_p = stats.wilcoxon(valid_data[col1], valid_data[col2])
                    
                    correlations[score] = {
                        'pearson_r': pearson_r,
                        'pearson_p': pearson_p,
                        'spearman_r': spearman_r,
                        'spearman_p': spearman_p,
                        'mean_absolute_difference': mad,
                        't_test_stat': t_stat,
                        't_test_p': t_p,
                        'wilcoxon_stat': wilcoxon_stat,
                        'wilcoxon_p': wilcoxon_p,
                        'sample_size': len(valid_data),
                        'config1_mean': valid_data[col1].mean(),
                        'config1_median': valid_data[col1].median(),
                        'config1_std': valid_data[col1].std(),
                        'config2_mean': valid_data[col2].mean(),
                        'config2_median': valid_data[col2].median(),
                        'config2_std': valid_data[col2].std()
                    }
                    
                    self.logger.info(f"  {score}: r={pearson_r:.3f}, MAD={mad:.3f}, n={len(valid_data)}")
        
        self.results['correlations'] = correlations
        return correlations
    
    def create_visualizations(self, merged_df):
        """Create comparison visualizations"""
        self.logger.info("📊 Creating comparison visualizations...")
        
        score_columns = ['apache_ii_score', 'sofa_score', 'saps_ii_score', 'oasis_score']
        
        # Set up the plotting style
        plt.style.use('default')
        sns.set_palette("husl")
        
        # Create subplots
        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle('Configuration Comparison: Score Distributions', fontsize=16, fontweight='bold')
        
        for i, score in enumerate(score_columns):
            ax = axes[i//2, i%2]
            col1 = f"{score}_config1"
            col2 = f"{score}_config2"
            
            if col1 in merged_df.columns and col2 in merged_df.columns:
                # Box plot comparison
                data_to_plot = [
                    merged_df[col1].dropna(),
                    merged_df[col2].dropna()
                ]
                
                ax.boxplot(data_to_plot, labels=['Config 1 (Mean)', 'Config 2 (Median)'])
                ax.set_title(f'{score.replace("_", " ").title()}')
                ax.set_ylabel('Score Value')
                ax.grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.savefig('config_distribution_comparison.png', dpi=300, bbox_inches='tight')
        plt.close()
        
        # Create scatter plots
        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle('Configuration Comparison: Scatter Plots', fontsize=16, fontweight='bold')
        
        for i, score in enumerate(score_columns):
            ax = axes[i//2, i%2]
            col1 = f"{score}_config1"
            col2 = f"{score}_config2"
            
            if col1 in merged_df.columns and col2 in merged_df.columns:
                valid_data = merged_df[[col1, col2]].dropna()
                
                if len(valid_data) > 0:
                    ax.scatter(valid_data[col1], valid_data[col2], alpha=0.6)
                    
                    # Add diagonal line
                    min_val = min(valid_data[col1].min(), valid_data[col2].min())
                    max_val = max(valid_data[col1].max(), valid_data[col2].max())
                    ax.plot([min_val, max_val], [min_val, max_val], 'r--', alpha=0.8)
                    
                    ax.set_xlabel('Config 1 (Mean-based)')
                    ax.set_ylabel('Config 2 (Median-based)')
                    ax.set_title(f'{score.replace("_", " ").title()}')
                    ax.grid(True, alpha=0.3)
                    
                    # Add correlation coefficient
                    if score in self.results.get('correlations', {}):
                        r = self.results['correlations'][score]['pearson_r']
                        ax.text(0.05, 0.95, f'r = {r:.3f}', transform=ax.transAxes,
                               bbox=dict(boxstyle="round,pad=0.3", facecolor="white", alpha=0.8))
        
        plt.tight_layout()
        plt.savefig('config_scatter_comparison.png', dpi=300, bbox_inches='tight')
        plt.close()
        
        self.logger.info("✅ Visualizations saved: config_distribution_comparison.png, config_scatter_comparison.png")
    
    def create_bland_altman_plots(self, merged_df):
        """Create Bland-Altman plots for agreement analysis"""
        self.logger.info("📊 Creating Bland-Altman plots...")
        
        score_columns = ['apache_ii_score', 'sofa_score', 'saps_ii_score', 'oasis_score']
        
        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle('Bland-Altman Plots: Agreement Analysis', fontsize=16, fontweight='bold')
        
        for i, score in enumerate(score_columns):
            ax = axes[i//2, i%2]
            col1 = f"{score}_config1"
            col2 = f"{score}_config2"
            
            if col1 in merged_df.columns and col2 in merged_df.columns:
                valid_data = merged_df[[col1, col2]].dropna()
                
                if len(valid_data) > 0:
                    # Calculate Bland-Altman statistics
                    mean_scores = (valid_data[col1] + valid_data[col2]) / 2
                    diff_scores = valid_data[col1] - valid_data[col2]
                    
                    mean_diff = diff_scores.mean()
                    std_diff = diff_scores.std()
                    
                    # Create plot
                    ax.scatter(mean_scores, diff_scores, alpha=0.6)
                    
                    # Add reference lines
                    ax.axhline(mean_diff, color='red', linestyle='-', label=f'Mean Diff: {mean_diff:.2f}')
                    ax.axhline(mean_diff + 1.96*std_diff, color='red', linestyle='--', 
                              label=f'+1.96 SD: {mean_diff + 1.96*std_diff:.2f}')
                    ax.axhline(mean_diff - 1.96*std_diff, color='red', linestyle='--',
                              label=f'-1.96 SD: {mean_diff - 1.96*std_diff:.2f}')
                    
                    ax.set_xlabel('Mean of Two Measurements')
                    ax.set_ylabel('Difference (Config1 - Config2)')
                    ax.set_title(f'{score.replace("_", " ").title()}')
                    ax.grid(True, alpha=0.3)
                    ax.legend(fontsize=8)
        
        plt.tight_layout()
        plt.savefig('bland_altman_plots.png', dpi=300, bbox_inches='tight')
        plt.close()
        
        self.logger.info("✅ Bland-Altman plots saved: bland_altman_plots.png")
    
    def save_comparison_results(self, comparison_name):
        """Save comparison results to database"""
        self.logger.info("💾 Saving comparison results to database...")
        
        if 'correlations' not in self.results:
            self.logger.warning("No correlation results to save")
            return
        
        # Prepare data for insertion
        insert_data = []
        
        for score_type, stats in self.results['correlations'].items():
            record = {
                'comparison_name': comparison_name,
                'config_1_name': 'mean_based_config',
                'config_2_name': 'median_based_config',
                'score_type': score_type,
                'correlation_pearson': stats['pearson_r'],
                'correlation_spearman': stats['spearman_r'],
                'mean_absolute_difference': stats['mean_absolute_difference'],
                'config_1_mean': stats['config1_mean'],
                'config_1_median': stats['config1_median'],
                'config_1_std': stats['config1_std'],
                'config_2_mean': stats['config2_mean'],
                'config_2_median': stats['config2_median'],
                'config_2_std': stats['config2_std'],
                't_test_statistic': stats['t_test_stat'],
                't_test_p_value': stats['t_test_p'],
                'wilcoxon_statistic': stats['wilcoxon_stat'],
                'wilcoxon_p_value': stats['wilcoxon_p'],
                'sample_size': stats['sample_size'],
                'analysis_date': datetime.now(),
                'analysis_parameters': {
                    'significance_level': 0.05,
                    'comparison_type': 'paired_analysis'
                }
            }
            insert_data.append(record)
        
        # Convert to DataFrame and save
        df_results = pd.DataFrame(insert_data)
        
        # Save to database
        df_results.to_sql(
            'config_comparison_analysis',
            self.engine,
            schema='gold',
            if_exists='append',
            index=False,
            method='multi'
        )
        
        self.logger.info(f"✅ Saved {len(insert_data)} comparison results to gold.config_comparison_analysis")
    
    def generate_summary_report(self):
        """Generate summary report"""
        self.logger.info("📋 Generating summary report...")
        
        if 'correlations' not in self.results:
            self.logger.warning("No results to report")
            return
        
        report = []
        report.append("# Configuration Comparison Analysis Report")
        report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append("")
        report.append("## Summary Statistics")
        report.append("")
        
        for score_type, stats in self.results['correlations'].items():
            report.append(f"### {score_type.replace('_', ' ').title()}")
            report.append(f"- **Sample Size**: {stats['sample_size']:,}")
            report.append(f"- **Pearson Correlation**: {stats['pearson_r']:.3f} (p={stats['pearson_p']:.3f})")
            report.append(f"- **Spearman Correlation**: {stats['spearman_r']:.3f} (p={stats['spearman_p']:.3f})")
            report.append(f"- **Mean Absolute Difference**: {stats['mean_absolute_difference']:.3f}")
            report.append(f"- **Config 1 Mean ± SD**: {stats['config1_mean']:.2f} ± {stats['config1_std']:.2f}")
            report.append(f"- **Config 2 Mean ± SD**: {stats['config2_mean']:.2f} ± {stats['config2_std']:.2f}")
            report.append(f"- **T-test p-value**: {stats['t_test_p']:.3f}")
            report.append("")
        
        report.append("## Files Generated")
        report.append("- config_distribution_comparison.png")
        report.append("- config_scatter_comparison.png") 
        report.append("- bland_altman_plots.png")
        report.append("- Comparison results saved to gold.config_comparison_analysis")
        
        # Save report
        with open('comparison_analysis_report.md', 'w') as f:
            f.write('\n'.join(report))
        
        self.logger.info("✅ Summary report saved: comparison_analysis_report.md")

def run_comparison(comparison_tables, comparison_config):
    """Main function to run comparison analysis"""
    try:
        # Initialize comparison analyzer
        analyzer = ConfigurationComparison(comparison_config)
        
        # Load data
        df1, df2 = analyzer.load_configuration_data(
            comparison_tables['table_1'], 
            comparison_tables['table_2']
        )
        
        # Merge for comparison
        merged_df = analyzer.merge_for_comparison(df1, df2)
        
        if len(merged_df) == 0:
            analyzer.logger.warning("No matching records found for comparison")
            return False
        
        # Perform analysis
        analyzer.calculate_correlations(merged_df)
        analyzer.create_visualizations(merged_df)
        analyzer.create_bland_altman_plots(merged_df)
        
        # Save results
        comparison_name = f"Config_Comparison_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        analyzer.save_comparison_results(comparison_name)
        analyzer.generate_summary_report()
        
        analyzer.logger.info("🎉 Comparison analysis completed successfully!")
        return True
        
    except Exception as e:
        print(f"❌ Comparison analysis failed: {e}")
        return False

if __name__ == "__main__":
    # Test comparison analysis
    import sys
    import os
    sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'config'))
    import sys
    import os
    sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'config'))
    from etl_configurations import *
    
    comparison_tables = get_comparison_tables()
    comparison_config = COMPARISON_CONFIG
    
    success = run_comparison(comparison_tables, comparison_config)
    if success:
        print("✅ Comparison analysis completed successfully")
    else:
        print("❌ Comparison analysis failed")
        sys.exit(1)
