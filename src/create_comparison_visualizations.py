#!/usr/bin/env python3
"""
Configuration Comparison Visualization Script for Task 5.4
Creates comprehensive visualizations comparing two ETL configurations
"""

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np
from scipy import stats
from scipy.stats import pearsonr, spearmanr
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'config'))
from src.config import etl_configurations as configg
from src.utils.file_paths import get_visualization_path, get_report_path
from config_local import DB_CONFIG
import psycopg2
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# Set visualization style
plt.style.use('seaborn-v0_8')
sns.set_palette("husl")

def load_configuration_data():
    """Load data from both configuration tables"""
    print("📊 Loading data from both configurations...")
    
    conn = psycopg2.connect(**DB_CONFIG)
    config_tables = configg.get_comparison_tables()
    
    try:
        # Load Config 1 data
        query1 = f"SELECT * FROM gold.{config_tables['table_1']}"
        df1 = pd.read_sql(query1, conn)
        print(f"✅ Config 1 data loaded: {len(df1)} records")
        
        # Load Config 2 data
        query2 = f"SELECT * FROM gold.{config_tables['table_2']}"
        df2 = pd.read_sql(query2, conn)
        print(f"✅ Config 2 data loaded: {len(df2)} records")
        
        return df1, df2, config_tables
        
    except Exception as e:
        print(f"❌ Error loading data: {e}")
        return None, None, None
    finally:
        conn.close()

def create_distribution_comparison(df1, df2, config_tables):
    """Create distribution comparison plots"""
    print("📈 Creating distribution comparison plots...")
    
    # Score columns to compare
    score_columns = ['sofa_score', 'apache_ii_score', 'saps_ii_score', 'oasis_score']
    
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle('Score Distribution Comparison Between Configurations', fontsize=16, fontweight='bold')
    
    for i, score_col in enumerate(score_columns):
        row = i // 2
        col = i % 2
        ax = axes[row, col]
        
        if score_col in df1.columns and score_col in df2.columns:
            # Remove NaN values
            data1 = df1[score_col].dropna()
            data2 = df2[score_col].dropna()
            
            if len(data1) > 0 and len(data2) > 0:
                # Create histograms
                ax.hist(data1, bins=30, alpha=0.7, label=f'Config 1 (Mean-based)', density=True)
                ax.hist(data2, bins=30, alpha=0.7, label=f'Config 2 (Median-based)', density=True)
                
                # Add statistics
                ax.axvline(data1.mean(), color='blue', linestyle='--', alpha=0.8, label=f'Config 1 Mean: {data1.mean():.2f}')
                ax.axvline(data2.mean(), color='orange', linestyle='--', alpha=0.8, label=f'Config 2 Mean: {data2.mean():.2f}')
                
                ax.set_title(f'{score_col.replace("_", " ").title()} Distribution')
                ax.set_xlabel('Score Value')
                ax.set_ylabel('Density')
                ax.legend()
                ax.grid(True, alpha=0.3)
            else:
                ax.text(0.5, 0.5, f'No data available for {score_col}', 
                       transform=ax.transAxes, ha='center', va='center')
        else:
            ax.text(0.5, 0.5, f'Column {score_col} not found', 
                   transform=ax.transAxes, ha='center', va='center')
    
    plt.tight_layout()
    filepath = get_visualization_path('config_distribution_comparison.png')
    plt.savefig(filepath, dpi=300, bbox_inches='tight')
    print(f"✅ Distribution comparison saved as '{filepath}'")
    return fig

def create_boxplot_comparison(df1, df2, config_tables):
    """Create box plot comparison"""
    print("📦 Creating box plot comparison...")
    
    score_columns = ['sofa_score', 'apache_ii_score', 'saps_ii_score', 'oasis_score']
    
    # Prepare data for box plots
    plot_data = []
    
    for score_col in score_columns:
        if score_col in df1.columns and score_col in df2.columns:
            # Config 1 data
            for value in df1[score_col].dropna():
                plot_data.append({
                    'Score': score_col.replace('_', ' ').title(),
                    'Value': value,
                    'Configuration': 'Config 1 (Mean-based)'
                })
            
            # Config 2 data
            for value in df2[score_col].dropna():
                plot_data.append({
                    'Score': score_col.replace('_', ' ').title(),
                    'Value': value,
                    'Configuration': 'Config 2 (Median-based)'
                })
    
    if plot_data:
        df_plot = pd.DataFrame(plot_data)
        
        fig, ax = plt.subplots(figsize=(14, 8))
        sns.boxplot(data=df_plot, x='Score', y='Value', hue='Configuration', ax=ax)
        
        ax.set_title('Score Distribution Comparison - Box Plots', fontsize=14, fontweight='bold')
        ax.set_xlabel('Score Type')
        ax.set_ylabel('Score Value')
        ax.legend(title='Configuration')
        plt.xticks(rotation=45)
        
        plt.tight_layout()
        filepath = get_visualization_path('config_boxplot_comparison.png')
        plt.savefig(filepath, dpi=300, bbox_inches='tight')
        print(f"✅ Box plot comparison saved as '{filepath}'")
        return fig
    else:
        print("⚠️ No data available for box plot comparison")
        return None

def create_scatter_correlation_plot(df1, df2, config_tables):
    """Create scatter plot and correlation analysis"""
    print("🔗 Creating scatter plot and correlation analysis...")
    
    # Merge data on patient_id for comparison
    merged_data = pd.merge(
        df1[['patient_id', 'sofa_score', 'apache_ii_score']].dropna(), 
        df2[['patient_id', 'sofa_score', 'apache_ii_score']].dropna(), 
        on='patient_id', 
        suffixes=('_config1', '_config2')
    )
    
    if len(merged_data) == 0:
        print("⚠️ No matching patients found between configurations")
        return None
    
    print(f"📊 Found {len(merged_data)} matching patients for correlation analysis")
    
    fig, axes = plt.subplots(1, 2, figsize=(16, 6))
    
    # SOFA Score Scatter Plot
    if 'sofa_score_config1' in merged_data.columns and 'sofa_score_config2' in merged_data.columns:
        ax1 = axes[0]
        ax1.scatter(merged_data['sofa_score_config1'], merged_data['sofa_score_config2'], 
                   alpha=0.6, s=50)
        
        # Add perfect correlation line
        max_val = max(merged_data['sofa_score_config1'].max(), merged_data['sofa_score_config2'].max())
        min_val = min(merged_data['sofa_score_config1'].min(), merged_data['sofa_score_config2'].min())
        ax1.plot([min_val, max_val], [min_val, max_val], 'r--', alpha=0.8, label='Perfect Correlation')
        
        # Calculate correlation
        pearson_corr, pearson_p = pearsonr(merged_data['sofa_score_config1'], merged_data['sofa_score_config2'])
        spearman_corr, spearman_p = spearmanr(merged_data['sofa_score_config1'], merged_data['sofa_score_config2'])
        
        ax1.set_xlabel('Config 1 SOFA Score (Mean-based)')
        ax1.set_ylabel('Config 2 SOFA Score (Median-based)')
        ax1.set_title(f'SOFA Score Correlation\nPearson: {pearson_corr:.3f}, Spearman: {spearman_corr:.3f}')
        ax1.legend()
        ax1.grid(True, alpha=0.3)
    
    # APACHE II Score Scatter Plot
    if 'apache_ii_score_config1' in merged_data.columns and 'apache_ii_score_config2' in merged_data.columns:
        ax2 = axes[1]
        ax2.scatter(merged_data['apache_ii_score_config1'], merged_data['apache_ii_score_config2'], 
                   alpha=0.6, s=50, color='orange')
        
        # Add perfect correlation line
        max_val = max(merged_data['apache_ii_score_config1'].max(), merged_data['apache_ii_score_config2'].max())
        min_val = min(merged_data['apache_ii_score_config1'].min(), merged_data['apache_ii_score_config2'].min())
        ax2.plot([min_val, max_val], [min_val, max_val], 'r--', alpha=0.8, label='Perfect Correlation')
        
        # Calculate correlation
        pearson_corr, pearson_p = pearsonr(merged_data['apache_ii_score_config1'], merged_data['apache_ii_score_config2'])
        spearman_corr, spearman_p = spearmanr(merged_data['apache_ii_score_config1'], merged_data['apache_ii_score_config2'])
        
        ax2.set_xlabel('Config 1 APACHE II Score (Mean-based)')
        ax2.set_ylabel('Config 2 APACHE II Score (Median-based)')
        ax2.set_title(f'APACHE II Score Correlation\nPearson: {pearson_corr:.3f}, Spearman: {spearman_corr:.3f}')
        ax2.legend()
        ax2.grid(True, alpha=0.3)
    
    plt.tight_layout()
    filepath = get_visualization_path('config_scatter_correlation.png')
    plt.savefig(filepath, dpi=300, bbox_inches='tight')
    print(f"✅ Scatter correlation plot saved as '{filepath}'")
    return fig, merged_data

def create_bland_altman_plot(merged_data):
    """Create Bland-Altman plot for agreement analysis"""
    print("📊 Creating Bland-Altman plot...")
    
    if merged_data is None or len(merged_data) == 0:
        print("⚠️ No merged data available for Bland-Altman plot")
        return None
    
    fig, axes = plt.subplots(1, 2, figsize=(16, 6))
    
    # SOFA Score Bland-Altman
    if 'sofa_score_config1' in merged_data.columns and 'sofa_score_config2' in merged_data.columns:
        ax1 = axes[0]
        
        mean_scores = (merged_data['sofa_score_config1'] + merged_data['sofa_score_config2']) / 2
        diff_scores = merged_data['sofa_score_config1'] - merged_data['sofa_score_config2']
        
        ax1.scatter(mean_scores, diff_scores, alpha=0.6, s=50)
        
        # Add mean difference line
        mean_diff = diff_scores.mean()
        std_diff = diff_scores.std()
        
        ax1.axhline(y=mean_diff, color='red', linestyle='-', label=f'Mean Diff: {mean_diff:.3f}')
        ax1.axhline(y=mean_diff + 1.96*std_diff, color='red', linestyle='--', 
                   label=f'+1.96 SD: {mean_diff + 1.96*std_diff:.3f}')
        ax1.axhline(y=mean_diff - 1.96*std_diff, color='red', linestyle='--', 
                   label=f'-1.96 SD: {mean_diff - 1.96*std_diff:.3f}')
        
        ax1.set_xlabel('Mean of Both Configurations')
        ax1.set_ylabel('Difference (Config1 - Config2)')
        ax1.set_title('SOFA Score Bland-Altman Plot')
        ax1.legend()
        ax1.grid(True, alpha=0.3)
    
    # APACHE II Score Bland-Altman
    if 'apache_ii_score_config1' in merged_data.columns and 'apache_ii_score_config2' in merged_data.columns:
        ax2 = axes[1]
        
        mean_scores = (merged_data['apache_ii_score_config1'] + merged_data['apache_ii_score_config2']) / 2
        diff_scores = merged_data['apache_ii_score_config1'] - merged_data['apache_ii_score_config2']
        
        ax2.scatter(mean_scores, diff_scores, alpha=0.6, s=50, color='orange')
        
        # Add mean difference line
        mean_diff = diff_scores.mean()
        std_diff = diff_scores.std()
        
        ax2.axhline(y=mean_diff, color='red', linestyle='-', label=f'Mean Diff: {mean_diff:.3f}')
        ax2.axhline(y=mean_diff + 1.96*std_diff, color='red', linestyle='--', 
                   label=f'+1.96 SD: {mean_diff + 1.96*std_diff:.3f}')
        ax2.axhline(y=mean_diff - 1.96*std_diff, color='red', linestyle='--', 
                   label=f'-1.96 SD: {mean_diff - 1.96*std_diff:.3f}')
        
        ax2.set_xlabel('Mean of Both Configurations')
        ax2.set_ylabel('Difference (Config1 - Config2)')
        ax2.set_title('APACHE II Score Bland-Altman Plot')
        ax2.legend()
        ax2.grid(True, alpha=0.3)
    
    plt.tight_layout()
    filepath = get_visualization_path('config_bland_altman.png')
    plt.savefig(filepath, dpi=300, bbox_inches='tight')
    print(f"✅ Bland-Altman plot saved as '{filepath}'")
    return fig

def generate_statistical_summary(df1, df2, merged_data):
    """Generate statistical summary report"""
    print("📋 Generating statistical summary report...")
    
    summary_report = []
    summary_report.append("=" * 70)
    summary_report.append("CONFIGURATION COMPARISON STATISTICAL SUMMARY")
    summary_report.append("=" * 70)
    summary_report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    summary_report.append("")
    
    # Data overview
    summary_report.append("📊 DATA OVERVIEW:")
    summary_report.append(f"  Config 1 (Mean-based): {len(df1)} records")
    summary_report.append(f"  Config 2 (Median-based): {len(df2)} records")
    summary_report.append(f"  Matching patients: {len(merged_data) if merged_data is not None else 0}")
    summary_report.append("")
    
    # Score statistics
    score_columns = ['sofa_score', 'apache_ii_score', 'saps_ii_score', 'oasis_score']
    
    for score_col in score_columns:
        if score_col in df1.columns and score_col in df2.columns:
            data1 = df1[score_col].dropna()
            data2 = df2[score_col].dropna()
            
            if len(data1) > 0 and len(data2) > 0:
                summary_report.append(f"📈 {score_col.replace('_', ' ').upper()} STATISTICS:")
                summary_report.append(f"  Config 1 - Mean: {data1.mean():.3f}, Median: {data1.median():.3f}, Std: {data1.std():.3f}")
                summary_report.append(f"  Config 2 - Mean: {data2.mean():.3f}, Median: {data2.median():.3f}, Std: {data2.std():.3f}")
                
                # Statistical tests
                try:
                    t_stat, t_p = stats.ttest_ind(data1, data2)
                    u_stat, u_p = stats.mannwhitneyu(data1, data2, alternative='two-sided')
                    
                    summary_report.append(f"  T-test: statistic={t_stat:.3f}, p-value={t_p:.6f}")
                    summary_report.append(f"  Mann-Whitney U: statistic={u_stat:.3f}, p-value={u_p:.6f}")
                except Exception as e:
                    summary_report.append(f"  Statistical tests failed: {e}")
                
                summary_report.append("")
    
    # Correlation analysis
    if merged_data is not None and len(merged_data) > 0:
        summary_report.append("🔗 CORRELATION ANALYSIS:")
        
        for score_col in ['sofa_score', 'apache_ii_score']:
            col1 = f"{score_col}_config1"
            col2 = f"{score_col}_config2"
            
            if col1 in merged_data.columns and col2 in merged_data.columns:
                try:
                    pearson_corr, pearson_p = pearsonr(merged_data[col1], merged_data[col2])
                    spearman_corr, spearman_p = spearmanr(merged_data[col1], merged_data[col2])
                    
                    summary_report.append(f"  {score_col.replace('_', ' ').title()}:")
                    summary_report.append(f"    Pearson: r={pearson_corr:.3f}, p={pearson_p:.6f}")
                    summary_report.append(f"    Spearman: ρ={spearman_corr:.3f}, p={spearman_p:.6f}")
                except Exception as e:
                    summary_report.append(f"    Correlation failed: {e}")
        
        summary_report.append("")
    
    # Save report
    filepath = get_report_path('configuration_comparison_report.txt')
    with open(filepath, 'w') as f:
        f.write('\n'.join(summary_report))
    
    print(f"✅ Statistical summary saved as '{filepath}'")
    return summary_report

def main():
    """Main visualization function"""
    print("🎨 Starting Configuration Comparison Visualization")
    print("=" * 60)
    
    # Load data
    df1, df2, config_tables = load_configuration_data()
    
    if df1 is None or df2 is None:
        print("❌ Failed to load configuration data")
        return
    
    # Create visualizations
    try:
        # Distribution comparison
        create_distribution_comparison(df1, df2, config_tables)
        
        # Box plot comparison
        create_boxplot_comparison(df1, df2, config_tables)
        
        # Scatter correlation plot
        scatter_fig, merged_data = create_scatter_correlation_plot(df1, df2, config_tables)
        
        # Bland-Altman plot
        create_bland_altman_plot(merged_data)
        
        # Statistical summary
        generate_statistical_summary(df1, df2, merged_data)
        
        print("\n✅ All visualizations created successfully!")
        print("📁 Generated files:")
        print("  - config_distribution_comparison.png")
        print("  - config_boxplot_comparison.png")
        print("  - config_scatter_correlation.png")
        print("  - config_bland_altman.png")
        print("  - configuration_comparison_report.txt")
        
    except Exception as e:
        print(f"❌ Visualization creation failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
