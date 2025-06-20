#!/usr/bin/env python3
"""
Mortality Analysis Visualization Script for Task 5.4
Creates mortality correlation and outcome analysis visualizations
"""

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np
from scipy import stats
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'config'))
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'config'))
from etl_configurations import *
from config_local import DB_CONFIG
import psycopg2
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# Set visualization style
plt.style.use('seaborn-v0_8')
sns.set_palette("viridis")

def load_mortality_data():
    """Load mortality analysis data"""
    print("💀 Loading mortality analysis data...")
    
    conn = psycopg2.connect(**DB_CONFIG)
    
    try:
        # Load mortality correlation data
        query = """
        SELECT 
            config_name,
            apache_ii_score,
            sofa_score,
            saps_ii_score,
            oasis_score,
            hospital_mortality,
            icu_mortality,
            day_30_mortality,
            age,
            gender,
            admission_type,
            los_hospital,
            los_icu
        FROM gold.mortality_correlation_analysis
        WHERE apache_ii_score IS NOT NULL 
        AND sofa_score IS NOT NULL
        """
        
        df = pd.read_sql(query, conn)
        print(f"✅ Mortality data loaded: {len(df)} records")
        
        return df
        
    except Exception as e:
        print(f"❌ Error loading mortality data: {e}")
        return None
    finally:
        conn.close()

def create_mortality_by_score_plots(df):
    """Create mortality rate by score plots"""
    print("📊 Creating mortality by score plots...")
    
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle('Mortality Rate by Clinical Scores', fontsize=16, fontweight='bold')
    
    score_columns = ['sofa_score', 'apache_ii_score', 'saps_ii_score', 'oasis_score']
    
    for i, score_col in enumerate(score_columns):
        row = i // 2
        col = i % 2
        ax = axes[row, col]
        
        if score_col in df.columns and 'hospital_mortality' in df.columns:
            # Create score bins
            df_temp = df[[score_col, 'hospital_mortality', 'config_name']].dropna()
            
            if len(df_temp) > 0:
                # Group by score ranges and config
                df_temp['score_bin'] = pd.cut(df_temp[score_col], bins=10, precision=1)
                
                mortality_by_score = df_temp.groupby(['score_bin', 'config_name'])['hospital_mortality'].agg(['mean', 'count']).reset_index()
                mortality_by_score = mortality_by_score[mortality_by_score['count'] >= 5]  # At least 5 patients per bin
                
                # Plot for each configuration
                for config in df_temp['config_name'].unique():
                    config_data = mortality_by_score[mortality_by_score['config_name'] == config]
                    if len(config_data) > 0:
                        x_vals = range(len(config_data))
                        ax.plot(x_vals, config_data['mean'], marker='o', label=config, linewidth=2)
                
                ax.set_title(f'{score_col.replace("_", " ").title()} vs Mortality')
                ax.set_xlabel('Score Bins')
                ax.set_ylabel('Mortality Rate')
                ax.legend()
                ax.grid(True, alpha=0.3)
                
                # Rotate x-axis labels
                if len(mortality_by_score) > 0:
                    ax.set_xticks(range(len(mortality_by_score['score_bin'].unique())))
                    ax.set_xticklabels([str(x) for x in mortality_by_score['score_bin'].unique()], rotation=45)
            else:
                ax.text(0.5, 0.5, f'No data for {score_col}', transform=ax.transAxes, ha='center')
        else:
            ax.text(0.5, 0.5, f'Missing columns for {score_col}', transform=ax.transAxes, ha='center')
    
    plt.tight_layout()
    plt.savefig('mortality_by_scores.png', dpi=300, bbox_inches='tight')
    print("✅ Mortality by scores plot saved as 'mortality_by_scores.png'")
    return fig

def create_score_distribution_by_mortality(df):
    """Create score distribution by mortality outcome"""
    print("📦 Creating score distribution by mortality...")
    
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle('Score Distribution by Hospital Mortality', fontsize=16, fontweight='bold')
    
    score_columns = ['sofa_score', 'apache_ii_score', 'saps_ii_score', 'oasis_score']
    
    for i, score_col in enumerate(score_columns):
        row = i // 2
        col = i % 2
        ax = axes[row, col]
        
        if score_col in df.columns and 'hospital_mortality' in df.columns:
            df_temp = df[[score_col, 'hospital_mortality', 'config_name']].dropna()
            
            if len(df_temp) > 0:
                # Box plot by mortality status and configuration
                sns.boxplot(data=df_temp, x='hospital_mortality', y=score_col, 
                           hue='config_name', ax=ax)
                
                ax.set_title(f'{score_col.replace("_", " ").title()} Distribution')
                ax.set_xlabel('Hospital Mortality')
                ax.set_ylabel('Score Value')
                
                # Add statistical annotation
                survivors = df_temp[df_temp['hospital_mortality'] == False][score_col]
                non_survivors = df_temp[df_temp['hospital_mortality'] == True][score_col]
                
                if len(survivors) > 0 and len(non_survivors) > 0:
                    # Mann-Whitney U test
                    try:
                        u_stat, u_p = stats.mannwhitneyu(survivors, non_survivors, alternative='two-sided')
                        ax.text(0.02, 0.98, f'Mann-Whitney p={u_p:.4f}', 
                               transform=ax.transAxes, verticalalignment='top',
                               bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))
                    except:
                        pass
            else:
                ax.text(0.5, 0.5, f'No data for {score_col}', transform=ax.transAxes, ha='center')
        else:
            ax.text(0.5, 0.5, f'Missing columns for {score_col}', transform=ax.transAxes, ha='center')
    
    plt.tight_layout()
    plt.savefig('score_distribution_by_mortality.png', dpi=300, bbox_inches='tight')
    print("✅ Score distribution by mortality saved as 'score_distribution_by_mortality.png'")
    return fig

def create_age_stratified_analysis(df):
    """Create age-stratified mortality analysis"""
    print("👴 Creating age-stratified analysis...")
    
    if 'age' not in df.columns:
        print("⚠️ Age column not found, skipping age analysis")
        return None
    
    # Create age groups
    df_temp = df.copy()
    df_temp['age_group'] = pd.cut(df_temp['age'], 
                                 bins=[0, 45, 65, 80, 100], 
                                 labels=['<45', '45-65', '65-80', '80+'])
    
    fig, axes = plt.subplots(1, 2, figsize=(16, 6))
    
    # Mortality rate by age group and configuration
    ax1 = axes[0]
    mortality_by_age = df_temp.groupby(['age_group', 'config_name'])['hospital_mortality'].agg(['mean', 'count']).reset_index()
    mortality_by_age = mortality_by_age[mortality_by_age['count'] >= 10]  # At least 10 patients
    
    # Pivot for easier plotting
    mortality_pivot = mortality_by_age.pivot(index='age_group', columns='config_name', values='mean')
    mortality_pivot.plot(kind='bar', ax=ax1)
    
    ax1.set_title('Mortality Rate by Age Group and Configuration')
    ax1.set_xlabel('Age Group')
    ax1.set_ylabel('Mortality Rate')
    ax1.legend(title='Configuration')
    ax1.grid(True, alpha=0.3)
    
    # SOFA score by age group
    ax2 = axes[1]
    sns.boxplot(data=df_temp, x='age_group', y='sofa_score', hue='config_name', ax=ax2)
    ax2.set_title('SOFA Score Distribution by Age Group')
    ax2.set_xlabel('Age Group')
    ax2.set_ylabel('SOFA Score')
    
    plt.tight_layout()
    plt.savefig('age_stratified_analysis.png', dpi=300, bbox_inches='tight')
    print("✅ Age-stratified analysis saved as 'age_stratified_analysis.png'")
    return fig

def create_correlation_heatmap(df):
    """Create correlation heatmap between scores and outcomes"""
    print("🔥 Creating correlation heatmap...")
    
    # Select numeric columns for correlation
    numeric_cols = ['sofa_score', 'apache_ii_score', 'saps_ii_score', 'oasis_score', 
                   'age', 'los_hospital', 'los_icu']
    outcome_cols = ['hospital_mortality', 'icu_mortality', 'day_30_mortality']
    
    # Convert boolean outcomes to numeric
    df_corr = df.copy()
    for col in outcome_cols:
        if col in df_corr.columns:
            df_corr[col] = df_corr[col].astype(int)
    
    # Calculate correlations for each configuration
    configs = df_corr['config_name'].unique()
    
    fig, axes = plt.subplots(1, len(configs), figsize=(8*len(configs), 6))
    if len(configs) == 1:
        axes = [axes]
    
    for i, config in enumerate(configs):
        config_data = df_corr[df_corr['config_name'] == config]
        
        # Select available columns
        available_cols = [col for col in numeric_cols + outcome_cols if col in config_data.columns]
        corr_matrix = config_data[available_cols].corr()
        
        # Create heatmap
        sns.heatmap(corr_matrix, annot=True, cmap='RdBu_r', center=0, 
                   square=True, ax=axes[i], cbar_kws={'shrink': 0.8})
        axes[i].set_title(f'Correlation Matrix - {config}')
    
    plt.tight_layout()
    plt.savefig('correlation_heatmap.png', dpi=300, bbox_inches='tight')
    print("✅ Correlation heatmap saved as 'correlation_heatmap.png'")
    return fig

def generate_mortality_report(df):
    """Generate mortality analysis report"""
    print("📋 Generating mortality analysis report...")
    
    report = []
    report.append("=" * 70)
    report.append("MORTALITY ANALYSIS REPORT")
    report.append("=" * 70)
    report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report.append("")
    
    # Overall statistics
    report.append("📊 OVERALL STATISTICS:")
    report.append(f"  Total patients: {len(df)}")
    
    if 'hospital_mortality' in df.columns:
        mortality_rate = df['hospital_mortality'].mean()
        report.append(f"  Hospital mortality rate: {mortality_rate:.3f} ({mortality_rate*100:.1f}%)")
    
    # Configuration comparison
    report.append("\n🔧 CONFIGURATION COMPARISON:")
    for config in df['config_name'].unique():
        config_data = df[df['config_name'] == config]
        report.append(f"\n  {config}:")
        report.append(f"    Patients: {len(config_data)}")
        
        if 'hospital_mortality' in config_data.columns:
            mortality_rate = config_data['hospital_mortality'].mean()
            report.append(f"    Mortality rate: {mortality_rate:.3f} ({mortality_rate*100:.1f}%)")
        
        # Score statistics
        score_cols = ['sofa_score', 'apache_ii_score']
        for score_col in score_cols:
            if score_col in config_data.columns:
                score_data = config_data[score_col].dropna()
                if len(score_data) > 0:
                    report.append(f"    {score_col}: mean={score_data.mean():.2f}, std={score_data.std():.2f}")
    
    # Save report
    with open('mortality_analysis_report.txt', 'w') as f:
        f.write('\n'.join(report))
    
    print("✅ Mortality analysis report saved as 'mortality_analysis_report.txt'")
    return report

def main():
    """Main mortality analysis function"""
    print("💀 Starting Mortality Analysis Visualization")
    print("=" * 60)
    
    # Load data
    df = load_mortality_data()
    
    if df is None or len(df) == 0:
        print("❌ No mortality data available")
        return
    
    # Create visualizations
    try:
        # Mortality by score plots
        create_mortality_by_score_plots(df)
        
        # Score distribution by mortality
        create_score_distribution_by_mortality(df)
        
        # Age-stratified analysis
        create_age_stratified_analysis(df)
        
        # Correlation heatmap
        create_correlation_heatmap(df)
        
        # Generate report
        generate_mortality_report(df)
        
        print("\n✅ All mortality visualizations created successfully!")
        print("📁 Generated files:")
        print("  - mortality_by_scores.png")
        print("  - score_distribution_by_mortality.png")
        print("  - age_stratified_analysis.png")
        print("  - correlation_heatmap.png")
        print("  - mortality_analysis_report.txt")
        
    except Exception as e:
        print(f"❌ Mortality visualization creation failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
