import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

df = pd.read_csv("analysis_output/stats_summary.csv")

for param in df['concept_name'].unique():
    subset = df[df['concept_name'] == param]
    sns.barplot(data=subset, x='group', y='mean')
    plt.title(f"{param} – Mittelwertvergleich")
    plt.ylabel("Mittelwert")
    plt.tight_layout()
    plt.savefig(f"plots/{param}_mean_comparison.png")
    plt.clf()
print("✅ Boxplots gespeichert unter /plots/")
