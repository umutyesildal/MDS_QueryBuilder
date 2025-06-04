import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from config_local import DATABASE_URI

engine = create_engine(DATABASE_URI)

def load_data():
    all_query = """
        SELECT stay_id, concept_name, value AS valuenum, charttime
        FROM silver.collection_disease_std
        WHERE value IS NOT NULL
    """

    cohort_query = """
        SELECT d.stay_id, d.concept_name, d.value AS valuenum, d.charttime
        FROM silver.collection_disease_std d
        JOIN silver.cohort_disease c ON d.stay_id = c.stay_id
        WHERE d.value IS NOT NULL
    """

    return pd.read_sql(all_query, engine), pd.read_sql(cohort_query, engine)

def describe_group(df, group):
    desc = df.groupby('concept_name')['valuenum'].agg([
        ('count', 'count'), ('mean', 'mean'), ('std', 'std'),
        ('min', 'min'), ('25%', lambda x: np.percentile(x, 25)),
        ('50%', 'median'), ('75%', lambda x: np.percentile(x, 75)), ('max', 'max')
    ]).reset_index()
    desc['group'] = group
    return desc

if __name__ == "__main__":
    df_all, df_cohort = load_data()
    stats_all = describe_group(df_all, 'All')
    stats_cohort = describe_group(df_cohort, 'Cohort')
    combined = pd.concat([stats_all, stats_cohort])
    combined.to_csv("analysis_output/stats_summary.csv", index=False)
    print("📊 Statistik erfolgreich gespeichert in /stats_summary.csv.")

