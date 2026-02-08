import pandas as pd
import json
from sqlalchemy import create_engine
import os
import re
from sqlalchemy import text
import sys


DB_URL = "postgresql://postgres:password@postgres:5432/postgres"
engine = create_engine(DB_URL)


def apply_regex_repair(df):
    valid_regions = ['North America', 'Europe',
                     'Asia Pacific', 'Africa', 'South America', 'Oceania']
    valid_set = {r.lower().replace(' ', '_') for r in valid_regions}
    region_col = 'region' if 'region' in df.columns else 'Region'

    if region_col in df.columns:
        mask = df[region_col].notnull() & ~df[region_col].astype(
            str).str.lower().str.replace(' ', '_').isin(valid_set)

        if mask.any():
            target_cols = [c for c in df.columns if c not in [
                'order_id', 'customer_id']]
            df.loc[mask, 'payload'] = df.loc[mask, target_cols].fillna(
                '').astype(str).agg(' '.join, axis=1)
            df.loc[mask, 'order_date'] = df.loc[mask, 'payload'].str.extract(
                r'(\d{4}[-/]\d{2}[-/]\d{2}|[a-zA-Z]+ \d{1,2} \d{4}|\d{2}/\d{2}/\d{4})', expand=False)
            df.loc[mask, 'revenue'] = df.loc[mask, 'payload'].str.extract(
                r'(\d+\.\d{2})', expand=False)
            df.loc[mask, 'channel_attributed'] = df.loc[mask, 'payload'].str.extract(
                r'(?i)(google|facebook)', expand=False)
            df.loc[mask, 'campaign_source'] = df.loc[mask, 'payload'].str.extract(
                r'((?:goog|fb)_camp_\d+)', expand=False)
            reg_pattern = '|'.join(valid_regions)
            df.loc[mask, region_col] = df.loc[mask, 'payload'].str.extract(
                f'(?i)({reg_pattern})', expand=False)
            df.drop(columns=['payload'], inplace=True)

    return df


def python_pre_clean(df, source_name):
    df.columns = [
        re.sub(r'[\s-]+', '_', c.strip().lower())
        for c in df.columns
    ]

    str_cols = df.select_dtypes(include=['object']).columns
    date_cols = [c for c in df.columns if 'date' in c]
    numeric_str_cols = ['revenue']

    for col in date_cols:
        df[col] = df[col].astype(str).str.replace('_', '-').str.strip()
        df[col] = pd.to_datetime(
            df[col], dayfirst=True, format='mixed', errors='coerce').dt.strftime('%Y-%m-%d')

    for col in str_cols:
        if col in date_cols:
            continue

        not_null_mask = df[col].notnull()
        if not_null_mask.any():
            df.loc[not_null_mask, col] = df.loc[not_null_mask,
                                                col].astype(str).str.lower().str.strip()
        if col not in numeric_str_cols:
            if df.loc[not_null_mask, col].str.contains('&', na=False).any():
                df.loc[not_null_mask, col] = df.loc[not_null_mask,
                                                    col].str.replace(r'\s+', '', regex=True)
            else:
                df.loc[not_null_mask, col] = df.loc[not_null_mask,
                                                    col].str.replace(r'[\s-]+', '_', regex=True)
        else:
            df.loc[not_null_mask, col] = df.loc[not_null_mask,
                                                col].str.replace(r'\s+', '', regex=True)

    return df


def ingest_data():
    current_script_path = os.path.abspath(__file__)
    data_dir = os.path.join(os.path.dirname(current_script_path), '..', 'data')
    staging_tables = ['stg_crm_revenue', 'stg_facebook_ads', 'stg_google_ads']

    with engine.connect() as conn:
        with conn.begin():
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS stg_data;"))
            conn.execute(text("GRANT ALL ON SCHEMA stg_data TO postgres;"))

    print("Schema 'stg_data' initialized and tables cleared.")

    # Ingest crm data
    crm_path = os.path.join(data_dir, 'crm_revenue.csv')
    if os.path.exists(crm_path):
        raw_header = pd.read_csv(crm_path, nrows=0).columns.tolist()
        extended_names = raw_header + [f'overflow_{i}' for i in range(1, 5)]

        df_crm = pd.read_csv(
            crm_path, names=extended_names, skiprows=1, engine='python', on_bad_lines='warn')
        raw_header = pd.read_csv(crm_path, nrows=0).columns.tolist()
        df_crm = apply_regex_repair(df_crm)
        df_crm = python_pre_clean(df_crm, "CRM")
        db_cols = [re.sub(r'[\s-]+', '_', c.strip().lower())
                   for c in raw_header]
        df_crm[db_cols].to_sql('stg_crm_revenue', con=engine, schema='stg_data',
                               if_exists='replace', index=False)
        print(f"Ingested CRM data: {len(df_crm)} rows")

    # Ingest facebook data
    fb_path = os.path.join(data_dir, 'facebook_export.csv')
    if os.path.exists(fb_path):
        df_fb = pd.read_csv(fb_path, on_bad_lines='warn', engine='python')
        df_fb = python_pre_clean(df_fb, "Facebook")
        df_fb.to_sql('stg_facebook_ads', con=engine, schema='stg_data',
                     if_exists='replace', index=False)
        print(f"Ingested Facebook data: {len(df_fb)} rows")

    # Ingest google data(Json)
    google_path = os.path.join(data_dir, 'google_ads_api.json')
    if os.path.exists(google_path):
        with open(google_path, 'r') as f:
            google_data = json.load(f)

        # Transform data from Json to Dataframe
        df_google = pd.json_normalize(
            google_data['campaigns'],
            record_path=['daily_metrics'],
            meta=['campaign_name', 'campaign_id', 'campaign_type']
        )
        df_google = python_pre_clean(df_google, "Google")
        df_google.to_sql('stg_google_ads', con=engine, schema='stg_data',
                         if_exists='replace', index=False)
        print(f"Ingested Google Ads data: {len(df_google)} rows")


if __name__ == "__main__":
    try:
        ingest_data()
        print("\n Data Ingestion Done!")
    except Exception as e:
        print(f"\n Data Ingestion Failed: {e}")
        sys.exit(1)
