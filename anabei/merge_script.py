# ============================================================ 
# 🧠 Merged Performance Data Builder (v9.1 - inclui POLAR nos totais)
# ============================================================

import pandas as pd
from datetime import datetime
import re

# === 1. Leitura com detecção automática de delimitador e remoção de BOM ===
def load_csv_safely(path):
    """Lê CSV com detecção de separador, remoção de BOM e parsing numérico correto."""
    try:
        with open(path, "r", encoding="utf-8-sig") as f:  # remove BOM invisível
            df = pd.read_csv(f, sep=None, engine="python", thousands=",", decimal=".")
    except Exception:
        df = pd.read_csv(path, sep=None, engine="python")
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
    return df

# === 2. Leitura dos arquivos ===
google_ads = load_csv_safely("google_ads.csv")
tw_data = load_csv_safely("TW_data.csv")
nb_data = load_csv_safely("NB_data.csv")
polar_data = load_csv_safely("Polar_data.csv")

# === 3. Normalização de colunas ===
def normalize_cols(df):
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
    return df

google_ads = normalize_cols(google_ads)
tw_data = normalize_cols(tw_data)
nb_data = normalize_cols(nb_data)
polar_data = normalize_cols(polar_data)

# === 4. Funções auxiliares ===
def serial_date(date_col):
    """Converte datas em formato serial (compatível com planilha)"""
    base = datetime(1899, 12, 30)
    return (pd.to_datetime(date_col, errors="coerce") - base).dt.days.astype("Int64").astype(str)

def clean_id_column(series):
    """Limpa IDs removendo espaços, pontos, letras e mantendo apenas números válidos"""
    def clean_value(v):
        if pd.isna(v):
            return ""
        s = str(v).strip()
        s = s.replace(".0", "").replace(",", "")
        s = re.sub(r"[^0-9]", "", s)
        if len(s) < 6 or len(s) > 20:
            return ""
        return s
    return series.apply(clean_value)

# === 5. Correção de valores numéricos ===
# TW_data
for col in ["pixel_cv_lp", "pixel_cv_fc", "pixel_cv_lc"]:
    if col in tw_data.columns:
        tw_data[col] = tw_data[col].astype(str).str.replace(",", "").astype(float)

# NB_data
for col in ["ltv_attributed_rev", "ltv_attributed_rev_1st_time"]:
    if col in nb_data.columns:
        nb_data[col] = pd.to_numeric(nb_data[col], errors="coerce").fillna(0)

# Polar_data
for c in ["first_click_conversion_value", "last_click_conversion_value", "linear_paid_conversion_value"]:
    if c in polar_data.columns:
        polar_data[c] = pd.to_numeric(polar_data[c], errors="coerce").fillna(0)

# === 6. Criação das unique_keys ===
google_ads["unique_key"] = (
    serial_date(google_ads["date"])
    + clean_id_column(google_ads["campaign_id"])
    + clean_id_column(google_ads["ad_group_id"])
    + "x"
)

tw_data["unique_key"] = (
    serial_date(tw_data["event_date"])
    + clean_id_column(tw_data["campaign_id"])
    + clean_id_column(tw_data["adset_id"])
    + "x"
)

nb_data["unique_key"] = (
    serial_date(nb_data["date"])
    + clean_id_column(nb_data["campaign_id"])
    + clean_id_column(nb_data["adset_id"])
    + "x"
)

polar_data["unique_key"] = (
    serial_date(polar_data["date"])
    + clean_id_column(polar_data["campaign_id"])
    + clean_id_column(polar_data["adset_id"])
    + "x"
)

# === 7. Agregações ===
# TW_data
tw_agg = tw_data.groupby("unique_key").agg(
    TW_LP=("pixel_cv_lp", "sum"),
    TW_FC=("pixel_cv_fc", "sum"),
    TW_LC=("pixel_cv_lc", "sum"),
).reset_index()

# NB_data
nb_data["attribution_model"] = nb_data["attribution_model"].astype(str).str.strip().str.lower()

nb_co = nb_data[nb_data["attribution_model"] == "clicks only"]
nb_ft = nb_data[nb_data["attribution_model"] == "first touch"]
nb_ln = nb_data[nb_data["attribution_model"] == "last non-direct touch"]

nb_co_agg = nb_co.groupby("unique_key", as_index=False)["ltv_attributed_rev"].sum().rename(columns={"ltv_attributed_rev": "NB_CO"})
nb_ft_agg = nb_ft.groupby("unique_key", as_index=False)["ltv_attributed_rev"].sum().rename(columns={"ltv_attributed_rev": "NB_FT"})
nb_ln_agg = nb_ln.groupby("unique_key", as_index=False)["ltv_attributed_rev"].sum().rename(columns={"ltv_attributed_rev": "NB_LN"})

nb_agg = (
    nb_co_agg
    .merge(nb_ft_agg, on="unique_key", how="outer")
    .merge(nb_ln_agg, on="unique_key", how="outer")
    .fillna(0)
)

# Polar_data
polar_agg = polar_data.groupby("unique_key").agg(
    PO_FC=("first_click_conversion_value", "sum"),
    PO_LC=("last_click_conversion_value", "sum"),
    PO_LP=("linear_paid_conversion_value", "sum"),
).reset_index()

# === 8. Merge final ===
final = (
    google_ads
    .merge(tw_agg, on="unique_key", how="outer")
    .merge(nb_agg, on="unique_key", how="outer")
    .merge(polar_agg, on="unique_key", how="outer")
    .fillna(0)
)

# === 9. Reorganizar colunas (unique_key na frente) ===
cols = ["unique_key"] + [c for c in final.columns if c != "unique_key"]
final = final.reindex(columns=cols)

# === 10. Exportação ===
final.to_csv("merged_performance_data.csv", index=False)

# === 11. Totais gerais ===
print("\n✅ Arquivo final salvo como 'merged_performance_data.csv'")
for col in ["TW_FC", "TW_LC", "TW_LP", "NB_CO", "NB_FT", "NB_LN", "PO_FC", "PO_LC", "PO_LP"]:
    if col in final.columns:
        print(f"{col}: {final[col].sum():,.2f}")
