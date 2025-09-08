# pages/04_estoque.py
# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import gspread
from gspread_dataframe import get_as_dataframe
from google.oauth2.service_account import Credentials
import unicodedata, re

st.set_page_config(page_title="Estoque — Movimentos & Ajustes", page_icon="📊", layout="wide")
st.title("📊 Estoque — Movimentos & Ajustes")

# ======================
# FUNÇÕES AUXILIARES
# ======================
def _normalize_private_key(key: str) -> str:
    if not isinstance(key, str):
        return key
    return key.replace("\\n", "\n")

def _to_num(x):
    try:
        return float(str(x).replace(",", "."))
    except:
        return 0

def _norm(s: str) -> str:
    s = str(s)
    s = "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))
    s = re.sub(r"[\W_]+", "", s, flags=re.UNICODE)  # remove acentos, espaços, símbolos
    return s.lower()

# ======================
# CONEXÃO GOOGLE SHEETS
# ======================
SHEET_ID = "1qtOF1I7Ap4By2388ySThoVlZHbI3rAJv_haEcil0IUE"
ABA_ESTOQUE = "Estoque"

scope = ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
creds = Credentials.from_service_account_info(
    st.secrets["gcp_service_account"],
    scopes=scope
)
gc = gspread.authorize(creds)
sh = gc.open_by_key(SHEET_ID)
worksheet = sh.worksheet(ABA_ESTOQUE)

df_estoque = get_as_dataframe(worksheet, dtype=str).fillna("")

# ======================
# AJUSTE DE COLUNAS
# ======================
esperadas = ["Entradas", "Saidas", "Ajustes", "EstoqueAtual", "CustoAtual"]

# mapa normalizado -> original
norm2orig = {_norm(c): c for c in df_estoque.columns}

custo_aliases = ["custoatual", "customedio", "custounitario", "custo", "precoatual", "precomedio"]

for alvo in esperadas:
    alvo_norm = _norm(alvo)

    if alvo == "CustoAtual":
        achou = None
        for cand in custo_aliases:
            if cand in norm2orig:
                achou = norm2orig[cand]
                break
        if achou:
            if achou != "CustoAtual":
                df_estoque.rename(columns={achou: "CustoAtual"}, inplace=True)
        else:
            df_estoque["CustoAtual"] = 0
        continue

    if alvo_norm in norm2orig:
        col_orig = norm2orig[alvo_norm]
        if col_orig != alvo:
            df_estoque.rename(columns={col_orig: alvo}, inplace=True)
    else:
        df_estoque[alvo] = 0

# converter para numérico
num_cols = ["Entradas", "Saidas", "Ajustes", "EstoqueAtual", "CustoAtual"]
df_estoque[num_cols] = df_estoque[num_cols].apply(pd.to_numeric, errors="coerce").fillna(0)

# ======================
# LÓGICA DE ESTOQUE
# ======================
df_estoque["EstoqueFinal"] = (
    df_estoque["Entradas"]
    - df_estoque["Saidas"]
    + df_estoque["Ajustes"]
    + df_estoque["EstoqueAtual"]
)

df_estoque["ValorTotal"] = df_estoque["EstoqueFinal"] * df_estoque["CustoAtual"]

# ======================
# EXIBIÇÃO
# ======================
st.subheader("📦 Estoque Atual")
st.dataframe(df_estoque, use_container_width=True, hide_index=True)

totais = {
    "Total Itens": df_estoque["EstoqueFinal"].sum(),
    "Valor Total (R$)": df_estoque["ValorTotal"].sum()
}
st.metric("Total de Itens", f"{totais['Total Itens']:.0f}")
st.metric("Valor Total", f"R$ {totais['Valor Total (R$)']:.2f}")
