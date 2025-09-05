# pages/01_produtos.py
# -*- coding: utf-8 -*-
import json
import unicodedata
from typing import Optional

import pandas as pd
import streamlit as st

import gspread
from gspread_dataframe import get_as_dataframe
from google.oauth2.service_account import Credentials


st.set_page_config(page_title="Produtos — Ebenezér Variedades", page_icon="🧾", layout="wide")
st.title("🧾 Produtos — Ebenezér Variedades")

# =========================
# CONFIG INICIAL
# =========================
DEFAULT_SHEET_ID = "1q3XZB9Pv7fj87d-FBd4Ver5uRzIMM2kVf2jVLMvY5u0"  # substitua se quiser
ABA_PRODUTOS = "Produtos"  # nome da aba/worksheet

# Campo para o usuário confirmar/alterar a planilha
SHEET_ID = st.text_input(
    "Google Sheet ID ou URL da planilha",
    value=DEFAULT_SHEET_ID,
    help="Cole apenas o ID (entre /d/ e /edit) ou a URL completa do Google Sheets."
)

# =========================
# HELPERS DE SEGREDO E CONEXÃO
# =========================
def _normalize_private_key(key: str) -> str:
    """
    Corrige o private_key quando veio com '\\n' literais (formato comum do secrets).
    Também remove BOM/espaços invisíveis acidentais.
    """
    if not isinstance(key, str):
        return key
    key = key.replace("\\n", "\n")
    # Remove caracteres de controle invisíveis
    key = "".join(ch for ch in key if unicodedata.category(ch)[0] != "C" or ch in ("\n", "\r", "\t"))
    return key

def _load_service_account_from_secrets() -> Optional[dict]:
    """
    Lê o service account de st.secrets["GCP_SERVICE_ACCOUNT"].
    Aceita:
      - dict diretamente
      - string JSON
    Normaliza o campo private_key.
    Retorna dict pronto para Credentials.from_service_account_info ou None se não existir.
    """
    if "GCP_SERVICE_ACCOUNT" not in st.secrets:
        return None

    svc = st.secrets["GCP_SERVICE_ACCOUNT"]

    # Pode vir como dict (ok) ou como string JSON
    if isinstance(svc, str):
        try:
            svc = json.loads(svc)
        except Exception as e:
            st.error("🛑 'GCP_SERVICE_ACCOUNT' em Secrets parece ser uma string inválida (JSON malformado).")
            st.caption(f"Detalhe: {e}")
            return None

    if not isinstance(svc, dict):
        st.error("🛑 'GCP_SERVICE_ACCOUNT' precisa ser um JSON/objeto com as chaves do Service Account.")
        return None

    # Campos mínimos
    required = ["type", "project_id", "private_key_id", "private_key", "client_email", "token_uri"]
    missing = [k for k in required if k not in svc]
    if missing:
        st.error(f"🛑 Faltam campos no Service Account: {', '.join(missing)}")
        return None

    # Normaliza a private_key
    svc = {**svc, "private_key": _normalize_private_key(svc["private_key"])}

    return svc

@st.cache_resource(show_spinner=True)
def conectar_sheets(sheet_id: str):
    """
    Abre a planilha via gspread usando Service Account do secrets.
    Retorna objeto Spreadsheet do gspread.
    """
    svc = _load_service_account_from_secrets()
    if svc is None:
        raise ValueError(
            "Segredo 'GCP_SERVICE_ACCOUNT' não configurado corretamente. "
            "Adicione o JSON do Service Account em Settings → Secrets e compartilhe a planilha "
            "com o e-mail do service account (Editor)."
        )

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(svc, scopes=scopes)
    gc = gspread.authorize(creds)

    # Aceita URL completa ou apenas ID
    if sheet_id.startswith("http"):
        return gc.open_by_url(sheet_id)
    return gc.open_by_key(sheet_id)

@st.cache_data(show_spinner=True)
def carregar_df_produtos(sheet_id: str, aba: str) -> pd.DataFrame:
    """
    Tenta carregar via API (autenticada). Se falhar por auth/perm, tenta fallback CSV público.
    """
    # 1) Tenta via API autenticada
    try:
        sh = conectar_sheets(sheet_id)
        ws = sh.worksheet(aba)
        df = get_as_dataframe(ws, evaluate_formulas=True, dtype=str, header=1)
        df = df.dropna(how="all")
        return df
    except Exception as e_api:
        # 2) Fallback: CSV público (se a planilha/aba estiver publicada ou com link público)
        try:
            # csv via gviz (precisa estar com compartilhamento Público ou Publicado na Web)
            if sheet_id.startswith("http"):
                # extrai ID
                # URL típica: https://docs.google.com/spreadsheets/d/<ID>/edit...
                parts = sheet_id.split("/d/")
                if len(parts) > 1:
                    sheet_id = parts[1].split("/")[0]
            url_csv = (
                "https://docs.google.com/spreadsheets/d/"
                f"{sheet_id}/gviz/tq?tqx=out:csv&sheet={aba.replace(' ', '%20')}"
            )
            df = pd.read_csv(url_csv)
            df = df.dropna(how="all")
            st.info("⚠️ Carregado pelo fallback CSV público (verifique permissões do Service Account para usar a API).")
            return df
        except Exception as e_csv:
            # Propaga o erro original + msg do csv
            raise ValueError(
                "Falha ao carregar a aba de Produtos. "
                f"API: {type(e_api).__name__}: {e_api} | CSV: {type(e_csv).__name__}: {e_csv}"
            ) from e_csv

# =========================
# CARREGAR DADOS
# =========================
if not SHEET_ID.strip():
    st.warning("Informe o **Sheet ID** ou a **URL** da planilha.")
    st.stop()

try:
    df = carregar_df_produtos(SHEET_ID.strip(), ABA_PRODUTOS)
except Exception as e:
    st.error("Não consegui abrir a planilha com o Service Account.")
    with st.expander("Detalhes técnicos (clique para abrir)"):
        st.code(str(e))
    st.stop()

if df.empty:
    st.warning(f"A aba **{ABA_PRODUTOS}** está vazia ou não foi encontrada.")
    st.stop()

# =========================
# LIMPEZA BÁSICA
# =========================
def _clean_cols(df: pd.DataFrame) -> pd.DataFrame:
    df2 = df.copy()
    df2.columns = [c.strip() for c in df2.columns]
    return df2

df = _clean_cols(df)

# Nome padrão de colunas esperadas (ajuste conforme sua aba)
col_sku = "SKU" if "SKU" in df.columns else None
col_nome = "Produto" if "Produto" in df.columns else ("Nome" if "Nome" in df.columns else None)
col_preco = "Preço" if "Preço" in df.columns else ("Preco" if "Preco" in df.columns else None)
col_estoque = "Estoque" if "Estoque" in df.columns else ("Qtd" if "Qtd" in df.columns else None)
col_categoria = "Categoria" if "Categoria" in df.columns else None

# =========================
# FILTROS / UI
# =========================
left, right = st.columns([2, 1])

with left:
    termo = st.text_input("🔎 Buscar produto", value="", placeholder="Digite nome, SKU, categoria...").strip()

with right:
    if col_categoria:
        categorias = ["(todas)"] + sorted([c for c in df[col_categoria].dropna().astype(str).unique()])
        cat = st.selectbox("Categoria", categorias, index=0)
    else:
        cat = "(todas)"

# Filtro por texto
mask = pd.Series([True] * len(df))
if termo:
    termo_low = termo.lower()
    mask = mask & df.apply(lambda r: termo_low in " ".join([str(x).lower() for x in r.values]), axis=1)

# Filtro por categoria
if col_categoria and cat != "(todas)":
    mask = mask & (df[col_categoria].astype(str) == cat)

df_view = df[mask].reset_index(drop=True)

st.subheader("📦 Lista de Produtos")
st.dataframe(df_view, use_container_width=True, hide_index=True)

# KPIs simples
k1, k2, k3, k4 = st.columns(4)
total_itens = len(df_view)
total_skus = df_view[col_sku].nunique() if col_sku else total_itens
estoque_total = pd.to_numeric(df_view[col_estoque], errors="coerce").fillna(0).sum() if col_estoque else None
preco_medio = pd.to_numeric(df_view[col_preco], errors="coerce").mean() if col_preco else None

k1.metric("Itens exibidos", f"{total_itens}")
k2.metric("SKUs distintos", f"{total_skus}")
k3.metric("Estoque total", f"{int(estoque_total)}" if estoque_total is not None else "—")
k4.metric("Preço médio", f"R$ {preco_medio:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".") if preco_medio is not None else "—")

st.success("✅ Conexão OK. Se aparecer erro de credencial novamente, verifique:\n"
           "1) **Secrets**: `GCP_SERVICE_ACCOUNT` com o JSON completo;\n"
           "2) **private_key** com quebras de linha reais (o código acima normaliza `\\n`);\n"
           "3) **Compartilhamento**: dê acesso **Editor** ao e-mail do Service Account na planilha.\n"
           "4) Se usar URL, o código extrai o ID automaticamente.")
