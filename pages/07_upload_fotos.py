# pages/upload_fotos.py
# -*- coding: utf-8 -*-
import json
import unicodedata as _ud
from typing import Optional

import streamlit as st
import pandas as pd
import gspread
from gspread_dataframe import get_as_dataframe
from google.oauth2.service_account import Credentials

st.set_page_config(page_title="Upload de Fotos (Produtos)", page_icon="🖼️", layout="wide")
st.title("🖼️ Upload/URL de Foto para Produtos")

# ======================================================================
# Config / Conexão
# ======================================================================
ABA_PRODUTOS = "Produtos"  # nome da sua aba de catálogo

def _normalize_private_key(key: str) -> str:
    if not isinstance(key, str):
        return key
    key = key.replace("\\n", "\n")
    # remove chars de controle exceto \n \r \t
    key = "".join(ch for ch in key if _ud.category(ch)[0] != "C" or ch in ("\n", "\r", "\t"))
    return key

def _load_sa():
    svc = st.secrets.get("GCP_SERVICE_ACCOUNT")
    if svc is None:
        st.error("🛑 Falta o secret GCP_SERVICE_ACCOUNT.")
        st.stop()
    if isinstance(svc, str):
        svc = json.loads(svc)
    svc = dict(svc)
    svc["private_key"] = _normalize_private_key(svc["private_key"])
    return svc

@st.cache_resource
def _sheet():
    scopes = ["https://www.googleapis.com/auth/spreadsheets",
              "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(_load_sa(), scopes=scopes)
    gc = gspread.authorize(creds)
    url_or_id = st.secrets.get("PLANILHA_URL") or st.secrets.get("PLANILHA_ID")
    if not url_or_id:
        st.error("🛑 Coloque PLANILHA_URL ou PLANILHA_ID nos Secrets.")
        st.stop()
    return gc.open_by_url(url_or_id) if str(url_or_id).startswith("http") else gc.open_by_key(url_or_id)

def _headers(ws) -> list[str]:
    try:
        return [h.strip() for h in ws.row_values(1)]
    except Exception:
        return []

def _find_col(headers: list[str], candidates: list[str]) -> Optional[int]:
    """retorna índice 1-based da primeira coluna que casar (case-insensitive)."""
    if not headers:
        return None
    lower = {h.lower(): i+1 for i, h in enumerate(headers)}
    for c in candidates:
        if c.lower() in lower:
            return lower[c.lower()]
    return None

def _ensure_foto_col(ws) -> tuple[str, int]:
    """Garante que exista uma coluna para foto. Retorna (nome, índice 1-based)."""
    hdrs = _headers(ws)
    foto_idx = _find_col(hdrs, ["Foto", "FotoURL", "Imagem", "Image", "Link", "UrlFoto", "URL_Foto"])
    if foto_idx:
        return hdrs[foto_idx-1], foto_idx
    # cria uma nova no fim chamada "Foto"
    new_idx = len(hdrs) + 1 if hdrs else 1
    ws.update_cell(1, new_idx, "Foto")
    return "Foto", new_idx

@st.cache_data(ttl=20, show_spinner=False)
def carregar_produtos() -> pd.DataFrame:
    ws = _sheet().worksheet(ABA_PRODUTOS)
    df = get_as_dataframe(ws, evaluate_formulas=True, dtype=str, header=0).dropna(how="all")
    df.columns = [c.strip() for c in df.columns]
    return df.fillna("")

# ======================================================================
# UI — escolher produto (mostrar só o NOME)
# ======================================================================
dfp = carregar_produtos()
if dfp.empty:
    st.info("Nenhum produto cadastrado na aba **Produtos**.")
    st.stop()

# tenta adivinhar a coluna de nome
NOME_CANDS = ["Nome", "Produto", "Descrição", "Descricao", "Título", "Titulo"]
col_nome = None
for c in NOME_CANDS:
    if c in dfp.columns:
        col_nome = c
        break
if not col_nome:
    st.error("Não encontrei uma coluna de nome (ex.: Nome/Produto/Descrição) na aba Produtos.")
    st.stop()

st.subheader("Selecione o produto")
# usamos o índice como 'valor' e mostramos apenas o nome
opcoes = dfp.index.tolist()
sel_idx = st.selectbox(
    "Produto",
    options=opcoes,
    format_func=lambda i: str(dfp.loc[i, col_nome] or "(sem nome)")
)
nome_prod = str(dfp.loc[sel_idx, col_nome] or "").strip()

# ======================================================================
# URL manual de imagem
# ======================================================================
st.divider()
st.subheader("Colar URL da imagem")
url = st.text_input("URL da imagem (https…)", value="", placeholder="https://…")
preview_size = st.slider("Tamanho da prévia (px)", 120, 600, 320, 10)

col1, col2 = st.columns([0.5, 0.5])
with col1:
    if st.button("💾 Salvar URL manual no catálogo", type="primary"):
        if not url.strip():
            st.warning("Cole uma URL primeiro.")
        else:
            try:
                sh = _sheet()
                ws = sh.worksheet(ABA_PRODUTOS)
                # acha/cria coluna Foto
                _, foto_col = _ensure_foto_col(ws)
                # linha do produto (DataFrame começa em 0; planilha começa na linha 2)
                target_row = int(sel_idx) + 2
                ws.update_cell(target_row, foto_col, url.strip())
                st.success("URL salva no catálogo!")
                # força refresh para outras páginas
                st.session_state["_force_refresh"] = True
            except Exception as e:
                st.error(f"Erro ao salvar: {e}")

with col2:
    # só para visual: limitar tamanho
    if url.strip():
        st.image(url.strip(), width=preview_size, caption=nome_prod)

# dica
st.caption("A URL fica apenas registrada na coluna **Foto** da aba Produtos. O arquivo continua hospedado no endereço da URL.")

# ======================================================================
# Preview do que está salvo (se já houver)
# ======================================================================
st.divider()
st.subheader("Pré-visualização do que está salvo")
try:
    # recarrega rápido só o produto selecionado
    ws = _sheet().worksheet(ABA_PRODUTOS)
    hdrs = _headers(ws)
    foto_idx = _find_col(hdrs, ["Foto", "FotoURL", "Imagem", "Image", "Link", "UrlFoto", "URL_Foto"])
    if foto_idx:
        foto_url_salva = dfp.iloc[sel_idx, foto_idx-1] if (foto_idx-1) < len(dfp.columns) else ""
        if str(foto_url_salva).strip():
            st.image(str(foto_url_salva).strip(), width=preview_size, caption=f"{nome_prod} (salvo)")
        else:
            st.info("Este produto ainda não tem foto salva.")
    else:
        st.info("A planilha ainda não tem a coluna **Foto**.")
except Exception:
    st.info("Não consegui carregar a foto salva agora, mas a URL foi gravada.")
