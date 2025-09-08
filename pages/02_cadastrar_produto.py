# -*- coding: utf-8 -*-
# pages/02_cadastrar_produto.py — Cadastrar/Editar Produtos
import json, unicodedata, re, math
import streamlit as st
import pandas as pd
import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from google.oauth2.service_account import Credentials
from datetime import datetime

st.set_page_config(page_title="Cadastrar/Editar Produtos", page_icon="➕", layout="wide")
st.title("➕ Cadastrar / Editar Produto")

# -----------------------------
# Helpers de credencial/Sheets
# -----------------------------
def _normalize_private_key(key: str) -> str:
    if not isinstance(key, str):
        return key
    key = key.replace("\\n", "\n")
    key = "".join(ch for ch in key if unicodedata.category(ch)[0] != "C" or ch in ("\n", "\r", "\t"))
    return key

def _load_sa():
    svc = st.secrets.get("GCP_SERVICE_ACCOUNT")
    if svc is None:
        st.error("🛑 GCP_SERVICE_ACCOUNT ausente em st.secrets."); st.stop()
    if isinstance(svc, str):
        svc = json.loads(svc)
    svc = dict(svc)
    svc["private_key"] = _normalize_private_key(svc["private_key"])
    return svc

@st.cache_resource
def _client():
    scopes = ["https://www.googleapis.com/auth/spreadsheets",
              "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(_load_sa(), scopes=scopes)
    return gspread.authorize(creds)

@st.cache_resource
def _sheet():
    gc = _client()
    url_or_id = st.secrets.get("PLANILHA_URL")
    if not url_or_id:
        st.error("🛑 PLANILHA_URL ausente em st.secrets."); st.stop()
    return gc.open_by_url(url_or_id) if str(url_or_id).startswith("http") else gc.open_by_key(url_or_id)

@st.cache_data
def carregar_produtos(aba="Produtos") -> pd.DataFrame:
    ws = _sheet().worksheet(aba)
    df = get_as_dataframe(ws, evaluate_formulas=True, dtype=str, header=0).dropna(how="all")
    df.columns = [c.strip() for c in df.columns]
    return df

def _ensure_cols(df: pd.DataFrame) -> pd.DataFrame:
    # Colunas "recomendadas" (flexível — só cria na hora de salvar se não existirem)
    cols = [
        "ID", "Nome", "Categoria", "Fornecedor",
        "Preço", "Estoque", "Ativo",
        "Código de Barras", "Descrição", "AtualizadoEm"
    ]
    for c in cols:
        if c not in df.columns:
            df[c] = ""
    return df[cols] if set(cols).issubset(df.columns) else df

def _to_float(x):
    if x is None or str(x).strip()=="":
        return ""
    s = str(x).strip().replace("R$", "").replace(".", "").replace(",", ".")
    try:
        return float(s)
    except:
        return ""

def _to_int(x):
    if x is None or str(x).strip()=="":
        return ""
    try:
        return int(float(str(x).strip()))
    except:
        return ""

def _gen_id():
    return "P-" + datetime.now().strftime("%Y%m%d%H%M%S")

def _msg_ok(msg):
    st.success(msg)
    # limpa caches de dados do catálogo para refletir alterações
    try:
        st.cache_data.clear()
    except:
        pass

# -----------------------------
# Carregar dados existentes
# -----------------------------
ABA = "Produtos"
try:
    df = carregar_produtos(ABA)
except Exception as e:
    st.error("Erro ao abrir a aba Produtos.")
    with st.expander("Detalhes técnicos"):
        st.code(str(e))
    st.stop()

df = _ensure_cols(df)

# -----------------------------
# UI — escolher ação
# -----------------------------
m1, m2 = st.columns([1.4, 2])
with m1:
    modo = st.radio("Ação", ["Cadastrar novo", "Editar existente"], horizontal=True)

with m2:
    st.caption("Dica: use nomes consistentes de **Categoria** e **Fornecedor** para facilitar filtros no catálogo.")

st.divider()

# -----------------------------
# Modo: Editar existente
# -----------------------------
if modo == "Editar existente":
    # Campo de busca e seleção
    l, r = st.columns([2,1])
    with l:
        termo = st.text_input("🔎 Buscar produto", placeholder="Nome, fornecedor, categoria, código de barras...")
    with r:
        apenas_ativos = st.checkbox("Apenas ativos", value=True)

    mask = pd.Series(True, index=df.index)
    if termo:
        t = termo.lower()
        mask &= df.apply(lambda row: t in " ".join([str(x).lower() for x in row.values]), axis=1)
    if apenas_ativos and "Ativo" in df.columns:
        mask &= df["Ativo"].astype(str).str.strip().str.lower().isin(["1","true","sim","ativo","yes"])

    dfe = df[mask].copy().reset_index(drop=True)
    if dfe.empty:
        st.info("Nada encontrado para os filtros atuais.")
        st.stop()

    nomes_fmt = dfe.apply(lambda r: f'{r.get("Nome","(sem nome)")} — {r.get("Fornecedor","(s/ forn)")} — R$ {r.get("Preço","")}', axis=1).tolist()
    idx = st.selectbox("Selecione um produto para editar", options=range(len(dfe)), format_func=lambda i: nomes_fmt[i])
    sel = dfe.iloc[idx].to_dict()

    st.subheader("Editar")
    with st.form("editar_produto"):
        c1, c2, c3 = st.columns([1.6,1,1])
        with c1:
            nome = st.text_input("Nome", value=str(sel.get("Nome","")).strip())
        with c2:
            categoria = st.text_input("Categoria", value=str(sel.get("Categoria","")).strip())
        with c3:
            fornecedor = st.text_input("Fornecedor", value=str(sel.get("Fornecedor","")).strip())

        c4, c5, c6 = st.columns([1,1,1])
        with c4:
            preco = st.text_input("Preço (R$)", value=str(sel.get("Preço","")).strip())
        with c5:
            estoque = st.text_input("Estoque (un)", value=str(sel.get("Estoque","")).strip())
        with c6:
            ativo = st.checkbox("Ativo", value=str(sel.get("Ativo","")).strip().lower() in ["1","true","sim","ativo","yes"])

        c7, c8 = st.columns([1,2])
        with c7:
            codb = st.text_input("Código de Barras", value=str(sel.get("Código de Barras","")).strip())
        with c8:
            desc = st.text_area("Descrição", value=str(sel.get("Descrição","")).strip(), height=100)

        salvar = st.form_submit_button("💾 Atualizar produto")

    if salvar:
        # Validações básicas
        if not nome.strip():
            st.error("Informe o **Nome**."); st.stop()
        pf = _to_float(preco)
        if pf=="":
            st.error("Preço inválido. Use números (ex: 19,90)."); st.stop()
        est = _to_int(estoque)
        if est=="":
            st.error("Estoque inválido. Use número inteiro."); st.stop()

        # Atualiza DataFrame em memória
        row_mask = (df["ID"] == sel.get("ID",""))
        if not row_mask.any():
            st.error("Não foi possível localizar a linha do produto para atualizar."); st.stop()

        df.loc[row_mask, ["Nome","Categoria","Fornecedor","Preço","Estoque","Ativo","Código de Barras","Descrição","AtualizadoEm"]] = [
            nome.strip(), categoria.strip(), fornecedor.strip(),
            f"{pf:.2f}".replace(".", ","), str(est),
            "1" if ativo else "0",
            codb.strip(), desc.strip(),
            datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        ]

        # Escrever de volta (mantendo cabeçalho)
        ws = _sheet().worksheet(ABA)
        # lê tudo bruto para preservar linhas
        df_old = get_as_dataframe(ws, evaluate_formulas=False, dtype=str, header=0)
        df_old.columns = [c.strip() for c in df_old.columns]
        # garante colunas
        df_old = _ensure_cols(df_old)
        # substitui pela versão atualizada (merge por ID)
        ids = df_old["ID"].tolist() if "ID" in df_old.columns else []
        for _, row in df.iterrows():
            if "ID" in row and row["ID"] in ids:
                i = ids.index(row["ID"])
                for col in df.columns:
                    df_old.loc[i, col] = row.get(col, "")
        # grava planilha inteira (mais simples e confiável para manter header)
        ws.clear()
        set_with_dataframe(ws, df_old.fillna(""), include_index=False, include_column_header=True, resize=True)

        _msg_ok("Produto atualizado com sucesso! Abra o catálogo para ver a mudança.")

# -----------------------------
# Modo: Cadastrar novo
# -----------------------------
else:
    st.subheader("Cadastrar novo produto")

    with st.form("cadastrar_produto"):
        c1, c2, c3 = st.columns([1.6,1,1])
        with c1:
            nome = st.text_input("Nome")
        with c2:
            categoria = st.text_input("Categoria")
        with c3:
            fornecedor = st.text_input("Fornecedor")

        c4, c5, c6 = st.columns([1,1,1])
        with c4:
            preco = st.text_input("Preço (R$)", placeholder="19,90")
        with c5:
            estoque = st.text_input("Estoque (un)", placeholder="0")
        with c6:
            ativo = st.checkbox("Ativo", value=True)

        c7, c8 = st.columns([1,2])
        with c7:
            codb = st.text_input("Código de Barras", placeholder="opcional")
        with c8:
            desc = st.text_area("Descrição", placeholder="Ex.: Cor, tamanho, material, observações...", height=100)

        salvar = st.form_submit_button("➕ Cadastrar produto")

    if salvar:
        # Validações
        if not nome.strip():
            st.error("Informe o **Nome**."); st.stop()
        pf = _to_float(preco)
        if pf=="":
            st.error("Preço inválido. Use números (ex: 19,90)."); st.stop()
        est = _to_int(estoque)
        if est=="":
            st.error("Estoque inválido. Use número inteiro."); st.stop()

        # Checagem de duplicidade por Nome+Fornecedor
        dup = df[
            df["Nome"].astype(str).str.strip().str.lower().eq(nome.strip().lower()) &
            df["Fornecedor"].astype(str).str.strip().str.lower().eq(fornecedor.strip().lower())
        ]
        if not dup.empty:
            st.warning("Já existe um produto com o **mesmo Nome e Fornecedor**. Considere editar o existente.")
            st.dataframe(dup[["ID","Nome","Fornecedor","Preço","Estoque","Ativo"]], use_container_width=True, hide_index=True)
            st.stop()

        novo = {
            "ID": _gen_id(),
            "Nome": nome.strip(),
            "Categoria": categoria.strip(),
            "Fornecedor": fornecedor.strip(),
            "Preço": f"{pf:.2f}".replace(".", ","),
            "Estoque": str(est),
            "Ativo": "1" if ativo else "0",
            "Código de Barras": codb.strip(),
            "Descrição": desc.strip(),
            "AtualizadoEm": datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
        }

        # Append na planilha (preserva header)
        ws = _sheet().worksheet(ABA)
        df_atual = get_as_dataframe(ws, evaluate_formulas=False, dtype=str, header=0)
        df_atual.columns = [c.strip() for c in df_atual.columns]
        df_atual = _ensure_cols(df_atual)
        df_atual = pd.concat([df_atual, pd.DataFrame([novo])], ignore_index=True)
        set_with_dataframe(ws, df_atual.fillna(""), include_index=False, include_column_header=True, resize=True)

        _msg_ok("Produto cadastrado com sucesso! Ele já aparece no catálogo.")
        st.toast("Cadastro concluído ✅", icon="✅")
        st.balloons()

st.divider()
st.page_link("pages/01_produtos.py", label="↩️ Ir para Catálogo de Produtos", icon="📦")
