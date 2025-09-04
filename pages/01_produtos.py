# pages/01_produtos.py
# -*- coding: utf-8 -*-
import re
import math
import time
import pandas as pd
import streamlit as st
import gspread
from google.oauth2.service_account import Credentials

# =========================
# CONFIG BÁSICA
# =========================
st.set_page_config(page_title="Produtos — Ebenezér Variedades", page_icon="📦", layout="wide")
st.title("📦 Produtos")

ABA_PRODUTOS = "Produtos"
COLS_PRODUTOS = [
    "ID", "Nome", "Categoria", "Unidade", "Fornecedor",
    "CustoAtual", "PreçoVenda", "Markup %", "Margem %",
    "EstoqueAtual", "EstoqueMin", "LeadTimeDias", "Ativo?"
]

# =========================
# CONEXÃO GOOGLE SHEETS
# =========================
@st.cache_resource(show_spinner=False)
def conectar_sheets(sheet_id: str):
    svc = st.secrets.get("GCP_SERVICE_ACCOUNT", None)
    if not svc:
        st.error("🚫 Faltam as credenciais em st.secrets['GCP_SERVICE_ACCOUNT'].")
        st.stop()
    scopes = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(svc, scopes=scopes)
    client = gspread.authorize(creds)
    return client.open_by_key(sheet_id)

def _obter_aba(sh, nome):
    try:
        return sh.worksheet(nome)
    except gspread.WorksheetNotFound:
        return None

def _criar_aba_produtos(sh):
    ws = sh.add_worksheet(title=ABA_PRODUTOS, rows=1000, cols=max(len(COLS_PRODUTOS)+3, 20))
    ws.update("A1", [COLS_PRODUTOS])
    return ws

def _garantir_estrutura_produtos(sh):
    ws = _obter_aba(sh, ABA_PRODUTOS)
    if ws is None:
        ws = _criar_aba_produtos(sh)
        return ws, True
    # garante cabeçalhos
    vals = ws.get_all_values()
    if not vals or not vals[0] or len(vals[0]) < len(COLS_PRODUTOS):
        ws.update("A1", [COLS_PRODUTOS])
    return ws, False

@st.cache_data(show_spinner=False)
def carregar_df_produtos(sheet_id: str) -> pd.DataFrame:
    sh = conectar_sheets(sheet_id)
    ws, _ = _garantir_estrutura_produtos(sh)
    data = ws.get_all_values()
    if not data:
        return pd.DataFrame(columns=COLS_PRODUTOS)
    df = pd.DataFrame(data[1:], columns=data[0] if data[0] else COLS_PRODUTOS)
    # garante colunas
    for c in COLS_PRODUTOS:
        if c not in df.columns:
            df[c] = ""
    # normaliza numéricos
    num_cols = ["CustoAtual", "PreçoVenda", "Markup %", "Margem %", "EstoqueAtual", "EstoqueMin", "LeadTimeDias"]
    for c in num_cols:
        df[c] = pd.to_numeric(df[c].replace("", pd.NA), errors="coerce")
    # Ativo?
    if "Ativo?" in df.columns:
        df["Ativo?"] = df["Ativo?"].fillna("").astype(str).str.upper().str.strip()
    return df[COLS_PRODUTOS]

def proximo_id(df: pd.DataFrame) -> str:
    """Gera próximo ID no padrão PRO-0001."""
    if df.empty or "ID" not in df.columns:
        return "PRO-0001"
    padrao = re.compile(r"PRO-(\d{4})$")
    numeros = []
    for x in df["ID"].dropna().astype(str):
        m = padrao.match(x.strip())
        if m:
            try:
                numeros.append(int(m.group(1)))
            except ValueError:
                pass
    n = (max(numeros) + 1) if numeros else 1
    return f"PRO-{n:04d}"

def pct_markup(custo: float, preco: float) -> float:
    if custo is None or preco is None or custo <= 0:
        return float("nan")
    return (preco - custo) / custo

def pct_margem(custo: float, preco: float) -> float:
    if preco is None or preco <= 0:
        return float("nan")
    return (preco - custo) / preco

def sim_nao(flag: bool) -> str:
    return "SIM" if flag else "NÃO"

def user_entered_append(ws, row_values):
    # salva permitindo números reais (não só texto)
    ws.append_row(row_values, value_input_option="USER_ENTERED")

# =========================
# CARREGAR DADOS
# =========================
SHEET_ID = st.secrets.get("SHEET_ID", "")
if not SHEET_ID:
    st.error("🚫 Configure o SHEET_ID nos *Secrets* do app.")
    st.stop()

df = carregar_df_produtos(SHEET_ID)

# =========================
# KPIs RÁPIDOS
# =========================
col_a, col_b, col_c = st.columns(3)
with col_a:
    total_itens = len(df)
    st.metric("Itens cadastrados", total_itens)
with col_b:
    ativos = (df["Ativo?"] == "SIM").sum() if not df.empty else 0
    st.metric("Ativos", int(ativos))
with col_c:
    criticos = ((pd.to_numeric(df["EstoqueAtual"], errors="coerce") <= pd.to_numeric(df["EstoqueMin"], errors="coerce")) & (df["Ativo?"] == "SIM")).sum() if not df.empty else 0
    st.metric("Estoque crítico", int(criticos))

st.divider()

# =========================
# FILTROS / LISTAGEM
# =========================
st.subheader("🔎 Lista de produtos")
col1, col2, col3 = st.columns([2,1,1])
with col1:
    termo = st.text_input("Buscar por nome / fornecedor / categoria", "")
with col2:
    categoria = st.selectbox(
        "Categoria",
        options=["(todas)"] + sorted([c for c in df["Categoria"].dropna().unique().tolist() if c]),
        index=0
    )
with col3:
    filtro_ativo = st.selectbox("Status", ["(todos)", "Ativos", "Inativos"], index=1)

df_view = df.copy()

if termo:
    t = termo.strip().lower()
    df_view = df_view[
        df_view["Nome"].astype(str).str.lower().str.contains(t)
        | df_view["Fornecedor"].astype(str).str.lower().str.contains(t)
        | df_view["Categoria"].astype(str).str.lower().str.contains(t)
    ]

if categoria != "(todas)":
    df_view = df_view[df_view["Categoria"].astype(str) == categoria]

if filtro_ativo == "Ativos":
    df_view = df_view[df_view["Ativo?"] == "SIM"]
elif filtro_ativo == "Inativos":
    df_view = df_view[df_view["Ativo?"] == "NÃO"]

st.dataframe(
    df_view.reset_index(drop=True),
    use_container_width=True,
    height=420
)

st.divider()

# =========================
# FORMULÁRIO: ADICIONAR PRODUTO
# =========================
st.subheader("➕ Adicionar novo produto")

with st.form("form_add_prod"):
    c1, c2 = st.columns([2,1])
    with c1:
        nome = st.text_input("Nome *", "")
        categoria_n = st.text_input("Categoria", "")
        fornecedor = st.text_input("Fornecedor", "")
    with c2:
        unidade = st.selectbox("Unidade", ["un", "L", "kg", "pct", "cx", "mL", "g"], index=0)
        estoque_atual = st.number_input("EstoqueAtual", min_value=0.0, value=0.0, step=1.0)
        estoque_min = st.number_input("EstoqueMin", min_value=0.0, value=0.0, step=1.0)
        leadtime = st.number_input("LeadTimeDias", min_value=0, value=3, step=1)

    c3, c4 = st.columns(2)
    with c3:
        custo = st.number_input("CustoAtual (R$) *", min_value=0.0, value=0.0, step=0.01, format="%.2f")
    with c4:
        preco = st.number_input("PreçoVenda (R$) *", min_value=0.0, value=0.0, step=0.01, format="%.2f")

    mk = pct_markup(float(custo), float(preco)) if (custo or preco) else float("nan")
    mg = pct_margem(float(custo), float(preco)) if (custo or preco) else float("nan")

    colmk, colmg, colatv = st.columns([1,1,1])
    with colmk:
        st.caption("Markup % (auto)")
        st.write("**" + (f"{mk*100:.2f} %" if not math.isnan(mk) else "—") + "**")
    with colmg:
        st.caption("Margem % (auto)")
        st.write("**" + (f"{mg*100:.2f} %" if not math.isnan(mg) else "—") + "**")
    with colatv:
        ativo_flag = st.checkbox("Ativo?", value=True)

    # Validação simples
    erro = None
    if st.form_submit_button("Salvar produto", use_container_width=True):
        if not nome.strip():
            erro = "Informe o **Nome**."
        elif float(preco) <= 0 or float(custo) < 0:
            erro = "Preencha **CustoAtual** e **PreçoVenda** válidos (Preço > 0)."
        else:
            # Verifica duplicidade por Nome (case-insensitive)
            if not df.empty and (df["Nome"].str.lower().str.strip() == nome.lower().strip()).any():
                erro = "Já existe um produto com esse **Nome**. Altere o nome ou edite o existente."

        if erro:
            st.error(erro)
        else:
            try:
                sh = conectar_sheets(SHEET_ID)
                ws, _ = _garantir_estrutura_produtos(sh)
                # recarrega df atualizado para gerar ID certo
                df_atual = carregar_df_produtos(SHEET_ID)
                novo_id = proximo_id(df_atual)

                # Monta a linha na ordem das colunas
                linha = [
                    novo_id,
                    nome.strip(),
                    categoria_n.strip(),
                    unidade,
                    fornecedor.strip(),
                    float(custo),
                    float(preco),
                    (pct_markup(float(custo), float(preco)) if float(custo) > 0 else ""),
                    (pct_margem(float(custo), float(preco)) if float(preco) > 0 else ""),
                    float(estoque_atual),
                    float(estoque_min),
                    int(leadtime),
                    sim_nao(ativo_flag),
                ]

                user_entered_append(ws, linha)
                st.success(f"✅ Produto **{nome}** salvo com ID **{novo_id}**.")
                # Limpa cache e recarrega lista
                carregar_df_produtos.clear()  # limpa cache
                time.sleep(0.4)
                st.rerun()
            except Exception as e:
                st.error(f"❌ Erro ao salvar: {e}")
