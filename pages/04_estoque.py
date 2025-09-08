# pages/02_estoque.py — Estoque: Movimentos & Ajustes
# -*- coding: utf-8 -*-
import io, json, unicodedata
import streamlit as st
import pandas as pd
import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from google.oauth2.service_account import Credentials
from datetime import datetime

st.set_page_config(page_title="Estoque — Ebenezér Variedades", page_icon="📦", layout="wide")
st.title("📦 Estoque — Movimentos & Ajustes")

# =========================
# Utilitários
# =========================
def _normalize_private_key(key: str) -> str:
    if not isinstance(key, str):
        return key
    key = key.replace("\\n", "\n")
    key = "".join(ch for ch in key if unicodedata.category(ch)[0] != "C" or ch in ("\n", "\r", "\t"))
    return key

def _load_sa():
    svc = st.secrets.get("GCP_SERVICE_ACCOUNT")
    if svc is None:
        st.error("🛑 GCP_SERVICE_ACCOUNT ausente."); st.stop()
    if isinstance(svc, str):
        svc = json.loads(svc)
    svc = dict(svc)
    svc["private_key"] = _normalize_private_key(svc["private_key"])
    return svc

@st.cache_resource
def conectar_sheets():
    svc = _load_sa()
    scopes = ["https://www.googleapis.com/auth/spreadsheets",
              "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(svc, scopes=scopes)
    gc = gspread.authorize(creds)
    url_or_id = st.secrets.get("PLANILHA_URL")
    if not url_or_id:
        st.error("🛑 PLANILHA_URL ausente."); st.stop()
    return gc.open_by_url(url_or_id) if str(url_or_id).startswith("http") else gc.open_by_key(url_or_id)

@st.cache_data
def carregar_aba(nome_aba: str) -> pd.DataFrame:
    sh = conectar_sheets()
    ws = sh.worksheet(nome_aba)
    df = get_as_dataframe(ws, evaluate_formulas=True, dtype=str, header=0).dropna(how="all")
    df.columns = [c.strip() for c in df.columns]
    return df

def _first_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for c in candidates:
        if c in df.columns:
            return c
    lower_map = {c.lower(): c for c in df.columns}
    for c in candidates:
        if c.lower() in lower_map:
            return lower_map[c.lower()]
    return None

def _to_num(s):
    if s is None:
        return 0.0
    if isinstance(s, (int, float)):
        return float(s)
    s = str(s).strip()
    if s == "" or s.lower() in ("nan", "none"):
        return 0.0
    # vírgula como decimal
    s = s.replace(".", "").replace(",", ".") if s.count(",") == 1 and s.count(".") > 1 else s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return 0.0

def _to_int(s):
    try:
        return int(round(_to_num(s)))
    except Exception:
        return 0

# =========================
# Nomes das abas
# =========================
ABA_PRODUTOS = "Produtos"
ABA_COMPRAS  = "Compras"
ABA_VENDAS   = "Vendas"
ABA_AJUSTES  = "Ajustes"   # será criada se não existir

# =========================
# Carregamento
# =========================
try:
    df_prod = carregar_aba(ABA_PRODUTOS)
except Exception as e:
    st.error("Erro ao abrir a aba Produtos.")
    with st.expander("Detalhes técnicos"):
        st.code(str(e))
    st.stop()

# Compras/Vendas podem não existir ainda — tratamos como vazias
try:
    df_comp = carregar_aba(ABA_COMPRAS)
except Exception:
    df_comp = pd.DataFrame()

try:
    df_vend = carregar_aba(ABA_VENDAS)
except Exception:
    df_vend = pd.DataFrame()

# Ajustes: se não existir, criaremos ao salvar
try:
    df_aj = carregar_aba(ABA_AJUSTES)
except Exception:
    df_aj = pd.DataFrame()

# =========================
# Identificação de colunas
# =========================
# Produtos
col_id_prod    = _first_col(df_prod, ["ID", "Id", "Codigo", "Código", "SKU"])
col_nome       = _first_col(df_prod, ["Nome", "Produto", "Descrição"])
col_cat        = _first_col(df_prod, ["Categoria"])
col_forn_prod  = _first_col(df_prod, ["Fornecedor"])
col_estq_min   = _first_col(df_prod, ["EstoqueMin", "Estoque Mínimo", "EstqMin"])
col_preco      = _first_col(df_prod, ["PreçoVenda", "PrecoVenda", "Preço", "Preco"])
col_custo_atual= _first_col(df_prod, ["CustoAtual", "Custo Médio", "CustoMedio"])

# Compras
col_comp_idprod = _first_col(df_comp, ["IDProduto", "IdProduto", "ProdutoID", "ID Prod", "ID_Produto"])
col_comp_qtd    = _first_col(df_comp, ["Qtd", "Quantidade", "Qtde", "Qde"])
col_comp_custo  = _first_col(df_comp, ["Custo Unitário", "CustoUnitário", "CustoUnit", "Custo Unit", "Custo"])
col_comp_data   = _first_col(df_comp, ["Data", "Emissão", "Dt", "Data da compra"])

# Vendas
col_vend_idprod = _first_col(df_vend, ["IDProduto", "IdProduto", "ProdutoID", "ID Prod", "ID_Produto"])
col_vend_qtd    = _first_col(df_vend, ["Qtd", "Quantidade", "Qtde", "Qde"])
col_vend_data   = _first_col(df_vend, ["Data", "Dt", "DataVenda"])

# Ajustes
col_aj_idprod = _first_col(df_aj, ["IDProduto", "IdProduto", "ProdutoID", "ID Prod", "ID_Produto"])
col_aj_qtd    = _first_col(df_aj, ["Qtd", "Quantidade", "Qtde", "Qde", "Ajuste"])
col_aj_data   = _first_col(df_aj, ["Data", "Dt"])
col_aj_motivo = _first_col(df_aj, ["Motivo", "Obs", "Observação", "Observacoes"])

# =========================
# Cálculo de estoque e custo médio
# =========================
entradas = pd.Series(dtype=float)
custo_medio = pd.Series(dtype=float)

if not df_comp.empty and col_comp_idprod and col_comp_qtd:
    comp = df_comp[[col_comp_idprod, col_comp_qtd]].copy()
    comp[col_comp_qtd] = comp[col_comp_qtd].map(_to_num)
    entradas = comp.groupby(col_comp_idprod, dropna=True)[col_comp_qtd].sum()

    if col_comp_custo:
        comp_cost = df_comp[[col_comp_idprod, col_comp_qtd, col_comp_custo]].copy()
        comp_cost[col_comp_qtd]  = comp_cost[col_comp_qtd].map(_to_num)
        comp_cost[col_comp_custo]= comp_cost[col_comp_custo].map(_to_num)
        comp_cost["parcial"] = comp_cost[col_comp_qtd] * comp_cost[col_comp_custo]
        soma_parcial = comp_cost.groupby(col_comp_idprod)["parcial"].sum()
        soma_qtd     = comp_cost.groupby(col_comp_idprod)[col_comp_qtd].sum().replace(0, pd.NA)
        custo_medio = (soma_parcial / soma_qtd).fillna(0.0)

saidas = pd.Series(dtype=float)
if not df_vend.empty and col_vend_idprod and col_vend_qtd:
    vend = df_vend[[col_vend_idprod, col_vend_qtd]].copy()
    vend[col_vend_qtd] = vend[col_vend_qtd].map(_to_num)
    saidas = vend.groupby(col_vend_idprod, dropna=True)[col_vend_qtd].sum()

ajustes = pd.Series(dtype=float)
if not df_aj.empty and col_aj_idprod and col_aj_qtd:
    aj = df_aj[[col_aj_idprod, col_aj_qtd]].copy()
    aj[col_aj_qtd] = aj[col_aj_qtd].map(_to_num)
    ajustes = aj.groupby(col_aj_idprod, dropna=True)[col_aj_qtd].sum()

calc = pd.DataFrame({
    "Entradas": entradas,
    "Saidas":   saidas,
    "Ajustes":  ajustes
}).fillna(0.0)

calc["EstoqueAtual"] = calc["Entradas"] - calc["Saidas"] + calc["Ajustes"]
calc["CustoAtual"]   = custo_medio if not custo_medio.empty else 0.0
calc = calc.reset_index().rename(columns={"index": "ID_join"})

# Merge com Produtos
if not col_id_prod:
    st.error("Não encontrei a coluna de ID na aba Produtos (ex.: 'ID')."); st.stop()

df_prod["_ID_join"] = df_prod[col_id_prod].astype(str)
df_estoque = df_prod.merge(calc, how="left", left_on="_ID_join", right_on="ID_join").drop(columns=["ID_join"])
df_estoque[["Entradas", "Saidas", "Ajustes", "EstoqueAtual", "CustoAtual"]] = \
    df_estoque[["Entradas", "Saidas", "Ajustes", "EstoqueAtual", "CustoAtual"]].fillna(0).applymap(_to_num)

# Valor do estoque (custo médio * estoque atual)
df_estoque["ValorEstoque"] = df_estoque["CustoAtual"] * df_estoque["EstoqueAtual"]

# =========================
# KPIs
# =========================
col1, col2, col3 = st.columns(3)
with col1:
    st.metric("SKUs cadastrados", len(df_estoque))
with col2:
    baixo = 0
    if col_estq_min and col_estq_min in df_estoque.columns:
        baixo = int(((df_estoque["EstoqueAtual"] <= df_estoque[col_estq_min].map(_to_num)).fillna(False)).sum())
    st.metric("Itens em baixo estoque", baixo)
with col3:
    st.metric("Valor de estoque (R$)", f"{df_estoque['ValorEstoque'].sum():,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))

st.divider()

# =========================
# Filtros
# =========================
top, mid = st.columns([2.5, 1.5])
with top:
    termo = st.text_input("🔎 Buscar", placeholder="ID, nome, fornecedor, categoria...").strip()
with mid:
    only_low = st.checkbox("⚠️ Somente baixo estoque", value=False)

cat_col, forn_col = st.columns(2)
with cat_col:
    if col_cat and col_cat in df_estoque.columns:
        cats = ["(todas)"] + sorted(pd.Series(df_estoque[col_cat].dropna().astype(str).unique()).tolist())
        cat = st.selectbox("Categoria", cats, index=0)
    else:
        cat = "(todas)"
with forn_col:
    if col_forn_prod and col_forn_prod in df_estoque.columns:
        forns = ["(todos)"] + sorted(pd.Series(df_estoque[col_forn_prod].dropna().astype(str).unique()).tolist())
        forn = st.selectbox("Fornecedor", forns, index=0)
    else:
        forn = "(todos)"

mask = pd.Series(True, index=df_estoque.index)
if termo:
    t = termo.lower()
    mask &= df_estoque.apply(lambda r: t in " ".join([str(x).lower() for x in r.values]), axis=1)
if col_cat and cat != "(todas)":
    mask &= (df_estoque[col_cat].astype(str) == cat)
if col_forn_prod and forn != "(todos)":
    mask &= (df_estoque[col_forn_prod].astype(str) == forn)
if only_low and col_estq_min and col_estq_min in df_estoque.columns:
    mask &= (df_estoque["EstoqueAtual"] <= df_estoque[col_estq_min].map(_to_num))

dfv = df_estoque[mask].reset_index(drop=True)

# Tabela
cols_show = [c for c in [
    col_id_prod, col_nome, col_cat, col_forn_prod,
    "Entradas", "Saidas", "Ajustes", "EstoqueAtual",
    col_estq_min, "CustoAtual", "ValorEstoque", col_preco
] if c and c in dfv.columns]

st.dataframe(dfv[cols_show] if cols_show else dfv, use_container_width=True, hide_index=True)

# Exportação
csv = dfv[cols_show].to_csv(index=False).encode("utf-8-sig") if cols_show else dfv.to_csv(index=False).encode("utf-8-sig")
st.download_button("⬇️ Baixar CSV (filtro aplicado)", data=csv, file_name="estoque.csv", mime="text/csv")

st.caption("• **EstoqueAtual = Compras − Vendas ± Ajustes** • **CustoAtual = custo médio ponderado das compras**")

st.divider()

# =========================
# Histórico por produto
# =========================
st.subheader("🧾 Histórico de movimentos por produto")

# Seletor de produto
opts = df_estoque[[col_id_prod, col_nome]].copy() if col_nome else df_estoque[[col_id_prod]].copy()
opts = opts.dropna().astype(str)
sel = st.selectbox(
    "Escolha o produto",
    ["(selecionar)"] + (opts.apply(lambda r: " — ".join(r.values), axis=1).tolist() if not opts.empty else [])
)

if sel != "(selecionar)":
    pid = sel.split(" — ")[0]
    # Compras do produto
    if not df_comp.empty and col_comp_idprod:
        comp_hist = df_comp[df_comp[col_comp_idprod].astype(str) == pid].copy()
        if col_comp_qtd:  comp_hist[col_comp_qtd]  = comp_hist[col_comp_qtd].map(_to_num)
        if col_comp_custo:comp_hist[col_comp_custo]= comp_hist[col_comp_custo].map(_to_num)
        st.markdown("**Entradas (Compras)**")
        st.dataframe(comp_hist, use_container_width=True, hide_index=True)
    else:
        st.info("Sem registros de compras para este produto.")

    # Vendas do produto
    if not df_vend.empty and col_vend_idprod:
        vend_hist = df_vend[df_vend[col_vend_idprod].astype(str) == pid].copy()
        if col_vend_qtd: vend_hist[col_vend_qtd] = vend_hist[col_vend_qtd].map(_to_num)
        st.markdown("**Saídas (Vendas)**")
        st.dataframe(vend_hist, use_container_width=True, hide_index=True)
    else:
        st.info("Sem registros de vendas para este produto.")

    # Ajustes do produto
    if not df_aj.empty and col_aj_idprod:
        aj_hist = df_aj[df_aj[col_aj_idprod].astype(str) == pid].copy()
        if col_aj_qtd: aj_hist[col_aj_qtd] = aj_hist[col_aj_qtd].map(_to_num)
        st.markdown("**Ajustes**")
        st.dataframe(aj_hist, use_container_width=True, hide_index=True)
    else:
        st.info("Sem registros de ajustes para este produto.")

st.divider()

# =========================
# Lançar Ajuste de Estoque
# =========================
st.subheader("➕ Lançar ajuste de estoque")

with st.form("form_ajuste"):
    colA, colB = st.columns([2.2, 1])
    with colA:
        prod_escolha = st.selectbox("Produto", opts.apply(lambda r: " — ".join(r.values), axis=1) if not opts.empty else [])
    with colB:
        data_ajuste = st.date_input("Data", value=datetime.now().date(), format="DD/MM/YYYY")
    col1, col2 = st.columns([1, 3])
    with col1:
        qtd = st.number_input("Quantidade (positivo entra / negativo sai)", value=0, step=1, format="%d")
    with col2:
        motivo = st.text_input("Motivo/Observação", placeholder="quebra, perda, acerto, inventário...")

    enviado = st.form_submit_button("Salvar ajuste")

if enviado:
    if not prod_escolha:
        st.error("Selecione um produto.")
    elif qtd == 0:
        st.error("Informe uma quantidade diferente de zero.")
    else:
        pid = prod_escolha.split(" — ")[0]
        # Garantir a planilha Ajustes (cria se não existir)
        sh = conectar_sheets()
        try:
            ws_aj = sh.worksheet(ABA_AJUSTES)
        except Exception:
            ws_aj = sh.add_worksheet(title=ABA_AJUSTES, rows=1000, cols=6)
            headers = [["Data", "IDProduto", "Qtd", "Motivo"]]
            ws_aj.update("A1:D1", headers)

        # Lê planilha atual para append seguro
        df_atual = get_as_dataframe(ws_aj, evaluate_formulas=False, dtype=str, header=0)
        df_atual = df_atual.dropna(how="all")
        df_atual.columns = [c.strip() for c in df_atual.columns] if len(df_atual) else ["Data", "IDProduto", "Qtd", "Motivo"]

        nova_linha = {
            "Data":    data_ajuste.strftime("%d/%m/%Y"),
            "IDProduto": pid,
            "Qtd":     str(int(qtd)),
            "Motivo":  motivo.strip(),
        }

        df_novo = pd.concat([df_atual, pd.DataFrame([nova_linha])], ignore_index=True)
        ws_aj.clear()
        set_with_dataframe(ws_aj, df_novo)

        st.success("Ajuste salvo com sucesso! Recarregue a página para ver o efeito no estoque.")
