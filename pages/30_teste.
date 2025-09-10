# -*- coding: utf-8 -*-
# pages/04_estoque.py — Movimentos & Ajustes de Estoque (com normalização de tipos e refresh)
import json, unicodedata as _ud, re
import streamlit as st
import pandas as pd
import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from google.oauth2.service_account import Credentials
from datetime import date

st.set_page_config(page_title="Estoque — Movimentos & Ajustes", page_icon="📦", layout="wide")
st.title("📦 Estoque — Movimentos & Ajustes")

# ---- Refresh rápido (limpa cache e recarrega)
if st.button("🔄 Atualizar estoques agora"):
    st.cache_data.clear()
    st.experimental_rerun()

# ========= credenciais =========
def _normalize_private_key(key: str) -> str:
    if not isinstance(key, str): return key
    key = key.replace("\\n", "\n")
    key = "".join(ch for ch in key if _ud.category(ch)[0] != "C" or ch in ("\n", "\r", "\t"))
    return key

def _load_sa():
    svc = st.secrets.get("GCP_SERVICE_ACCOUNT")
    if svc is None:
        st.error("🛑 GCP_SERVICE_ACCOUNT ausente."); st.stop()
    if isinstance(svc, str): svc = json.loads(svc)
    svc = dict(svc); svc["private_key"] = _normalize_private_key(svc["private_key"])
    return svc

@st.cache_resource
def _client():
    scopes = ["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(_load_sa(), scopes=scopes)
    return gspread.authorize(creds)

@st.cache_resource
def _sheet():
    gc = _client()
    url_or_id = st.secrets.get("PLANILHA_URL")
    if not url_or_id:
        st.error("🛑 PLANILHA_URL ausente."); st.stop()
    return gc.open_by_url(url_or_id) if str(url_or_id).startswith("http") else gc.open_by_key(url_or_id)

@st.cache_data(ttl=5)
def _load_df(aba: str) -> pd.DataFrame:
    ws = _sheet().worksheet(aba)
    df = get_as_dataframe(ws, evaluate_formulas=True, dtype=str, header=0).dropna(how="all")
    df.columns = [c.strip() for c in df.columns]
    return df.fillna("")

def _ensure_ws(name: str, headers: list[str]):
    sh = _sheet()
    try:
        ws = sh.worksheet(name)
        cur = get_as_dataframe(ws, evaluate_formulas=False, header=0)
        if cur.empty or any(h not in cur.columns for h in headers):
            cols = list(dict.fromkeys(headers + cur.columns.tolist()))
            df_head = pd.DataFrame(columns=cols)
            ws.clear()
            set_with_dataframe(ws, df_head, include_index=False, include_column_header=True, resize=True)
        return ws
    except Exception:
        ws = sh.add_worksheet(title=name, rows=2, cols=max(10, len(headers)))
        df_head = pd.DataFrame(columns=headers)
        set_with_dataframe(ws, df_head, include_index=False, include_column_header=True, resize=True)
        return ws

def _append_row(ws, row: dict):
    cur = get_as_dataframe(ws, evaluate_formulas=False, header=0)
    for col in cur.columns:
        row.setdefault(col, "")
    out = pd.concat([cur, pd.DataFrame([row])], ignore_index=True)
    ws.clear()
    set_with_dataframe(ws, out.fillna(""), include_index=False, include_column_header=True, resize=True)

# ========= utilidades =========
def _to_float_or_zero(x):
    """Converte '1,16', '11.60', '1.234,56' em float seguro."""
    if x is None: return 0.0
    s = str(x).strip()
    if s == "": return 0.0
    s = s.replace("R$", "").replace(" ", "")
    s = s.replace(",", ".")
    s = re.sub(r"[^0-9.]", "", s)
    if s.count(".") > 1:
        parts = s.split(".")
        s = "".join(parts[:-1]) + "." + parts[-1]
    try:
        return float(s)
    except:
        return 0.0

def _nz(x):
    if x is None: return ""
    try:
        if pd.isna(x): return ""
    except Exception:
        pass
    s = str(x).strip()
    return "" if s.lower() in ("nan", "none") else s

def _pick_col(df, cands):
    for c in cands:
        if c in df.columns: return c
    return None

# ========= Abas & headers =========
ABA_PRODUTOS  = "Produtos"
ABA_COMPRAS   = "Compras"
ABA_MOV       = "MovimentosEstoque"

COMPRAS_HEADERS = ["Data","Produto","Unidade","Fornecedor","Qtd","Custo Unitário","Total","IDProduto","Obs"]
MOV_HEADERS     = ["Data","IDProduto","Produto","Tipo","Qtd","Obs","ID","Documento/NF","Origem","SaldoApós"]

# ========= Carregar bases =========
try:
    prod_df = _load_df(ABA_PRODUTOS)
except Exception as e:
    st.error("Erro ao abrir a aba Produtos.")
    with st.expander("Detalhes"):
        st.code(str(e))
    st.stop()

COLP = {
    "id":   _pick_col(prod_df, ["ID","Id","id","Codigo","Código","SKU"]),
    "nome": _pick_col(prod_df, ["Nome","Produto","Descrição","Descricao"]),
    "unid": _pick_col(prod_df, ["Unidade","Unid","Und"]),
}
for k,v in COLP.items():
    if v is None and k in ("id","nome"):
        prod_df[k.upper()] = ""
        COLP[k] = k.upper()

try:
    compras_df = _load_df(ABA_COMPRAS)
except Exception:
    compras_df = pd.DataFrame(columns=COMPRAS_HEADERS)

try:
    mov_df = _load_df(ABA_MOV)
except Exception:
    mov_df = pd.DataFrame(columns=MOV_HEADERS)

# ======== Normalização: Compras ========
for c in COMPRAS_HEADERS:
    if c not in compras_df.columns:
        compras_df[c] = ""
compras_df["Qtd_num"]   = compras_df["Qtd"].apply(_to_float_or_zero)
compras_df["Custo_num"] = compras_df["Custo Unitário"].apply(_to_float_or_zero)

def _key_prod(row):
    pid = _nz(row.get("IDProduto",""))
    nome = _nz(row.get("Produto",""))
    return (pid or "").strip() + "||" + (nome or "").strip()

if not compras_df.empty:
    compras_df["__key"] = compras_df.apply(_key_prod, axis=1)
    last_cost = compras_df.dropna(subset=["__key"]).groupby("__key", as_index=True).tail(1)
    custo_atual_map = {k: v for k, v in zip(last_cost["__key"], last_cost["Custo_num"])}
else:
    custo_atual_map = {}

# ======== Normalização: Movimentos ========
for c in MOV_HEADERS:
    if c not in mov_df.columns:
        mov_df[c] = ""
# --- Normalizador de tipo (resolve 'B entrada', 'B saída', 'Baixa', 'Estorno', etc.)
def _strip_accents_low(s: str) -> str:
    s = _ud.normalize("NFKD", str(s or ""))
    s = "".join(ch for ch in s if _ud.category(ch) != "Mn")
    return s.lower()

def _norm_tipo(t: str) -> str:
    t = _strip_accents_low(t)
    t = re.sub(r"[^a-z]", "", t)  # remove espaços, hífens, etc.: "b entrada" -> "bentrada"
    if "entrada" in t or "compra" in t or "estorno" in t:
        return "entrada"
    if "saida" in t or "venda" in t or "baixa" in t:
        return "saida"
    if "ajuste" in t:
        return "ajuste"
    return "outro"

mov_df["Qtd_num"]   = mov_df["Qtd"].apply(_to_float_or_zero)
mov_df["Tipo_norm"] = mov_df["Tipo"].apply(_norm_tipo)

def _signed_qty(row):
    q = row["Qtd_num"]
    b = row["Tipo_norm"]
    if b == "entrada":
        return abs(q)
    if b == "saida":
        return -abs(q)
    if b == "ajuste":
        return q  # mantém o sinal informado nos ajustes
    return 0.0

mov_df["Qtd_signed"] = mov_df.apply(_signed_qty, axis=1)

# ======== Montagem do estoque ========
def _prod_key_from(prod_id, prod_nome):
    return f"{_nz(prod_id)}||{_nz(prod_nome)}".strip("|")

base = prod_df.copy()
base["__key"] = base.apply(lambda r: _prod_key_from(r.get(COLP["id"], ""), r.get(COLP["nome"], "")), axis=1)
base["Produto"] = base[COLP["nome"]]
base["IDProduto"] = base[COLP["id"]] if COLP["id"] else ""

if not mov_df.empty:
    mov_df["__key"] = mov_df.apply(lambda r: _prod_key_from(r.get("IDProduto",""), r.get("Produto","")), axis=1)
    saldos = mov_df.groupby("__key")["Qtd_signed"].sum().rename("SaldoMov").reset_index()
else:
    saldos = pd.DataFrame(columns=["__key","SaldoMov"])

df_estoque = base[["__key","Produto","IDProduto"]].merge(saldos, on="__key", how="left").fillna({"SaldoMov":0.0})

def _sum_bucket(bucket_name: str):
    if mov_df.empty:
        return pd.DataFrame(columns=["__key","sum"])
    m = mov_df[mov_df["Tipo_norm"]==bucket_name].copy()
    if m.empty:
        return pd.DataFrame(columns=["__key","sum"])
    m["__key"] = m.apply(lambda r: _prod_key_from(r.get("IDProduto",""), r.get("Produto","")), axis=1)
    return m.groupby("__key")["Qtd_num"].sum().reset_index().rename(columns={"Qtd_num":"sum"})

entradas_sum = _sum_bucket("entrada").rename(columns={"sum":"Entradas"})
saidas_sum   = _sum_bucket("saida").rename(columns={"sum":"Saidas"})
ajustes_sum  = _sum_bucket("ajuste").rename(columns={"sum":"Ajustes"})

for part in (entradas_sum, saidas_sum, ajustes_sum):
    df_estoque = df_estoque.merge(part, on="__key", how="left")

df_estoque[["Entradas","Saidas","Ajustes"]] = df_estoque[["Entradas","Saidas","Ajustes"]].fillna(0.0)

df_estoque["EstoqueAtual"] = df_estoque["SaldoMov"].fillna(0.0)
df_estoque["CustoAtual"]   = df_estoque["__key"].apply(lambda k: float(custo_atual_map.get(k, 0.0)))
df_estoque["ValorTotal"]   = df_estoque["EstoqueAtual"].astype(float) * df_estoque["CustoAtual"].astype(float)

# ======== RESUMO ========
c1, c2, c3 = st.columns(3)
with c1:
    st.metric("🧮 Itens com estoque > 0", int((df_estoque["EstoqueAtual"] > 0).sum()))
with c2:
    st.metric("📦 Quantidade total em estoque", f"{df_estoque['EstoqueAtual'].sum():.0f}")
with c3:
    st.metric("💰 Valor total (R$)", f"R$ {df_estoque['ValorTotal'].sum():.2f}")

st.subheader("Tabela de Estoque")
cols_show = ["IDProduto","Produto","Entradas","Saidas","Ajustes","EstoqueAtual","CustoAtual","ValorTotal"]
for c in cols_show:
    if c not in df_estoque.columns:
        df_estoque[c] = 0 if c not in ("IDProduto","Produto") else ""
st.dataframe(df_estoque[cols_show].sort_values("Produto"), use_container_width=True, hide_index=True)

# Debug opcional: ver últimos movimentos já normalizados
with st.expander("🧾 Últimos movimentos (debug)"):
    if mov_df.empty:
        st.caption("Sem movimentos ainda.")
    else:
        dbg_cols = [c for c in ["Data","Produto","IDProduto","Tipo","Tipo_norm","Qtd","Qtd_num","Qtd_signed","Obs"] if c in mov_df.columns]
        st.dataframe(mov_df[dbg_cols].tail(30), use_container_width=True, hide_index=True)

st.divider()

# ======== FORM: Registrar Saída ========
st.subheader("➖ Registrar Saída / Baixa de Estoque")
with st.form("form_saida"):
    usar_lista_s = st.checkbox("Selecionar produto da lista", value=True, key="saida_lista")
    if usar_lista_s:
        if df_estoque.empty:
            st.warning("Sem produtos para saída."); st.stop()

        def _fmt_saida(i):
            r = df_estoque.iloc[i]
            return f"{_nz(r['Produto'])} — Estq: {int(float(r['EstoqueAtual']))}"

        idx = st.selectbox("Produto", options=range(len(df_estoque)), format_func=_fmt_saida)
        row = df_estoque.iloc[idx]
        prod_nome_s = _nz(row["Produto"])
        prod_id_s   = _nz(row["IDProduto"])
    else:
        prod_nome_s = st.text_input("Produto (nome exato)", key="saida_nome")
        prod_id_s   = st.text_input("ID (opcional)", key="saida_id")

    csa, csb = st.columns(2)
    with csa:
        data_s = st.date_input("Data da saída", value=date.today(), key="saida_data")
    with csb:
        qtd_s  = st.text_input("Qtd", placeholder="Ex.: 2", key="saida_qtd")
    obs_s = st.text_input("Observações (opcional)", key="saida_obs")
    salvar_s = st.form_submit_button("Registrar saída")

if salvar_s:
    if not prod_nome_s.strip():
        st.error("Selecione ou digite um produto."); st.stop()
    q = _to_float_or_zero(qtd_s)
    if q <= 0:
        st.error("Informe uma quantidade válida (> 0)."); st.stop()
    ws_mov = _ensure_ws(ABA_MOV, MOV_HEADERS)
    _append_row(ws_mov, {
        "Data": data_s.strftime("%d/%m/%Y"),
        "IDProduto": _nz(prod_id_s),
        "Produto": prod_nome_s,
        "Tipo": "saida",
        "Qtd": (str(int(q)) if float(q).is_integer() else str(q)).replace(".", ","),
        "Obs": _nz(obs_s)
    })
    st.success("Saída registrada com sucesso! ✅")
    st.toast("Saída lançada", icon="➖")
    st.cache_data.clear()

st.divider()

# ======== FORM: Registrar Ajuste ========
st.subheader("🛠️ Registrar Ajuste de Estoque")
with st.form("form_ajuste"):
    usar_lista_a = st.checkbox("Selecionar produto da lista", value=True, key="ajuste_lista")
    if usar_lista_a:
        if df_estoque.empty:
            st.warning("Sem produtos para ajuste."); st.stop()

        def _fmt_aj(i):
            r = df_estoque.iloc[i]
            return f"{_nz(r['Produto'])} — Estq: {int(float(r['EstoqueAtual']))}"

        idxa = st.selectbox("Produto", options=range(len(df_estoque)), format_func=_fmt_aj, key="ajuste_idx")
        rowa = df_estoque.iloc[idxa]
        prod_nome_a = _nz(rowa["Produto"])
        prod_id_a   = _nz(rowa["IDProduto"])
    else:
        prod_nome_a = st.text_input("Produto (nome exato)", key="ajuste_nome")
        prod_id_a   = st.text_input("ID (opcional)", key="ajuste_id")

    ca1, ca2 = st.columns(2)
    with ca1:
        data_a = st.date_input("Data do ajuste", value=date.today(), key="ajuste_data")
    with ca2:
        qtd_a  = st.text_input("Qtd (use negativo para baixar, positivo para repor)", placeholder="Ex.: -1 ou 5", key="ajuste_qtd")

    obs_a = st.text_input("Motivo/Observações", key="ajuste_obs")
    salvar_a = st.form_submit_button("Registrar ajuste")

if salvar_a:
    if not prod_nome_a.strip():
        st.error("Selecione ou digite um produto."); st.stop()
    qa = _to_float_or_zero(qtd_a)  # pode ser negativo
    if qa == 0:
        st.error("Informe uma quantidade diferente de zero."); st.stop()
    ws_mov = _ensure_ws(ABA_MOV, MOV_HEADERS)
    _append_row(ws_mov, {
        "Data": data_a.strftime("%d/%m/%Y"),
        "IDProduto": _nz(prod_id_a),
        "Produto": prod_nome_a,
        "Tipo": "ajuste",
        "Qtd": (str(int(qa)) if float(qa).is_integer() else str(qa)).replace(".", ","),
        "Obs": _nz(obs_a)
    })
    st.success("Ajuste registrado com sucesso! ✅")
    st.toast("Ajuste lançado", icon="🛠️")
    st.cache_data.clear()

st.divider()
st.page_link("pages/03_compras_entradas.py", label="🧾 Registrar Compras / Entradas", icon="🧾")
st.page_link("pages/01_produtos.py", label="📦 Ir ao Catálogo", icon="📦")
