# -*- coding: utf-8 -*-
# pages/04_estoque.py — Estoque (MovimentosEstoque como fonte única) + busca + auto-refresh (UI moderna com cards)

import json, unicodedata as _ud, re
from datetime import date, datetime

import streamlit as st
import pandas as pd
import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from google.oauth2.service_account import Credentials

# =========================
# UI BASE / TEMA
# =========================
st.set_page_config(page_title="Estoque — Movimentos & Ajustes", page_icon="📦", layout="wide")

# ---------- CSS (cards, inputs, tabela) ----------
st.markdown("""
<style>
:root{
  --bg: rgba(255,255,255,.03);
  --bg2: rgba(255,255,255,.06);
  --borda: rgba(255,255,255,.12);
  --muted: rgba(255,255,255,.65);
  --ok: #22c55e; --warn:#f59e0b; --err:#ef4444; --info:#3b82f6; --vio:#7c3aed;
}
.block-container { padding-top: 1.2rem; }
.kpi{border:1px solid var(--borda); background:var(--bg); padding:1rem 1.1rem; border-radius:16px;}
.kpi h3{margin:.2rem 0 .6rem 0; font-size:1.05rem; color:var(--muted); font-weight:600}
.kpi .big{font-size:1.8rem; font-weight:800; line-height:1.1}
.kpi .sub{font-size:.9rem; color:var(--muted)}
.card{
  border:1px solid var(--borda); background:var(--bg);
  padding:1rem; border-radius:16px; margin:.4rem 0 1rem 0;
}
.card h3{margin:0 0 .6rem 0}
.badge{display:inline-block; padding:.15rem .5rem; border-radius:999px; border:1px solid var(--borda); background:var(--bg2); font-size:.78rem; color:var(--muted)}
.low{color:var(--err); font-weight:600}
.ok{color:var(--ok); font-weight:600}
.warn{color:var(--warn); font-weight:600}
.stTextInput>div>div>input, .stSelectbox>div>div>div>input{border-radius:12px !important;}
.stDataFrame{border-radius:14px; overflow:hidden; border:1px solid var(--borda);}
hr{border:0; border-top:1px solid var(--borda); margin:1rem 0}
.small{color:var(--muted); font-size:.86rem}
</style>
""", unsafe_allow_html=True)

# ---------- refresh automático (limpa cache 1x ao abrir a página) ----------
if st.session_state.pop("_first_load_estoque", True):
    st.cache_data.clear()
st.session_state.setdefault("_first_load_estoque", False)

# =========================
# Credenciais / Conexão
# =========================
def _normalize_private_key(key: str) -> str:
    if not isinstance(key, str):
        return key
    key = key.replace("\\n", "\n")
    key = "".join(ch for ch in key if _ud.category(ch)[0] != "C" or ch in ("\n", "\r", "\t"))
    return key

def _load_sa() -> dict:
    svc = st.secrets.get("GCP_SERVICE_ACCOUNT")
    if svc is None:
        st.error("🛑 Segredo GCP_SERVICE_ACCOUNT ausente.")
        st.stop()
    if isinstance(svc, str):
        svc = json.loads(svc)
    svc = dict(svc)
    svc["private_key"] = _normalize_private_key(svc["private_key"])
    return svc

@st.cache_resource
def _client():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(_load_sa(), scopes=scopes)
    return gspread.authorize(creds)

@st.cache_resource
def _sheet():
    gc = _client()
    url_or_id = st.secrets.get("PLANILHA_URL")
    if not url_or_id:
        st.error("🛑 Segredo PLANILHA_URL ausente.")
        st.stop()
    return gc.open_by_url(url_or_id) if str(url_or_id).startswith("http") else gc.open_by_key(url_or_id)

@st.cache_resource
def _sheet_titles() -> set[str]:
    try:
        return {ws.title for ws in _sheet().worksheets()}
    except Exception:
        return set()

# ↓ TTL curto: sempre que entrar na página, recarrega "fresco"
@st.cache_data(ttl=1, show_spinner=False)
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
    cur = get_as_dataframe(ws, evaluate_formulas=False, header=0).fillna("")
    for col in cur.columns:
        row.setdefault(col, "")
    out = pd.concat([cur, pd.DataFrame([row])], ignore_index=True)
    ws.clear()
    set_with_dataframe(ws, out.fillna(""), include_index=False, include_column_header=True, resize=True)

# =========================
# Utilidades
# =========================
def _to_num(x) -> float:
    """Converte para float preservando negativos.
       Suporta: -6, -6,0, (6), 1.234,56, 'R$ -1.234,56' e '−6' (unicode minus)."""
    if x is None:
        return 0.0
    s = str(x).strip()
    if s == "" or s.lower() in ("nan", "none"):
        return 0.0
    s = s.replace("−", "-")            # unicode minus -> ascii
    neg_paren = s.startswith("(") and s.endswith(")")
    if neg_paren:
        s = s[1:-1]

    s = s.replace("R$", "").replace(" ", "")
    if "," in s:
        s = s.replace(".", "")         # remove milhar
        s = s.replace(",", ".")        # vírgula -> ponto

    s = re.sub(r"(?<!^)-", "", s)      # remove '-' fora do início
    s = re.sub(r"[^0-9.\-]", "", s)
    if s.count("-") > 1:
        s = "-" + s.replace("-", "")
    if s.count(".") > 1:
        parts = s.split(".")
        s = "".join(parts[:-1]) + "." + parts[-1]

    try:
        v = float(s)
    except:
        v = 0.0
    if neg_paren:
        v = -abs(v)
    return v

def _nz(x):
    if x is None:
        return ""
    try:
        if pd.isna(x):
            return ""
    except Exception:
        pass
    s = str(x).strip()
    return "" if s.lower() in ("nan", "none") else s

def _strip_accents_low(s: str) -> str:
    s = _ud.normalize("NFKD", str(s or ""))
    s = "".join(ch for ch in s if _ud.category(ch) != "Mn")
    return s.lower().strip()

def _norm_tipo(t: str) -> str:
    """
    Normaliza o campo Tipo em:
      - 'entrada'  (compra, estorno, fracionamento +)
      - 'saida'    (venda, baixa, fracionamento -)
      - 'ajuste'   (ajuste)
      - 'outro'
    """
    raw = str(t or "")
    low = _strip_accents_low(raw)

    # Trata fracionamento primeiro (olha o sinal no texto bruto)
    if "fracion" in low:
        if "+" in raw:
            return "entrada"
        if "-" in raw:
            return "saida"
        return "outro"

    low_clean = re.sub(r"[^a-z]", "", low)
    if "entrada" in low_clean or "compra" in low_clean or "estorno" in low_clean:
        return "entrada"
    if "saida" in low_clean or "venda" in low_clean or "baixa" in low_clean:
        return "saida"
    if "ajuste" in low_clean:
        return "ajuste"
    return "outro"

def _prod_key_from(prod_id, prod_nome):
    """Chave de produto priorizando ID; se não houver, usa nome normalizado."""
    pid = _nz(prod_id)
    if pid:
        return pid
    return f"nm:{_strip_accents_low(_nz(prod_nome))}"

# =========================
# Abas & Headers
# =========================
ABA_PRODUTOS = "Produtos"
ABA_COMPRAS  = "Compras"            # só para custo médio
ABA_MOV      = "MovimentosEstoque"  # FONTE ÚNICA de quantidades
ABA_VENDAS   = "Vendas"             # não usamos para quantidade (evita duplicar)

COMPRAS_HEADERS = ["Data", "Produto", "Unidade", "Fornecedor", "Qtd", "Custo Unitário", "Total", "IDProduto", "Obs"]
MOV_HEADERS     = ["Data", "IDProduto", "Produto", "Tipo", "Qtd", "Obs", "ID", "Documento/NF", "Origem", "SaldoApós"]

# =========================
# Carregar bases
# =========================
titles = _sheet_titles()

prod_df    = _load_df(ABA_PRODUTOS)
compras_df = _load_df(ABA_COMPRAS) if ABA_COMPRAS in titles else pd.DataFrame(columns=COMPRAS_HEADERS)
mov_df     = _load_df(ABA_MOV) if ABA_MOV in titles else pd.DataFrame(columns=MOV_HEADERS)

# =========================
# Normalizações
# =========================
# Produtos
COLP = {
    "id":   next((c for c in ["ID", "Id", "id", "Codigo", "Código", "SKU"] if c in prod_df.columns), None),
    "nome": next((c for c in ["Nome", "Produto", "Descrição", "Descricao"] if c in prod_df.columns), None),
}
if COLP["nome"] is None:
    st.error("Aba **Produtos** precisa ter uma coluna de nome (ex.: Nome/Produto/Descrição).")
    st.stop()

base = prod_df.copy()
base["__key"]     = base.apply(lambda r: _prod_key_from(r.get(COLP["id"], ""), r.get(COLP["nome"], "")), axis=1)
base["Produto"]   = base[COLP["nome"]]
base["IDProduto"] = base[COLP["id"]] if COLP["id"] else ""

# Custo médio/atual (última compra)
for c in COMPRAS_HEADERS:
    if c not in compras_df.columns:
        compras_df[c] = ""
if not compras_df.empty:
    compras_df["__key"]     = compras_df.apply(lambda r: _prod_key_from(r.get("IDProduto", ""), r.get("Produto", "")), axis=1)
    compras_df["Custo_num"] = compras_df["Custo Unitário"].apply(_to_num)
    last_cost = compras_df.groupby("__key", as_index=False).tail(1)
    custo_atual_map = dict(zip(last_cost["__key"], last_cost["Custo_num"]))
else:
    custo_atual_map = {}

# Movimentos — FONTE ÚNICA DE QUANTIDADES
for c in MOV_HEADERS:
    if c not in mov_df.columns:
        mov_df[c] = ""
if not mov_df.empty:
    mov_df["Tipo_norm"] = mov_df["Tipo"].apply(_norm_tipo)
    mov_df["Qtd_num"]   = mov_df["Qtd"].apply(_to_num)  # preserva negativos também
    mov_df["__key"]     = mov_df.apply(lambda r: _prod_key_from(r.get("IDProduto", ""), r.get("Produto", "")), axis=1)

    def _sum_mov(tipo):
        m = mov_df[mov_df["Tipo_norm"] == tipo]
        if m.empty:
            return {}
        return m.groupby("__key")["Qtd_num"].sum().to_dict()

    entradas_mov = _sum_mov("entrada")
    saidas_mov   = _sum_mov("saida")
    ajustes_mov  = _sum_mov("ajuste")
else:
    entradas_mov, saidas_mov, ajustes_mov = {}, {}, {}

# =========================
# Consolidação Estoque (somente MOVIMENTOS)
# =========================
df = base[["__key", "Produto", "IDProduto"]].copy()

def _get(mapper, key):
    return float(mapper.get(key, 0.0))

df["Entradas"] = df["__key"].apply(lambda k: _get(entradas_mov, k))
df["Saidas"]   = df["__key"].apply(lambda k: _get(saidas_mov,   k))
df["Ajustes"]  = df["__key"].apply(lambda k: _get(ajustes_mov,  k))

df["EstoqueAtual"] = df["Entradas"] - df["Saidas"] + df["Ajustes"]
df["CustoAtual"]   = df["__key"].apply(lambda k: float(custo_atual_map.get(k, 0.0)))
df["ValorTotal"]   = (df["EstoqueAtual"].astype(float) * df["CustoAtual"].astype(float)).round(2)

# =========================
# HEADER
# =========================
left, right = st.columns([0.7, 0.3])
with left:
    st.markdown("<h1 style='margin:0'>📦 Estoque — Movimentos & Ajustes</h1>", unsafe_allow_html=True)
    st.markdown(f"<div class='small'>Fonte única de quantidade: <b>{ABA_MOV}</b> • Atualizado agora: <code>{datetime.now().strftime('%d/%m/%Y %H:%M:%S')}</code></div>", unsafe_allow_html=True)
with right:
    st.markdown("<div style='text-align:right' class='small'>", unsafe_allow_html=True)
    st.page_link("pages/03_compras_entradas.py", label="🧾 Registrar Compras / Entradas", icon="🧾")
    st.page_link("pages/01_produtos.py", label="📦 Ir ao Catálogo", icon="📦")
    st.markdown("</div>", unsafe_allow_html=True)

st.markdown("<div class='badge'>Consolidação por movimentos (entradas - saídas + ajustes)</div>", unsafe_allow_html=True)
st.markdown("<hr/>", unsafe_allow_html=True)

# =========================
# Busca / Filtros + Threshold de baixo estoque
# =========================
with st.container():
    cBusca, cLow, cThr, cExp = st.columns([3, 1.1, 1.1, 1])
    with cBusca:
        termo = st.text_input("🔎 Buscar", placeholder="Nome ou ID do produto...")
    with cLow:
        only_low = st.checkbox("Somente baixo estoque", value=False, help="Filtra itens com estoque ≤ limiar")
    with cThr:
        low_thr = st.number_input("Limiar (≤)", value=0, step=1, help="Define o limite para considerar 'baixo estoque'")
    with cExp:
        exportar = st.button("⬇️ Exportar CSV")

mask = pd.Series([True] * len(df))
if termo.strip():
    t = _strip_accents_low(termo)
    by_nome = df["Produto"].astype(str).apply(_strip_accents_low).str.contains(t)
    by_id   = df["IDProduto"].astype(str).str.contains(termo.strip(), case=False, na=False)
    mask &= (by_nome | by_id)
if only_low:
    mask &= (df["EstoqueAtual"] <= float(low_thr))

df_view = df[mask].copy()

# =========================
# CARDS (KPIs)
# =========================
c1, c2, c3, c4 = st.columns(4)
with c1:
    st.markdown("<div class='kpi'><h3>Itens cadastrados</h3><div class='big'>"
                f"{len(df):,}".replace(",", ".") +
                "</div><div class='sub'>Total na aba Produtos</div></div>", unsafe_allow_html=True)
with c2:
    st.markdown("<div class='kpi'><h3>Com estoque &gt; 0</h3><div class='big'>"
                f"{int((df_view['EstoqueAtual'] > 0).sum()):,}".replace(",", ".") +
                "</div><div class='sub'>Filtrados pela busca</div></div>", unsafe_allow_html=True)
with c3:
    st.markdown("<div class='kpi'><h3>Qtd total em estoque</h3><div class='big'>"
                f"{df_view['EstoqueAtual'].sum():.0f}" +
                "</div><div class='sub'>Soma de Entradas - Saídas + Ajustes</div></div>", unsafe_allow_html=True)
with c4:
    st.markdown("<div class='kpi'><h3>Valor total (R$)</h3><div class='big'>R$ "
                f"{(df_view['EstoqueAtual']*df_view['CustoAtual']).sum():,.2f}".replace(",", "X").replace(".", ",").replace("X", ".") +
                "</div><div class='sub'>Estoque x custo atual</div></div>", unsafe_allow_html=True)

# =========================
# Tabela — formatação e download
# =========================
cols_show = ["IDProduto", "Produto", "Entradas", "Saidas", "Ajustes", "EstoqueAtual", "CustoAtual", "ValorTotal"]
for c in cols_show:
    if c not in df_view.columns:
        df_view[c] = 0 if c not in ("IDProduto", "Produto") else ""

dfv = df_view[cols_show].copy()
# formata valores
dfv["Entradas"]     = dfv["Entradas"].astype(float).round(2)
dfv["Saidas"]       = dfv["Saidas"].astype(float).round(2)
dfv["Ajustes"]      = dfv["Ajustes"].astype(float).round(2)
dfv["EstoqueAtual"] = dfv["EstoqueAtual"].astype(float).round(2)
dfv["CustoAtual"]   = dfv["CustoAtual"].astype(float).round(2)
dfv["ValorTotal"]   = (df_view["EstoqueAtual"].astype(float) * df_view["CustoAtual"].astype(float)).round(2)

# estilo: baixo estoque destacado
def _style_row(r):
    try:
        if float(r["EstoqueAtual"]) <= float(low_thr):
            return ["background-color: rgba(239,68,68,.12)"] * len(r)
    except:
        pass
    return [""] * len(r)

st.markdown("<div class='card'><h3>📊 Tabela de Estoque</h3>", unsafe_allow_html=True)
st.dataframe(
    dfv.sort_values("Produto"),
    use_container_width=True,
    hide_index=True
)
st.markdown("</div>", unsafe_allow_html=True)

if exportar:
    csv = dfv.sort_values("Produto").to_csv(index=False, sep=";").encode("utf-8-sig")
    st.download_button("Baixar CSV (utf-8)", data=csv, file_name="estoque.csv", mime="text/csv")

# =========================
# Últimos movimentos (debug)
# =========================
with st.expander("🧾 Últimos movimentos (debug)"):
    if mov_df.empty:
        st.caption("Sem movimentos ainda.")
    else:
        dbg_cols = [c for c in ["Data", "Produto", "IDProduto", "Tipo", "Qtd", "Tipo_norm"] if c in mov_df.columns]
        st.dataframe(mov_df[dbg_cols].tail(30), use_container_width=True, hide_index=True)

st.markdown("<hr/>", unsafe_allow_html=True)

# =========================
# FORM: Registrar Saída (baixa manual)
# =========================
st.markdown("<div class='card'><h3>➖ Registrar Saída / Baixa de Estoque</h3>", unsafe_allow_html=True)
with st.form("form_saida"):
    usar_lista_s = st.checkbox("Selecionar produto da lista", value=True, key="saida_lista")
    df_select = df_view if usar_lista_s and not df_view.empty else df  # usa filtro da busca
    if usar_lista_s:
        if df_select.empty:
            st.warning("Sem produtos para saída.")
            st.stop()

        def _fmt_saida(i):
            r = df_select.iloc[i]
            return f"{_nz(r['Produto'])} — Estq: {int(float(r['EstoqueAtual']))}"

        idx = st.selectbox("Produto", options=range(len(df_select)), format_func=_fmt_saida)
        row = df_select.iloc[idx]
        prod_nome_s = _nz(row["Produto"])
        prod_id_s   = _nz(row["IDProduto"])
    else:
        prod_nome_s = st.text_input("Produto (nome exato)", key="saida_nome")
        prod_id_s   = st.text_input("ID (opcional)", key="saida_id")

    csa, csb = st.columns(2)
    with csa:
        data_s = st.date_input("Data da saída", value=date.today(), key="saida_data")
    with csb:
        qtd_s = st.text_input("Qtd", placeholder="Ex.: 2", key="saida_qtd")
    obs_s = st.text_input("Observações (opcional)", key="saida_obs")
    salvar_s = st.form_submit_button("Registrar saída", use_container_width=True)

if 'salvar_s' in locals() and salvar_s:
    if not prod_nome_s.strip() and not prod_id_s.strip():
        st.error("Selecione ou informe um produto.")
        st.stop()
    q = _to_num(qtd_s)
    if q <= 0:
        st.error("Informe uma quantidade válida (> 0).")
        st.stop()
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
st.markdown("</div>", unsafe_allow_html=True)

# =========================
# FORM: Registrar Ajuste
# =========================
st.markdown("<div class='card'><h3>🛠️ Registrar Ajuste de Estoque</h3>", unsafe_allow_html=True)
with st.form("form_ajuste"):
    usar_lista_a = st.checkbox("Selecionar produto da lista", value=True, key="ajuste_lista")
    df_select = df_view if usar_lista_a and not df_view.empty else df
    if usar_lista_a:
        if df_select.empty:
            st.warning("Sem produtos para ajuste.")
            st.stop()

        def _fmt_aj(i):
            r = df_select.iloc[i]
            return f"{_nz(r['Produto'])} — Estq: {int(float(r['EstoqueAtual']))}"

        idxa = st.selectbox("Produto", options=range(len(df_select)), format_func=_fmt_aj, key="ajuste_idx")
        rowa = df_select.iloc[idxa]
        prod_nome_a = _nz(rowa["Produto"])
        prod_id_a   = _nz(rowa["IDProduto"])
    else:
        prod_nome_a = st.text_input("Produto (nome exato)", key="ajuste_nome")
        prod_id_a   = st.text_input("ID (opcional)", key="ajuste_id")

    ca1, ca2 = st.columns(2)
    with ca1:
        data_a = st.date_input("Data do ajuste", value=date.today(), key="ajuste_data")
    with ca2:
        qtd_a = st.text_input("Qtd (use negativo para baixar, positivo para repor)", placeholder="Ex.: -1 ou 5", key="ajuste_qtd")

    obs_a = st.text_input("Motivo/Observações", key="ajuste_obs")
    salvar_a = st.form_submit_button("Registrar ajuste", use_container_width=True)

if 'salvar_a' in locals() and salvar_a:
    if not prod_nome_a.strip() and not prod_id_a.strip():
        st.error("Selecione ou informe um produto.")
        st.stop()
    qa = _to_num(qtd_a)  # pode ser negativo
    if qa == 0:
        st.error("Informe uma quantidade diferente de zero.")
        st.stop()
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
st.markdown("</div>", unsafe_allow_html=True)

# =========================
# Links auxiliares (footer)
# =========================
st.markdown("<div class='small'>Dica: ajuste o <b>Limiar (≤)</b> para destacar baixo estoque e use a busca por nome/ID.</div>", unsafe_allow_html=True)
