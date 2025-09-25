# pages/01_produtos.py — Catálogo de Produtos (estoque via MovimentosEstoque) — Dark Cards + Persistência
# -*- coding: utf-8 -*-
import json, unicodedata as _ud, re
import streamlit as st
import pandas as pd
import gspread
from gspread_dataframe import get_as_dataframe
from google.oauth2.service_account import Credentials
from streamlit.components.v1 import html as sthtml

st.set_page_config(page_title="Produtos — Ebenezér Variedades", page_icon="📦", layout="wide")
st.title("📦 Produtos — Catálogo & Busca")

# =========================
# Utils
# =========================
def _normalize_private_key(key: str) -> str:
    if not isinstance(key, str): return key
    key = key.replace("\\n", "\n")
    key = "".join(ch for ch in key if _ud.category(ch)[0] != "C" or ch in ("\n","\r","\t"))
    return key

def _load_sa():
    svc = st.secrets.get("GCP_SERVICE_ACCOUNT")
    if svc is None:
        st.error("🛑 GCP_SERVICE_ACCOUNT ausente."); st.stop()
    if isinstance(svc, str): svc = json.loads(svc)
    svc = dict(svc); svc["private_key"] = _normalize_private_key(svc["private_key"])
    return svc

@st.cache_resource
def _sheet():
    scopes = ["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(_load_sa(), scopes=scopes)
    gc = gspread.authorize(creds)
    url_or_id = st.secrets.get("PLANILHA_URL")
    if not url_or_id:
        st.error("🛑 PLANILHA_URL ausente."); st.stop()
    return gc.open_by_url(url_or_id) if str(url_or_id).startswith("http") else gc.open_by_key(url_or_id)

@st.cache_data(ttl=10, show_spinner=False)
def carregar_aba(nome_aba: str) -> pd.DataFrame:
    ws = _sheet().worksheet(nome_aba)
    df = get_as_dataframe(ws, evaluate_formulas=True, dtype=str, header=0).dropna(how="all")
    df.columns = [c.strip() for c in df.columns]
    return df.fillna("")

def _first_col(df: pd.DataFrame, cands: list[str]) -> str | None:
    for c in cands:
        if c in df.columns: return c
    lower = {c.lower(): c for c in df.columns}
    for c in cands:
        if c.lower() in lower: return lower[c.lower()]
    return None

def _to_num(x) -> float:
    if x is None: return 0.0
    s = str(x).strip()
    if s == "" or s.lower() in ("nan","none"): return 0.0
    s = s.replace("−", "-")
    neg_paren = False
    if s.startswith("(") and s.endswith(")"):
        s = s[1:-1]; neg_paren = True
    s = s.replace("R$", "").replace(" ", "")
    if "," in s: s = s.replace(".", "").replace(",", ".")
    s = re.sub(r"(?<!^)-", "", s)
    s = re.sub(r"[^0-9.\-]", "", s)
    if s.count("-") > 1: s = "-" + s.replace("-", "")
    if s.count(".") > 1:
        p = s.split("."); s = "".join(p[:-1]) + "." + p[-1]
    try: v = float(s)
    except: v = 0.0
    if neg_paren: v = -abs(v)
    return v

def _nz(x):
    if x is None: return ""
    try:
        if pd.isna(x): return ""
    except: pass
    s = str(x).strip()
    return "" if s.lower() in ("nan","none") else s

def _strip_accents_low(s: str) -> str:
    s = _ud.normalize("NFKD", str(s or ""))
    s = "".join(ch for ch in s if _ud.category(ch) != "Mn")
    return s.lower().strip()

def _norm_tipo(t: str) -> str:
    raw = str(t or "")
    low = _strip_accents_low(raw)
    if "fracion" in low:
        if "+" in raw: return "entrada"
        if "-" in raw: return "saida"
        return "outro"
    lowc = re.sub(r"[^a-z]", "", low)
    if "entrada" in lowc or "compra" in lowc or "estorno" in lowc: return "entrada"
    if "saida" in lowc or "venda" in lowc or "baixa" in lowc: return "saida"
    if "ajuste" in lowc or "contagem" in lowc or "inventario" in lowc: return "ajuste"
    return "outro"

def _prod_key_from(prod_id, prod_nome):
    pid = _nz(prod_id)
    if pid: return pid
    return f"nm:{_strip_accents_low(_nz(prod_nome))}"

def _fmt_money_br(v: float | int) -> str:
    try: f = float(v)
    except: f = 0.0
    s = f"{abs(f):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return ("-R$ " if f < 0 else "R$ ") + s

def _fmt_num(v) -> str:
    try:
        f = float(v)
        if abs(f - round(f)) < 1e-9: return f"{int(round(f))}"
        return f"{f:.2f}".rstrip("0").rstrip(".")
    except: return str(v)

# -------------------------
# Persistência via Query Params + Session
# -------------------------
def _get_qp(name, default):
    try:
        val = st.query_params.get(name, default)
        return type(default)(val)
    except Exception:
        pass
    try:
        vals = st.experimental_get_query_params().get(name, [default])
        return type(default)(vals[0])
    except Exception:
        return default

def _set_qp(**kwargs):
    try:
        st.query_params.update(**kwargs)
    except Exception:
        st.experimental_set_query_params(**kwargs)

# =========================
# Abas / Carregamento
# =========================
ABA_PRODUTOS = "Produtos"
ABA_MOV      = "MovimentosEstoque"
ABA_COMPRAS  = "Compras"

df_prod = carregar_aba(ABA_PRODUTOS)
try: df_mov  = carregar_aba(ABA_MOV)
except: df_mov  = pd.DataFrame(columns=["Data","IDProduto","Produto","Tipo","Qtd","Obs"])
try: df_comp = carregar_aba(ABA_COMPRAS)
except: df_comp = pd.DataFrame(columns=["IDProduto","Qtd","Custo Unitário","Produto"])

# =========================
# Colunas
# =========================
col_id   = _first_col(df_prod, ["ID","Codigo","SKU","IDProduto","ProdutoID"])
col_nome = _first_col(df_prod, ["Nome","Produto","Descrição","Descricao"])
col_cat  = _first_col(df_prod, ["Categoria"])
col_forn = _first_col(df_prod, ["Fornecedor"])
col_estq_min = _first_col(df_prod, ["EstoqueMin","Estoque Mínimo"])
col_preco = _first_col(df_prod, ["PreçoVenda","PrecoVenda","Preço"])
col_img   = _first_col(df_prod, ["Imagem","Foto","URLImagem","ImagemURL"])
col_custo_prod = _first_col(df_prod, ["CustoAtual"])  # <- pode existir

if not col_nome:
    st.error("Aba **Produtos** precisa ter uma coluna de nome (ex.: Nome/Produto)."); st.stop()

# =========================
# Base / Mov / Compras
# =========================
base = df_prod.copy()
base["__key"] = base.apply(lambda r: _prod_key_from(r.get(col_id,""), r.get(col_nome,"")), axis=1)
base["IDProduto"] = base[col_id] if col_id else ""
base["Produto"]   = base[col_nome]
base["ImagemURL"] = base[col_img].astype(str).fillna("") if col_img and col_img in base.columns else ""

# ---- Movimentos (quantidade)
for c in ["Tipo","Qtd","IDProduto","Produto"]:
    if c not in df_mov.columns: df_mov[c] = ""
if not df_mov.empty:
    df_mov["Tipo_norm"] = df_mov["Tipo"].apply(_norm_tipo)
    df_mov["Qtd_num"]   = df_mov["Qtd"].map(_to_num)
    df_mov["__key"]     = df_mov.apply(lambda r: _prod_key_from(r.get("IDProduto",""), r.get("Produto","")), axis=1)
    def _sum_mov(tipo): 
        m = df_mov[df_mov["Tipo_norm"] == tipo]
        return {} if m.empty else m.groupby("__key")["Qtd_num"].sum().to_dict()
    entradas_mov = _sum_mov("entrada"); saidas_mov = _sum_mov("saida"); ajustes_mov = _sum_mov("ajuste")
else:
    entradas_mov, saidas_mov, ajustes_mov = {}, {}, {}

# ---- Custos (PRIORIDADE: Produtos.CustoAtual  ➜  fallback: última compra)
# 1) Produtos.CustoAtual
custo_produto_map = {}
if col_custo_prod and col_custo_prod in df_prod.columns:
    tmp = df_prod.copy()
    tmp["__key"] = tmp.apply(lambda r: _prod_key_from(r.get(col_id,""), r.get(col_nome,"")), axis=1)
    tmp["CustoAtual_num"] = tmp[col_custo_prod].map(_to_num)
    custo_produto_map = dict(zip(tmp["__key"], tmp["CustoAtual_num"]))

# 2) Última compra
col_comp_id = _first_col(df_comp, ["IDProduto","ProdutoID"])
col_comp_cu = _first_col(df_comp, ["Custo Unitário","Custo"])
if not df_comp.empty and col_comp_id and col_comp_cu:
    df_comp["__key"] = df_comp.apply(lambda r: _prod_key_from(r.get(col_comp_id,""), r.get("Produto","")), axis=1)
    df_comp["Custo_num"] = df_comp[col_comp_cu].map(_to_num)
    last_cost = df_comp.groupby("__key", as_index=False).tail(1)
    custo_compra_map = dict(zip(last_cost["__key"], last_cost["Custo_num"]))
else:
    custo_compra_map = {}

def _custo_atual(key: str) -> float:
    v_prod = float(custo_produto_map.get(key, 0.0))
    if v_prod > 0:
        return v_prod
    return float(custo_compra_map.get(key, 0.0))

# =========================
# Consolidação
# =========================
df = base[["__key","IDProduto","Produto","ImagemURL"]].copy()
df["Entradas"]     = df["__key"].apply(lambda k: float(entradas_mov.get(k,0)))
df["Saidas"]       = df["__key"].apply(lambda k: float(saidas_mov.get(k,0)))
df["Ajustes"]      = df["__key"].apply(lambda k: float(ajustes_mov.get(k,0)))
df["EstoqueAtual"] = df["Entradas"] - df["Saidas"] + df["Ajustes"]
df["CustoAtual"]   = df["__key"].apply(_custo_atual)  # <— prioridade Produtos; fallback Compras
df["ValorTotal"]   = (df["EstoqueAtual"] * df["CustoAtual"]).round(2)
if col_cat:       df[col_cat]       = df_prod[col_cat]
if col_forn:      df[col_forn]      = df_prod[col_forn]
if col_preco:     df[col_preco]     = df_prod[col_preco]
if col_estq_min:  df[col_estq_min]  = df_prod[col_estq_min]

# =========================
# Filtros (com persistência)
# =========================
cols = st.columns([2.5,1.5,1.2,1.2,1.2])
with cols[0]:
    termo = st.text_input("🔎 Buscar", value=st.session_state.get("prod_busca", ""))
    st.session_state["prod_busca"] = termo

with cols[1]:
    if col_cat:
        cat = st.selectbox("Categoria", ["(todas)"] + sorted(pd.Series(df[col_cat].dropna().astype(str).unique()).tolist()))
    else:
        cat = "(todas)"
with cols[2]:
    if col_forn:
        forn = st.selectbox("Fornecedor", ["(todos)"] + sorted(pd.Series(df[col_forn].dropna().astype(str).unique()).tolist()))
    else:
        forn = "(todos)"

default_only_low   = _get_qp("low",   int(st.session_state.get("prod_only_low", 0))) == 1
default_view_cards = _get_qp("cards", int(st.session_state.get("prod_view_cards", 1))) == 1
with cols[3]:
    only_low = st.checkbox("⚠️ Baixo estoque", value=default_only_low, key="prod_only_low")
with cols[4]:
    view_cards = st.toggle("🖼️ Cards", value=default_view_cards, key="prod_view_cards")
_set_qp(low=int(only_low), cards=int(view_cards))

# Sliders persistentes (default 240/250, mas ajustáveis)
st.caption(f"{len(df)} item(ns) no total.")
c1, c2, _ = st.columns([1.2, 1.2, 3])

try:
    qps = dict(st.query_params)
except Exception:
    qps = st.experimental_get_query_params()

if "prod_img_h" not in st.session_state:
    st.session_state["prod_img_h"] = int((qps.get("img_h",[240])[0] if isinstance(qps.get("img_h"), list) else qps.get("img_h", 240)) or 240)
if "prod_min_w" not in st.session_state:
    st.session_state["prod_min_w"] = int((qps.get("minw",[250])[0] if isinstance(qps.get("minw"), list) else qps.get("minw", 250)) or 250)

img_h_default      = _get_qp("img_h", st.session_state.get("prod_img_h", 240))
min_card_w_default = _get_qp("minw",  st.session_state.get("prod_min_w", 250))

with c1:
    img_h = st.slider("📷 Foto (px)", 100, 300, int(img_h_default), 10, key="prod_img_h")
with c2:
    min_card_w = st.slider("🧱 Largura mínima (px)", 180, 340, int(min_card_w_default), 10, key="prod_min_w")

_set_qp(img_h=st.session_state["prod_img_h"], minw=st.session_state["prod_min_w"])

# =========================
# Aplicando filtros
# =========================
mask = pd.Series(True, index=df.index)
if termo:
    t = termo.lower()
    mask &= df.apply(lambda r: t in " ".join([str(x).lower() for x in [
        r.get("IDProduto",""), r.get("Produto",""), r.get(col_cat,""), r.get(col_forn,"")
    ]]), axis=1)
if col_cat and cat != "(todas)":
    mask &= (df[col_cat].astype(str) == cat)
if col_forn and forn != "(todos)":
    mask &= (df[col_forn].astype(str) == forn)
if only_low and col_estq_min:
    mask &= (df["EstoqueAtual"] <= df[col_estq_min].map(_to_num).fillna(0))
dfv = df[mask].copy()

# =========================
# Exibição
# =========================
PLACEHOLDER = "https://res.cloudinary.com/db8ipmete/image/upload/v1752463905/Logo_sal%C3%A3o_kz9y9c.png"

if view_cards:
    st.caption(f"{len(dfv)} item(ns) filtrado(s).")

    css = f"""
    <style>
    body{{margin:0;font-family:sans-serif;color:#fff;background:#000}}
    .grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax({min_card_w}px,1fr));gap:12px}}
    .card{{border:1px solid #333;border-radius:12px;overflow:hidden;background:#111;color:#fff}}
    .img{{width:100%;height:{img_h}px;object-fit:contain;background:#000}}
    .body{{padding:10px}}
    .title{{font-weight:700;font-size:.95rem;margin-bottom:4px;color:#fff;line-height:1.25}}
    .meta{{font-size:.8rem;color:#ccc;margin-bottom:6px}}
    .badge{{display:inline-block;font-size:.75rem;padding:2px 8px;border-radius:6px;margin-right:6px;color:#fff}}
    .badge.low{{background:#ef4444}}
    .badge.ok{{background:#22c55e}}
    .price{{font-size:.9rem;font-weight:700;margin-top:6px}}
    .kpi{{font-size:.75rem;color:#ddd;margin-top:4px}}
    </style>
    """

    cards = [css, "<div class='grid'>"]
    for _, r in dfv.sort_values("Produto", na_position="last").iterrows():
        nome   = _nz(r.get("Produto",""))
        pid    = _nz(r.get("IDProduto",""))
        img    = _nz(r.get("ImagemURL","")) or PLACEHOLDER
        estq   = _to_num(r.get("EstoqueAtual",0))
        estq_min = _to_num(r.get(col_estq_min,0)) if col_estq_min else 0
        badge  = f"<span class='badge {'low' if estq<=estq_min else 'ok'}'>Estoque: { _fmt_num(estq) }</span>"

        preco  = _fmt_money_br(_to_num(r.get(col_preco,0))) if col_preco else ""
        custo  = _fmt_money_br(r.get("CustoAtual",0))
        vtot   = _fmt_money_br(r.get("ValorTotal",0))

        cat_txt  = _nz(r.get(col_cat,"")) if col_cat else ""
        forn_txt = _nz(r.get(col_forn,"")) if col_forn else ""
        meta = " • ".join([x for x in [f"🏷️ {cat_txt}" if cat_txt else "", f"🚚 {forn_txt}" if forn_txt else ""] if x])

        cards.append(f"""
        <div class='card'>
          <img class='img' src='{img}' alt='{nome}'>
          <div class='body'>
            <div class='title'>{nome}</div>
            <div class='meta'>#{pid}{' • ' + meta if meta else ''}</div>
            {badge}
            <div class='price'>{preco}</div>
            <div class='kpi'>Custo: {custo} • Total: {vtot}</div>
          </div>
        </div>
        """)

    cards.append("</div>")
    sthtml("".join(cards), height=900, scrolling=True)

else:
    # tabela
    cols_show = ["IDProduto","Produto","Entradas","Saidas","Ajustes","EstoqueAtual","CustoAtual","ValorTotal"]
    if col_cat and col_cat in dfv.columns: cols_show.insert(2, col_cat)
    if col_forn and col_forn in dfv.columns: cols_show.insert(3, col_forn)
    if col_estq_min and col_estq_min in dfv.columns and col_estq_min not in cols_show: cols_show.append(col_estq_min)
    if col_preco and col_preco in dfv.columns and col_preco not in cols_show: cols_show.insert(2, col_preco)
    if "ImagemURL" in dfv.columns and "ImagemURL" not in cols_show: cols_show.append("ImagemURL")

    df_show = dfv.loc[:, [c for c in cols_show if c in dfv.columns]].copy()
    if "CustoAtual" in df_show: df_show["CustoAtual"] = df_show["CustoAtual"].map(_fmt_money_br)
    if "ValorTotal" in df_show: df_show["ValorTotal"] = df_show["ValorTotal"].map(_fmt_money_br)
    if col_preco and col_preco in df_show: df_show[col_preco] = df_show[col_preco].map(lambda x: _fmt_money_br(_to_num(x)))
    st.dataframe(df_show.sort_values("Produto"), use_container_width=True, hide_index=True)

# =========================
# Rodapé
# =========================
st.caption("""
• **EstoqueAtual** = Entradas − Saídas ± Ajustes (aba **MovimentosEstoque**).
• **CustoAtual** = prioridade para **Produtos.CustoAtual**; se vazio/zero, usa **última compra** (aba **Compras**).
• Para fotos, adicione uma coluna **Imagem**/**Foto**/**URLImagem** na aba **Produtos**.
• Os controles (cards/baixo estoque/foto/largura) ficam salvos no URL e na sessão. 
""")

