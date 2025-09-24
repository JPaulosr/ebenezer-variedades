# pages/01_produtos.py — Catálogo de Produtos (estoque via MovimentosEstoque) — Dark Cards
# -*- coding: utf-8 -*-
import json, unicodedata as _ud, re
import streamlit as st
import pandas as pd
import gspread
from gspread_dataframe import get_as_dataframe
from google.oauth2.service_account import Credentials

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

# =========================
# Abas
# =========================
ABA_PRODUTOS = "Produtos"
ABA_MOV      = "MovimentosEstoque"
ABA_COMPRAS  = "Compras"

# =========================
# Carregamento
# =========================
df_prod = carregar_aba(ABA_PRODUTOS)
try: df_mov  = carregar_aba(ABA_MOV)
except: df_mov  = pd.DataFrame(columns=["Data","IDProduto","Produto","Tipo","Qtd","Obs"])
try: df_comp = carregar_aba(ABA_COMPRAS)
except: df_comp = pd.DataFrame(columns=["IDProduto","Qtd","Custo Unitário"])

# =========================
# Colunas
# =========================
col_id   = _first_col(df_prod, ["ID","Codigo","SKU"])
col_nome = _first_col(df_prod, ["Nome","Produto","Descrição"])
col_cat  = _first_col(df_prod, ["Categoria"])
col_forn = _first_col(df_prod, ["Fornecedor"])
col_estq_min = _first_col(df_prod, ["EstoqueMin","Estoque Mínimo"])
col_preco = _first_col(df_prod, ["PreçoVenda","PrecoVenda","Preço"])
col_img   = _first_col(df_prod, ["Imagem","Foto","URLImagem","ImagemURL"])

# =========================
# Base Produtos
# =========================
base = df_prod.copy()
base["__key"] = base.apply(lambda r: _prod_key_from(r.get(col_id,""), r.get(col_nome,"")), axis=1)
base["IDProduto"] = base[col_id] if col_id else ""
base["Produto"]   = base[col_nome]
base["ImagemURL"] = base[col_img].astype(str).fillna("") if col_img and col_img in base.columns else ""

# =========================
# Movimentos
# =========================
for c in ["Tipo","Qtd","IDProduto","Produto"]:
    if c not in df_mov.columns: df_mov[c] = ""
if not df_mov.empty:
    df_mov["Tipo_norm"] = df_mov["Tipo"].apply(_norm_tipo)
    df_mov["Qtd_num"]   = df_mov["Qtd"].map(_to_num)
    df_mov["__key"]     = df_mov.apply(lambda r: _prod_key_from(r.get("IDProduto",""), r.get("Produto","")), axis=1)
    def _sum_mov(tipo): return df_mov[df_mov["Tipo_norm"] == tipo].groupby("__key")["Qtd_num"].sum().to_dict()
    entradas_mov = _sum_mov("entrada"); saidas_mov = _sum_mov("saida"); ajustes_mov = _sum_mov("ajuste")
else: entradas_mov, saidas_mov, ajustes_mov = {}, {}, {}

# =========================
# Compras → custo atual
# =========================
col_comp_id = _first_col(df_comp, ["IDProduto","ProdutoID"])
col_comp_cu = _first_col(df_comp, ["Custo Unitário","Custo"])
if not df_comp.empty and col_comp_id and col_comp_cu:
    df_comp["__key"] = df_comp.apply(lambda r: _prod_key_from(r.get(col_comp_id,""), r.get("Produto","")), axis=1)
    df_comp["Custo_num"] = df_comp[col_comp_cu].map(_to_num)
    last_cost = df_comp.groupby("__key", as_index=False).tail(1)
    custo_atual_map = dict(zip(last_cost["__key"], last_cost["Custo_num"]))
else: custo_atual_map = {}

# =========================
# Consolidação
# =========================
df = base[["__key","IDProduto","Produto","ImagemURL"]].copy()
df["Entradas"]     = df["__key"].apply(lambda k: float(entradas_mov.get(k,0)))
df["Saidas"]       = df["__key"].apply(lambda k: float(saidas_mov.get(k,0)))
df["Ajustes"]      = df["__key"].apply(lambda k: float(ajustes_mov.get(k,0)))
df["EstoqueAtual"] = df["Entradas"] - df["Saidas"] + df["Ajustes"]
df["CustoAtual"]   = df["__key"].apply(lambda k: float(custo_atual_map.get(k,0)))
df["ValorTotal"]   = (df["EstoqueAtual"] * df["CustoAtual"]).round(2)
if col_cat:  df[col_cat]  = df_prod[col_cat]
if col_forn: df[col_forn] = df_prod[col_forn]
if col_preco: df[col_preco] = df_prod[col_preco]
if col_estq_min: df[col_estq_min] = df_prod[col_estq_min]

# =========================
# Filtros
# =========================
cols = st.columns([2.5,1.5,1.2,1.2,1.2])
with cols[0]: termo = st.text_input("🔎 Buscar").strip()
with cols[1]:
    cat = st.selectbox("Categoria", ["(todas)"]+sorted(df[col_cat].dropna().unique())) if col_cat else "(todas)"
with cols[2]:
    forn = st.selectbox("Fornecedor", ["(todos)"]+sorted(df[col_forn].dropna().unique())) if col_forn else "(todos)"
with cols[3]: only_low = st.checkbox("⚠️ Baixo estoque")
with cols[4]: view_cards = st.toggle("🖼️ Cards", True)

mask = pd.Series(True,index=df.index)
if termo: mask &= df.apply(lambda r: termo.lower() in " ".join(str(x).lower() for x in r), axis=1)
if col_cat and cat!="(todas)": mask &= (df[col_cat]==cat)
if col_forn and forn!="(todos)": mask &= (df[col_forn]==forn)
if only_low and col_estq_min: mask &= (df["EstoqueAtual"] <= df[col_estq_min].map(_to_num).fillna(0))
dfv = df[mask]

# =========================
# Exibição
# =========================
PLACEHOLDER = "https://res.cloudinary.com/db8ipmete/image/upload/v1752463905/Logo_sal%C3%A3o_kz9y9c.png"
if view_cards:
    st.caption(f"{len(dfv)} item(ns).")
    col1,col2,_=st.columns([1.2,1.2,3])
    with col1: img_h=st.slider("📷 Foto (px)",100,300,160,10)
    with col2: min_card_w=st.slider("🧱 Largura mínima (px)",180,340,220,10)

    css=f"""
    <style>
    body{{margin:0;font-family:sans-serif;color:#fff;background:#000}}
    .grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax({min_card_w}px,1fr));gap:12px}}
    .card{{border:1px solid #333;border-radius:12px;overflow:hidden;background:#111;color:#fff}}
    .img{{width:100%;height:{img_h}px;object-fit:contain;background:#000}}
    .body{{padding:10px}}
    .title{{font-weight:600;font-size:.95rem;margin-bottom:4px;color:#fff}}
    .meta{{font-size:.8rem;color:#ccc}}
    .badge{{font-size:.75rem;padding:2px 6px;border-radius:6px;margin-right:4px;color:#fff}}
    .badge.low{{background:#ef4444}}
    .badge.ok{{background:#22c55e}}
    </style>
    """
    html=[css,"<div class='grid'>"]
    for _,r in dfv.iterrows():
        nome=_nz(r.get("Produto",""))
        pid=_nz(r.get("IDProduto",""))
        img=_nz(r.get("ImagemURL","")) or PLACEHOLDER
        estq=_to_num(r.get("EstoqueAtual",0))
        estq_min=_to_num(r.get(col_estq_min,0)) if col_estq_min else 0
        badge=f"<span class='badge {'low' if estq<=estq_min else 'ok'}'>Estoque: {estq}</span>"
        html.append(f"""
        <div class='card'>
          <img class='img' src='{img}' alt='{nome}'>
          <div class='body'>
            <div class='title'>{nome}</div>
            <div class='meta'>#{pid}</div>
            {badge}
          </div>
        </div>
        """)
    html.append("</div>")
    from streamlit.components.v1 import html as sthtml
    sthtml("".join(html),height=800,scrolling=True)
else:
    st.dataframe(dfv,use_container_width=True,hide_index=True)

st.caption("• EstoqueAtual = Entradas − Saídas ± Ajustes\n• CustoAtual = último custo de compra\n• Adicione coluna Imagem/Foto na aba Produtos")
