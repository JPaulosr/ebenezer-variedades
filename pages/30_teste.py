# pages/01_produtos.py — Catálogo de Produtos (cards com foto)
# -*- coding: utf-8 -*-
import json, unicodedata as _ud, re
import streamlit as st
import pandas as pd
import gspread
from gspread_dataframe import get_as_dataframe
from google.oauth2.service_account import Credentials

st.set_page_config(page_title="Produtos — Ebenezér Variedades", page_icon="📦", layout="wide")
st.title("📦 Produtos — Catálogo & Busca")

# Auto-refresh sinalizado por outras páginas
if st.session_state.pop("_force_refresh", False):
    st.cache_data.clear()
    st.rerun()

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
    url_or_id = st.secrets.get("PLANILHA_URL") or st.secrets.get("PLANILHA_ID")
    if not url_or_id:
        st.error("🛑 PLANILHA_URL/PLANILHA_ID ausente."); st.stop()
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
    """Converte string/num para float preservando negativos, vírgula e 'R$'."""
    if x is None: return 0.0
    s = str(x).strip()
    if s == "" or s.lower() in ("nan","none"): return 0.0
    s = s.replace("−", "-")  # unicode minus
    neg_paren = False
    if s.startswith("(") and s.endswith(")"):
        s = s[1:-1]; neg_paren = True
    s = s.replace("R$", "").replace(" ", "")
    if "," in s:
        s = s.replace(".", "").replace(",", ".")
    s = re.sub(r"(?<!^)-", "", s)      # '-' só no início
    s = re.sub(r"[^0-9.\-]", "", s)
    if s.count("-") > 1: s = "-" + s.replace("-", "")
    if s.count(".") > 1:
        p = s.split("."); s = "".join(p[:-1]) + "." + p[-1]
    try: v = float(s)
    except: v = 0.0
    if neg_paren: v = -abs(v)
    return v

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
    if "saida"   in lowc or "venda"  in lowc or "baixa"   in lowc: return "saida"
    if "ajuste"  in lowc or "contagem" in lowc or "inventario" in lowc: return "ajuste"
    return "outro"

def _nz(x):
    if x is None: return ""
    try:
        if pd.isna(x): return ""
    except: pass
    s = str(x).strip()
    return "" if s.lower() in ("nan","none") else s

def _prod_key_from(prod_id, prod_nome):
    pid = _nz(prod_id)
    if pid: return pid
    return f"nm:{_strip_accents_low(_nz(prod_nome))}"

def _fmt_brl(v: float) -> str:
    try: v = float(v)
    except: v = 0.0
    return f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

# =========================
# Abas/nome das planilhas
# =========================
ABA_PRODUTOS = "Produtos"
ABA_MOV      = "MovimentosEstoque"   # fonte única de quantidades
ABA_COMPRAS  = "Compras"             # custo

# =========================
# Carregamento
# =========================
try:
    df_prod = carregar_aba(ABA_PRODUTOS)
except Exception as e:
    st.error("Erro ao abrir a aba Produtos."); st.code(str(e)); st.stop()

try:
    df_mov  = carregar_aba(ABA_MOV)
except Exception:
    df_mov  = pd.DataFrame(columns=["Data","IDProduto","Produto","Tipo","Qtd","Obs"])

try:
    df_comp = carregar_aba(ABA_COMPRAS)
except Exception:
    df_comp = pd.DataFrame(columns=["IDProduto","Qtd","Custo Unitário"])

# =========================
# Colunas importantes
# =========================
col_id   = _first_col(df_prod, ["ID","Id","Codigo","Código","SKU"])
col_nome = _first_col(df_prod, ["Nome","Produto","Descrição","Descricao"])
col_cat  = _first_col(df_prod, ["Categoria"])
col_forn = _first_col(df_prod, ["Fornecedor"])
col_estq_min = _first_col(df_prod, ["EstoqueMin","Estoque Mínimo","EstqMin"])
col_preco = _first_col(df_prod, ["PreçoVenda","PrecoVenda","Preço","Preco"])
col_foto = _first_col(df_prod, ["Foto","FotoURL","Imagem","Image","Link","UrlFoto","URL_Foto"])

if not col_nome:
    st.error("Aba **Produtos** precisa ter uma coluna de nome (ex.: Nome/Produto)."); st.stop()

# =========================
# Base Produtos + chaves
# =========================
base = df_prod.copy()
base["__key"] = base.apply(lambda r: _prod_key_from(r.get(col_id,""), r.get(col_nome,"")), axis=1)
base["Produto"] = base[col_nome]
if col_foto and col_foto in base.columns:
    base["Foto"] = base[col_foto].astype(str)
else:
    base["Foto"] = ""

# =========================
# Movimentos → Entradas/Saídas/Ajustes
# =========================
for c in ["Tipo","Qtd","IDProduto","Produto"]:
    if c not in df_mov.columns: df_mov[c] = ""

if not df_mov.empty:
    df_mov["Tipo_norm"] = df_mov["Tipo"].apply(_norm_tipo)
    df_mov["Qtd_num"]   = df_mov["Qtd"].map(_to_num)
    df_mov["__key"]     = df_mov.apply(lambda r: _prod_key_from(r.get("IDProduto",""), r.get("Produto","")), axis=1)
    def _sum_mov(tipo):
        m = df_mov[df_mov["Tipo_norm"] == tipo]
        if m.empty: return {}
        return m.groupby("__key")["Qtd_num"].sum().to_dict()
    entradas_mov = _sum_mov("entrada")
    saidas_mov   = _sum_mov("saida")
    ajustes_mov  = _sum_mov("ajuste")
else:
    entradas_mov, saidas_mov, ajustes_mov = {}, {}, {}

# =========================
# Compras → custo atual
# =========================
col_comp_id = _first_col(df_comp, ["IDProduto","ProdutoID","ID"])
col_comp_cu = _first_col(df_comp, ["Custo Unitário","CustoUnitário","Custo Unit","Custo"])
if not df_comp.empty and col_comp_id and col_comp_cu:
    df_comp["__key"] = df_comp.apply(lambda r: _prod_key_from(r.get(col_comp_id,""), r.get("Produto","")), axis=1)
    df_comp["Custo_num"] = df_comp[col_comp_cu].map(_to_num)
    last_cost = df_comp.groupby("__key", as_index=False).tail(1)
    custo_atual_map = dict(zip(last_cost["__key"], last_cost["Custo_num"]))
else:
    custo_atual_map = {}

# =========================
# Consolidação (sem mostrar ID)
# =========================
df = base[["__key","Produto","Foto"]].copy()
def _get(m, k): return float(m.get(k, 0.0))
df["Entradas"] = df["__key"].apply(lambda k: _get(entradas_mov, k))
df["Saidas"]   = df["__key"].apply(lambda k: _get(saidas_mov,   k))
df["Ajustes"]  = df["__key"].apply(lambda k: _get(ajustes_mov,  k))
df["EstoqueAtual"] = df["Entradas"] - df["Saidas"] + df["Ajustes"]
df["CustoAtual"]   = df["__key"].apply(lambda k: float(custo_atual_map.get(k, 0.0)))
df["ValorTotal"]   = (df["EstoqueAtual"] * df["CustoAtual"]).round(2)

# acopla categoria/fornecedor/estoque mínimo/preço (se existirem)
extra_cols = {}
if col_cat:      extra_cols[col_cat] = base[col_cat]
if col_forn:     extra_cols[col_forn] = base[col_forn]
if col_estq_min: extra_cols[col_estq_min] = base[col_estq_min]
if col_preco:    extra_cols[col_preco] = base[col_preco]
for k, series in extra_cols.items():
    df[k] = series

# =========================
# Filtros
# =========================
top, mid = st.columns([2.5, 1.5])
with top:
    termo = st.text_input("🔎 Buscar", placeholder="nome, fornecedor, categoria...").strip()
with mid:
    only_low = st.checkbox("⚠️ Somente baixo estoque", value=False,
                           help="Itens com EstoqueAtual ≤ EstoqueMin (se existir a coluna).")

c1, c2, c3 = st.columns(3)
with c1:
    if col_cat and col_cat in df.columns:
        cats = ["(todas)"] + sorted(pd.Series(df[col_cat].dropna().astype(str).unique()).tolist())
        cat = st.selectbox("Categoria", cats, index=0)
    else:
        cat = "(todas)"
with c2:
    if col_forn and col_forn in df.columns:
        forns = ["(todos)"] + sorted(pd.Series(df[col_forn].dropna().astype(str).unique()).tolist())
        forn = st.selectbox("Fornecedor", forns, index=0)
    else:
        forn = "(todos)"
with c3:
    n_cols = st.slider("Colunas (cards)", 2, 5, 3)
img_h = st.slider("Altura da imagem (px)", 140, 360, 220, 10)

mask = pd.Series(True, index=df.index)
if termo:
    t = termo.lower()
    mask &= df.apply(
        lambda r: t in " ".join(
            [str(x).lower() for x in [r.get("Produto",""), r.get(col_cat,""), r.get(col_forn,"")]
        ),
        axis=1
    )
if col_cat and cat != "(todas)" and col_cat in df.columns:
    mask &= (df[col_cat].astype(str) == cat)
if col_forn and forn != "(todos)" and col_forn in df.columns:
    mask &= (df[col_forn].astype(str) == forn)

dfv = df[mask].copy()
if only_low and col_estq_min and col_estq_min in dfv.columns:
    estq_min = dfv[col_estq_min].map(_to_num).fillna(0)
    dfv = dfv[dfv["EstoqueAtual"] <= estq_min]

dfv = dfv.sort_values("Produto").reset_index(drop=True)

# =========================
# Estilos (CSS) para os cards
# =========================
st.markdown(
    f"""
    <style>
    :root {{
        --card-bg: rgba(255,255,255,0.03);
        --card-bd: rgba(255,255,255,0.10);
        --chip-bg: rgba(255,255,255,0.06);
    }}
    .p-card {{
        border-radius: 16px;
        padding: 12px;
        background: var(--card-bg);
        border: 1px solid var(--card-bd);
        transition: border-color .15s ease, transform .15s ease;
    }}
    .p-card:hover {{ border-color: rgba(255,255,255,.25); transform: translateY(-1px); }}
    .p-imgwrap {{
        background:#0f0f0f; border-radius: 12px; overflow:hidden;
        display:flex; align-items:center; justify-content:center;
        height:{img_h}px;
        margin-bottom: 10px;
    }}
    .p-imgwrap img {{ width:100%; height:100%; object-fit:contain; }}
    .p-title {{ margin: 2px 0 6px 0; font-weight: 600; }}
    .p-chips {{ display:flex; gap:6px; flex-wrap:wrap; margin-bottom:6px; }}
    .p-chip {{
        display:inline-block; padding:2px 8px; border-radius:999px; font-size:12px;
        background: var(--chip-bg); border:1px solid var(--card-bd);
    }}
    .p-chip.warn {{ background:#2a130f; color:#ffceb8; border-color:#5b2a1e; }}
    .p-stats {{ display:flex; gap:8px; flex-wrap:wrap; }}
    .p-stat  {{
        flex:1; min-width: 45%; background: rgba(255,255,255,.04);
        border:1px solid var(--card-bd); border-radius:10px;
        padding:6px 8px; font-size:13px;
    }}
    .p-muted {{ opacity:.8; font-size:.9em; }}
    </style>
    """,
    unsafe_allow_html=True
)

# =========================
# Exibição
# =========================
tab_cards, tab_table = st.tabs(["🖼️ Cards", "📋 Tabela"])

with tab_cards:
    if dfv.empty:
        st.info("Nada para mostrar com os filtros atuais.")
    else:
        cols = st.columns(n_cols, gap="small")
        for i, r in dfv.iterrows():
            foto = str(r.get("Foto","")).strip() or "https://via.placeholder.com/600x400?text=Sem+foto"
            prod = str(r.get("Produto","")).strip() or "(sem nome)"
            catv = str(r.get(col_cat,"")).strip() if col_cat in dfv.columns else ""
            fornv= str(r.get(col_forn,"")).strip() if col_forn in dfv.columns else ""
            estq = float(r.get("EstoqueAtual", 0.0) or 0.0)
            custo= float(r.get("CustoAtual",   0.0) or 0.0)
            valtot= float(r.get("ValorTotal",  0.0) or 0.0)
            estqmin = _to_num(r.get(col_estq_min, 0)) if col_estq_min in dfv.columns else None
            preco = _to_num(r.get(col_preco, 0)) if col_preco in dfv.columns else None

            warn = (estqmin is not None) and (estq <= float(estqmin))
            chip_low = f'<span class="p-chip warn">Baixo estoque</span>' if warn else ""

            with cols[i % n_cols]:
                st.markdown(
                    f"""
                    <div class="p-card">
                        <div class="p-imgwrap">
                            <img src="{foto}">
                        </div>
                        <div class="p-title">{prod}</div>
                        <div class="p-chips">
                            {f'<span class="p-chip">{catv}</span>' if catv else ''}
                            {f'<span class="p-chip">{fornv}</span>' if fornv else ''}
                            {chip_low}
                        </div>
                        <div class="p-stats">
                            <div class="p-stat"><b>Estoque</b><br>{estq:g}</div>
                            <div class="p-stat"><b>Custo</b><br>{_fmt_brl(custo)}</div>
                            <div class="p-stat"><b>Total</b><br>{_fmt_brl(valtot)}</div>
                            {f'<div class="p-stat"><b>Preço</b><br>{_fmt_brl(preco)}</div>' if preco is not None and preco>0 else ''}
                        </div>
                        { (f'<div class="p-muted" style="margin-top:6px">Mín.: {estqmin:g}</div>' if estqmin is not None and estqmin!=0 else '') }
                    </div>
                    """,
                    unsafe_allow_html=True
                )

with tab_table:
    df_show = dfv.copy()
    # monta tabela sem ID, com colunas legíveis
    cols_show = ["Produto","Entradas","Saidas","Ajustes","EstoqueAtual","CustoAtual","ValorTotal"]
    if col_cat and col_cat in df_show.columns: cols_show.insert(1, col_cat)
    if col_forn and col_forn in df_show.columns: cols_show.insert(2, col_forn)
    if col_estq_min and col_estq_min in df_show.columns: cols_show.append(col_estq_min)
    if col_preco and col_preco in df_show.columns: cols_show.append(col_preco)
    if "Foto" in df_show.columns: cols_show.append("Foto")  # só para referência visual/links

    df_show = df_show.loc[:, [c for c in cols_show if c in df_show.columns]].copy()
    # formata valores
    for c in ["CustoAtual","ValorTotal"]:
        if c in df_show.columns:
            df_show[c] = df_show[c].map(_fmt_brl)
    st.dataframe(df_show, use_container_width=True, hide_index=True)

st.caption("""
• **EstoqueAtual** = Entradas − Saídas ± Ajustes (origem: **MovimentosEstoque**).
• **CustoAtual** = último custo de compra (aba **Compras**).
• Suba/edite a **foto** pela página de upload/URL; aqui ela só é exibida.
""")
