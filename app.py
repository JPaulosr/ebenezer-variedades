# -*- coding: utf-8 -*-
# Dashboard — Ebenezér Variedades (versão redesenhada)
import json, unicodedata, re
from collections.abc import Mapping
from datetime import datetime, date, timedelta

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import gspread
from gspread_dataframe import get_as_dataframe
from google.oauth2.service_account import Credentials

# =========================
# Setup & Estilo
# =========================

# Restaura o tema escuro (desfaz config.toml anterior que deixou tudo branco)
import pathlib
_cfg_dir = pathlib.Path(".streamlit")
_cfg_dir.mkdir(exist_ok=True)
(_cfg_dir / "config.toml").write_text("""
[theme]
base = "dark"
""")

st.set_page_config(
    page_title="Ebenezér Variedades",
    page_icon="🛍️",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Nunito:wght@400;600;700;800;900&family=DM+Sans:wght@300;400;500&display=swap');

/* Base */
html, body, [class*="css"] {
    font-family: 'DM Sans', sans-serif;
}


/* Header customizado */
.page-header {
    background: linear-gradient(135deg, #1a1a2e 0%, #16213e 60%, #0f3460 100%);
    border-radius: 20px;
    padding: 28px 36px;
    margin-bottom: 28px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    box-shadow: 0 8px 32px rgba(15,52,96,0.18);
}
.page-header h1 {
    font-family: 'Nunito', sans-serif;
    font-weight: 900;
    font-size: 1.9rem;
    color: #ffffff;
    margin: 0;
    letter-spacing: -0.5px;
}
.page-header .subtitle {
    font-size: 0.85rem;
    color: rgba(255,255,255,0.55);
    margin-top: 4px;
    font-weight: 400;
}
.header-badge {
    background: rgba(255,255,255,0.1);
    border: 1px solid rgba(255,255,255,0.2);
    border-radius: 50px;
    padding: 8px 18px;
    color: #fff;
    font-size: 0.82rem;
    font-weight: 600;
    backdrop-filter: blur(10px);
}

/* Período selector */
.periodo-bar {
    background: rgba(255,255,255,0.05);
    border-radius: 14px;
    padding: 14px 20px;
    margin-bottom: 24px;
    display: flex;
    align-items: center;
    gap: 12px;
    border: 1px solid rgba(255,255,255,0.08);
}

/* KPI Cards — tema escuro */
.kpi-card {
    background: rgba(255,255,255,0.06);
    border-radius: 18px;
    padding: 22px 24px;
    box-shadow: 0 2px 16px rgba(0,0,0,0.25);
    border: 1px solid rgba(255,255,255,0.1);
    height: 100%;
    transition: transform 0.15s ease, box-shadow 0.15s ease;
}
.kpi-card:hover {
    transform: translateY(-2px);
    box-shadow: 0 6px 24px rgba(0,0,0,0.35);
    background: rgba(255,255,255,0.09);
}
.kpi-icon {
    font-size: 1.5rem;
    margin-bottom: 8px;
    display: block;
}
.kpi-label {
    font-size: 0.75rem;
    font-weight: 600;
    color: rgba(255,255,255,0.45);
    text-transform: uppercase;
    letter-spacing: 0.6px;
    margin-bottom: 6px;
}
.kpi-value {
    font-family: 'Nunito', sans-serif;
    font-size: 1.65rem;
    font-weight: 800;
    color: #ffffff;
    line-height: 1.1;
}
.kpi-sub {
    font-size: 0.78rem;
    color: rgba(255,255,255,0.4);
    margin-top: 5px;
}
.kpi-positive { color: #4ade80; }
.kpi-negative { color: #f87171; }
.kpi-neutral  { color: #60a5fa; }

/* Alerta estoque — tema escuro */
.alerta-card {
    background: rgba(251,146,60,0.12);
    border: 1.5px solid rgba(251,146,60,0.35);
    border-radius: 14px;
    padding: 16px 20px;
    margin-bottom: 8px;
    display: flex;
    align-items: center;
    gap: 14px;
}
.alerta-nome {
    font-weight: 700;
    color: #fb923c;
    font-size: 0.92rem;
}
.alerta-info {
    font-size: 0.78rem;
    color: rgba(251,146,60,0.7);
}
.alerta-badge {
    background: rgba(251,146,60,0.2);
    color: #fb923c;
    border-radius: 8px;
    padding: 4px 10px;
    font-size: 0.75rem;
    font-weight: 700;
    white-space: nowrap;
    margin-left: auto;
    border: 1px solid rgba(251,146,60,0.3);
}

/* Seção — tema escuro */
.secao-titulo {
    font-family: 'Nunito', sans-serif;
    font-weight: 800;
    font-size: 1.1rem;
    color: rgba(255,255,255,0.9);
    margin: 28px 0 16px 0;
    display: flex;
    align-items: center;
    gap: 8px;
}
.secao-titulo::after {
    content: '';
    flex: 1;
    height: 1px;
    background: linear-gradient(to right, rgba(255,255,255,0.15), transparent);
    margin-left: 10px;
    border-radius: 2px;
}

/* Tabela */
.stDataFrame {
    border-radius: 14px !important;
    overflow: hidden;
    border: 1px solid rgba(255,255,255,0.08) !important;
}

/* Filtros no topo — tema escuro */
.filtro-container {
    background: rgba(255,255,255,0.05);
    border-radius: 14px;
    padding: 16px 20px;
    margin-bottom: 20px;
    border: 1px solid #eee;
    box-shadow: 0 2px 8px rgba(0,0,0,0.04);
}


/* Cards ficam brancos sobre o fundo cinza — contraste elegante */
/* sidebar toggle mantido visível para navegação */

/* Mantém a sidebar de navegação visível — só remove padding extra */
section[data-testid="stSidebar"] > div:first-child {
    padding-top: 1rem;
}

footer { display: none !important; }
#MainMenu { display: none !important; }
/* Mantém header visível para o botão de sidebar funcionar */
header { visibility: visible !important; height: auto !important; }
/* Esconde apenas o deploy button */
[data-testid="stToolbar"] { display: none !important; }
/* Garante que o botão de abrir/fechar sidebar apareça sempre */
[data-testid="collapsedControl"] { display: flex !important; }
[data-testid="stSidebarCollapsedControl"] { display: flex !important; }

/* Botão estilizado */
.stButton > button {
    background: #1a1a2e;
    color: white;
    border: none;
    border-radius: 10px;
    padding: 10px 20px;
    font-weight: 600;
    font-size: 0.85rem;
    transition: all 0.15s;
}
.stButton > button:hover {
    background: #0f3460;
    transform: translateY(-1px);
}

/* ===== RESPONSIVO MOBILE ===== */
@media (max-width: 768px) {
    .page-header {
        flex-direction: column;
        align-items: flex-start;
        gap: 10px;
        padding: 18px 16px;
    }
    .page-header h1 {
        font-size: 1.3rem !important;
    }
    .header-badge {
        padding: 6px 12px;
        font-size: 0.75rem;
    }
    .kpi-value {
        font-size: 1.2rem !important;
    }
    .kpi-card {
        padding: 14px 16px;
    }
    .kpi-label {
        font-size: 0.7rem;
    }
    .alerta-card {
        flex-direction: column;
        align-items: flex-start;
        gap: 8px;
    }
    .alerta-badge {
        margin-left: 0;
    }
    .secao-titulo {
        font-size: 0.95rem;
    }
    .filtro-container {
        padding: 12px 14px;
    }
}

[data-testid="stHeader"] { display: none !important; }
</style>
""", unsafe_allow_html=True)

# Refresh leve
if st.session_state.pop("_force_refresh", False):
    st.cache_data.clear()
    st.rerun()

# =========================
# Auth & Conexão
# =========================
def _normalize_private_key(key: str) -> str:
    if not isinstance(key, str): return key
    key = key.replace("\\n", "\n")
    key = "".join(ch for ch in key if unicodedata.category(ch)[0] != "C" or ch in ("\n","\r","\t"))
    return key

def _load_sa() -> dict:
    svc = st.secrets.get("GCP_SERVICE_ACCOUNT")
    if svc is None:
        st.error("🛑 Segredo GCP_SERVICE_ACCOUNT ausente."); st.stop()
    if isinstance(svc, str): svc = json.loads(svc)
    if not isinstance(svc, Mapping):
        st.error("🛑 GCP_SERVICE_ACCOUNT inválido."); st.stop()
    pk = str(svc.get("private_key",""))
    if "BEGIN PRIVATE KEY" not in pk:
        st.error("🛑 private_key inválida."); st.stop()
    svc = {**svc, "private_key": _normalize_private_key(pk)}
    return svc

@st.cache_resource
def conectar_sheets():
    scopes = ["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
    creds  = Credentials.from_service_account_info(_load_sa(), scopes=scopes)
    gc     = gspread.authorize(creds)
    url_or_id = st.secrets.get("PLANILHA_URL", "")
    if not url_or_id:
        st.error("🛑 PLANILHA_URL não está no Secrets."); st.stop()
    return gc.open_by_url(url_or_id) if url_or_id.startswith("http") else gc.open_by_key(url_or_id)

@st.cache_data(ttl=20, show_spinner=False)
def carregar_aba(nome: str) -> pd.DataFrame:
    ws = conectar_sheets().worksheet(nome)
    df = get_as_dataframe(ws, evaluate_formulas=True, dtype=str, header=0).dropna(how="all")
    df.columns = [c.strip() for c in df.columns]
    return df

# =========================
# Utils
# =========================
def _to_float(x, default=0.0):
    if x is None: return default
    s = str(x).strip()
    if s == "" or s.lower() in ("nan","none"): return default
    s = s.replace("R$","").replace(" ","").replace("\u00A0","")
    s = s.replace(",", ".")
    s = re.sub(r"[^0-9.\-]", "", s)
    if s.count(".") > 1:
        parts = s.split("."); s = "".join(parts[:-1]) + "." + parts[-1]
    try: return float(s)
    except: return default

def _parse_date_any(s):
    if s is None or (isinstance(s, float) and pd.isna(s)): return None
    txt = str(s).strip()
    for fmt in ("%d/%m/%Y","%Y-%m-%d","%d/%m/%y"):
        try: return datetime.strptime(txt, fmt).date()
        except: pass
    try:
        return pd.to_datetime(txt, dayfirst=True, errors="coerce").date()
    except: return None

def _first_col(df: pd.DataFrame, candidates) -> str | None:
    if df is None or df.empty: return None
    cols = list(df.columns)
    for c in candidates:
        if c in cols: return c
    low = {c.lower(): c for c in cols}
    for c in candidates:
        if c.lower() in low: return low[c.lower()]
    return None

def _fmt_brl(v):
    try:
        return ("R$ " + f"{float(v):,.2f}").replace(",", "X").replace(".", ",").replace("X",".")
    except: return "R$ 0,00"

def _canon_id(x):
    return re.sub(r"[^0-9]", "", str(x or ""))

# =========================
# Abas
# =========================
ABA_PROD, ABA_VEND, ABA_COMP = "Produtos", "Vendas", "Compras"

try:    prod = carregar_aba(ABA_PROD)
except: prod = pd.DataFrame()
try:    vend_raw = carregar_aba(ABA_VEND)
except: vend_raw = pd.DataFrame()
try:    comp_raw = carregar_aba(ABA_COMP)
except: comp_raw = pd.DataFrame()

# =========================
# Produtos
# =========================
if not prod.empty:
    ren = {
        "ID":"ID","Nome":"Nome","Categoria":"Categoria","Unidade":"Unidade","Fornecedor":"Fornecedor",
        "CustoAtual":"CustoAtual","PreçoVenda":"PrecoVenda","Preço Venda":"PrecoVenda","PrecoVenda":"PrecoVenda",
        "EstoqueAtual":"EstoqueAtual","EstoqueMin":"EstoqueMin","LeadTimeDias":"LeadTimeDias","Ativo?":"Ativo",
        "FatorCusto":"FatorCusto"
    }
    for k,v in ren.items():
        if k in prod.columns and v!=k: prod.rename(columns={k:v}, inplace=True)
    for c in ["ID","Nome","Categoria","Fornecedor","EstoqueAtual","EstoqueMin","CustoAtual","PrecoVenda","Ativo","FatorCusto"]:
        if c not in prod.columns: prod[c] = None
    if "FatorCusto" not in prod.columns: prod["FatorCusto"] = 1
    for c in ["EstoqueAtual","EstoqueMin","CustoAtual","PrecoVenda","FatorCusto"]:
        prod[c] = pd.to_numeric(prod[c], errors="coerce")
    prod["KeyID"] = prod["ID"].apply(_canon_id)
    prod["ValorEstoque"] = prod["CustoAtual"].fillna(0)*prod["EstoqueAtual"].fillna(0)

# =========================
# BARRA DE NAVEGAÇÃO NATIVA
# =========================
st.markdown("""
<style>
/* Estilo da barra de nav */
div[data-testid="stPageLink"] a {
    background: rgba(255,255,255,0.07) !important;
    border-radius: 10px !important;
    padding: 6px 10px !important;
    font-size: 0.78rem !important;
    font-weight: 600 !important;
    color: rgba(255,255,255,0.75) !important;
    text-decoration: none !important;
    transition: background 0.15s !important;
    white-space: nowrap !important;
}
div[data-testid="stPageLink"] a:hover {
    background: rgba(255,255,255,0.15) !important;
    color: #fff !important;
}
</style>
""", unsafe_allow_html=True)

with st.container():
    nav_cols = st.columns(12)
    pages = [
        ("🏠 Dashboard",    "app.py"),
        ("💰 Fiado",         "pages/000_Fiado_Dashboard.py"),
        ("🛒 Vendas",        "pages/00_Vendas.py"),
        ("🏦 Caixa",         "pages/01_Fechamento_Caixa.py"),
        ("📦 Produtos",      "pages/01_Produtos.py"),
        ("➕ Cadastrar",     "pages/02_Cadastrar_Produto.py"),
        ("🚚 Compras",       "pages/03_Compras_Produtos_Entradas.py"),
        ("📊 Estoque",       "pages/04_Estoque.py"),
        ("🔢 Contagem",      "pages/05_Contagem_Estoque.py"),
        ("✂️ Fracionar",     "pages/05_Fracionar.py"),
        ("🖼️ Fotos",         "pages/07_upload_fotos.py"),
    ]
    for i, (label, path) in enumerate(pages):
        with nav_cols[i]:
            st.page_link(path, label=label)

# =========================
# HEADER
# =========================
hoje = date.today()
st.markdown(f"""
<div class="page-header">
    <div>
        <div class="page-header h1" style="font-family:'Nunito',sans-serif;font-weight:900;font-size:1.9rem;color:#fff;margin:0;">
            🛍️ Ebenezér Variedades
        </div>
        <div class="subtitle">Painel de acompanhamento do negócio</div>
    </div>
    <div class="header-badge">📅 {hoje.strftime('%d/%m/%Y')}</div>
</div>
""", unsafe_allow_html=True)

# =========================
# FILTRO DE PERÍODO (no topo, simples)
# =========================
st.markdown('<div class="filtro-container">', unsafe_allow_html=True)
col_p1, col_p2, col_p3 = st.columns([2, 2, 3])
with col_p1:
    preset = st.selectbox("📅 Período", ["Hoje","Últimos 7 dias","Últimos 30 dias","Mês atual","Personalizado"], index=2, label_visibility="collapsed")
with col_p2:
    cats = sorted(pd.Series(prod["Categoria"].dropna().astype(str).unique()).tolist()) if not prod.empty else []
    cat_sel = st.multiselect("Categoria", cats, placeholder="Todas as categorias")
with col_p3:
    busca = st.text_input("🔍 Buscar produto por nome", placeholder="Digite para filtrar...")
st.markdown('</div>', unsafe_allow_html=True)

if preset == "Hoje":
    dt_ini, dt_fim = hoje, hoje
elif preset == "Últimos 7 dias":
    dt_ini, dt_fim = hoje - timedelta(days=6), hoje
elif preset == "Últimos 30 dias":
    dt_ini, dt_fim = hoje - timedelta(days=29), hoje
elif preset == "Mês atual":
    dt_ini, dt_fim = hoje.replace(day=1), hoje
else:
    cc1, cc2 = st.columns(2)
    with cc1: dt_ini = st.date_input("De:", value=hoje - timedelta(days=29))
    with cc2: dt_fim = st.date_input("Até:", value=hoje)

inclui_estornos = False  # padrão: excluídos (usuária leiga não precisa disso)
apenas_ativos   = True
ocultar_zerados = True

# =========================
# Vendas (período)
# =========================
def _normalize_vendas_period(v: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    if v.empty: return pd.DataFrame(), pd.DataFrame()
    v = v.copy(); v.columns = [c.strip() for c in v.columns]
    col_data  = _first_col(v, ["Data"])
    col_vid   = _first_col(v, ["VendaID","Pedido","Cupom"])
    col_idp   = _first_col(v, ["IDProduto","ID do Produto","ProdutoID","Produto Id","SKU","COD","Código","Codigo","ID"])
    col_qtd   = _first_col(v, ["Qtd","Quantidade","Qtde","Qde","QTD"])
    col_pu    = _first_col(v, ["PrecoUnit","Preço Unitário","PreçoUnitário","Preço","Preco","Preço Unit","Unitário"])
    col_tot   = _first_col(v, ["TotalLinha","Total","Total da Linha"])
    col_forma = _first_col(v, ["FormaPagto","Forma Pagamento","FormaPagamento","Pagamento","Forma"])
    col_obs   = _first_col(v, ["Obs","Observação"])
    col_desc  = _first_col(v, ["Desconto"])
    col_totcup= _first_col(v, ["TotalCupom"])
    col_stat  = _first_col(v, ["CupomStatus","Status"])
    out = pd.DataFrame({
        "Data":      v[col_data]  if col_data else None,
        "VendaID":   v[col_vid]   if col_vid  else "",
        "IDProduto": v[col_idp]   if col_idp  else None,
        "Qtd":       v[col_qtd]   if col_qtd  else 0,
        "PrecoUnit": v[col_pu]    if col_pu   else 0,
        "TotalLinha":v[col_tot]   if col_tot  else 0,
        "Forma":     v[col_forma] if col_forma else "",
        "Obs":       v[col_obs]   if col_obs  else "",
        "Desconto":  v[col_desc]  if col_desc else 0,
        "TotalCupom":v[col_totcup] if col_totcup else 0,
        "Status":    v[col_stat]  if col_stat  else "",
    })
    out["Data_d"]    = out["Data"].apply(_parse_date_any)
    out["QtdNum"]    = out["Qtd"].apply(_to_float)
    out["TotalNum"]  = out["TotalLinha"].apply(_to_float)
    out["KeyID"]     = out["IDProduto"].apply(_canon_id)
    out["DescNum"]   = out["Desconto"].apply(_to_float)
    out["TotalCupomNum"] = out["TotalCupom"].apply(_to_float)

    mask_periodo = (out["Data_d"] >= dt_ini) & (out["Data_d"] <= dt_fim)
    if not inclui_estornos:
        mask_estorno = (
            out["VendaID"].astype(str).str.upper().str.startswith("CN-") |
            out["Obs"].astype(str).str.upper().str.contains("ESTORNO", na=False)
        )
        mask_periodo = mask_periodo & (~mask_estorno)
    out_period = out[mask_periodo].copy()

    # Agrupar por cupom
    if out_period.empty:
        return out_period, pd.DataFrame()
    cupom_grp = out_period.groupby("VendaID", as_index=False).agg(
        Data_d=("Data_d","first"),
        Forma=("Forma","first"),
        ReceitaCupom=("TotalNum","sum"),
        Itens=("QtdNum","sum"),
    )
    cupom_grp["VendaID"] = cupom_grp["VendaID"].astype(str)
    return out_period, cupom_grp

vendas, cupom_grp = _normalize_vendas_period(vend_raw)

# =========================
# Compras período
# =========================
def _normalize_compras_period(c: pd.DataFrame) -> pd.DataFrame:
    if c.empty: return pd.DataFrame(columns=["Data_d","TotalNum"])
    c = c.copy(); c.columns = [x.strip() for x in c.columns]
    col_data = _first_col(c, ["Data"])
    col_tot  = _first_col(c, ["Total","TotalLinha","Total da Linha","Valor Total"])
    out = pd.DataFrame({"Data": c[col_data] if col_data else None, "TotalLinha": c[col_tot] if col_tot else 0})
    out["Data_d"]   = out["Data"].apply(_parse_date_any)
    out["TotalNum"] = out["TotalLinha"].apply(_to_float)
    out = out[(out["Data_d"]>=dt_ini) & (out["Data_d"]<=dt_fim)]
    return out

compras_periodo = _normalize_compras_period(comp_raw)

# =========================
# Compras (todas — para estoque)
# =========================
def _normalize_compras_all_with_date(c: pd.DataFrame) -> pd.DataFrame:
    if c is None or c.empty:
        return pd.DataFrame(columns=["KeyID", "QtdNum", "CustoNum", "Data_d"])
    d = c.copy()
    d.columns = [str(x).strip() for x in d.columns]
    def _norm(s: str) -> str:
        s = unicodedata.normalize("NFKD", str(s))
        s = "".join(ch for ch in s if not unicodedata.combining(ch))
        s = s.lower().strip()
        s = re.sub(r"[\s_]+", "", s)
        return s
    norm2orig = {_norm(col): col for col in d.columns}
    def _pick(*aliases):
        for a in aliases:
            col = norm2orig.get(_norm(a))
            if col: return col
        return None
    col_idp = _pick("IDProduto", "ID do Produto", "ProdutoID", "Produto Id", "SKU", "COD", "Código", "Codigo", "ID")
    col_qtd = _pick("Qtd", "Quantidade", "Qtde", "Qde", "QTD")
    col_cu  = _pick("Custo Unitário", "CustoUnitário", "Custo Unit", "CustoUnit", "Preço de Custo", "PrecoCusto", "Preço Custo", "Custo")
    col_tot = _pick("Total", "Total da Linha", "TotalLinha", "Valor Total")
    col_dat = _pick("Data", "Emissao", "Emissão")
    col_fre = _pick("FreteRateado", "Frete Rateado", "Frete")
    def to_f(x): return _to_float(x, default=0.0)
    out = pd.DataFrame({
        "KeyID":   d[col_idp].apply(_canon_id) if col_idp in d else "",
        "QtdNum":  d[col_qtd].apply(to_f)      if col_qtd in d else 0.0,
        "Data_d":  d[col_dat].apply(_parse_date_any) if col_dat in d else None,
    })
    if col_cu in d:
        out["CustoNum"] = d[col_cu].apply(to_f)
    else:
        out["CustoNum"] = 0.0
    if col_tot in d:
        total_num = d[col_tot].apply(to_f)
        mask_fb = (out["CustoNum"] <= 0) & (out["QtdNum"] > 0)
        out.loc[mask_fb, "CustoNum"] = (total_num[mask_fb] / out.loc[mask_fb, "QtdNum"]).astype(float)
    if col_fre in d:
        frete = d[col_fre].apply(to_f)
        out["CustoNum"] = (out["CustoNum"].fillna(0.0) + frete.fillna(0.0)).astype(float)
    out = out[(out["KeyID"] != "") & (out["QtdNum"] > 0)]
    out.loc[abs(out["CustoNum"]) > 1e6, "CustoNum"] = 0.0
    return out[["KeyID", "QtdNum", "CustoNum", "Data_d"]]

c_all = _normalize_compras_all_with_date(comp_raw)

# ── Estoque calculado via MovimentosEstoque (fonte única de verdade) ──
def _norm_tipo_mov(t: str) -> str:
    import unicodedata as _ud2, re as _re2
    raw = str(t or "")
    low = "".join(ch for ch in _ud2.normalize("NFKD", raw.lower()) if _ud2.category(ch) != "Mn")
    if "fracion" in low:
        return "entrada" if "+" in raw else "saida" if "-" in raw else "outro"
    lowc = _re2.sub(r"[^a-z]","",low)
    if "contagem" in lowc or "inventario" in lowc: return "ajuste"
    if "entrada" in lowc or "compra" in lowc or "estorno" in lowc: return "entrada"
    if "saida"   in lowc or "venda"  in lowc or "baixa"   in lowc: return "saida"
    if "ajuste"  in lowc: return "ajuste"
    return "outro"

try:
    mov_raw = carregar_aba("MovimentosEstoque")
except:
    mov_raw = pd.DataFrame()

entradas_mov = pd.Series(dtype=float)
saidas_mov   = pd.Series(dtype=float)
ajustes_mov  = pd.Series(dtype=float)

if not mov_raw.empty:
    dm = mov_raw.copy(); dm.columns = [c.strip() for c in dm.columns]
    col_mid  = _first_col(dm, ["IDProduto","ProdutoID","ID"])
    col_mqtd = _first_col(dm, ["Qtd","Quantidade"])
    col_mtip = _first_col(dm, ["Tipo","tipo"])
    if col_mid and col_mqtd and col_mtip:
        dm["_key"]  = dm[col_mid].apply(_canon_id)
        dm["_qtd"]  = dm[col_mqtd].apply(_to_float)
        dm["_tipo"] = dm[col_mtip].apply(_norm_tipo_mov)
        dm = dm[dm["_key"] != ""]
        entradas_mov = dm[dm["_tipo"]=="entrada"].groupby("_key")["_qtd"].sum()
        saidas_mov   = dm[dm["_tipo"]=="saida"  ].groupby("_key")["_qtd"].sum()
        ajustes_mov  = dm[dm["_tipo"]=="ajuste" ].groupby("_key")["_qtd"].sum()

# Monta calc com KeyID como coluna (não como index)
_rows = []
_all_keys = set(entradas_mov.index) | set(saidas_mov.index) | set(ajustes_mov.index)
for _k in _all_keys:
    _e = float(entradas_mov.get(_k, 0.0))
    _s = float(saidas_mov.get(_k, 0.0))
    _a = float(ajustes_mov.get(_k, 0.0))
    _rows.append({"KeyID": _k, "Entradas": _e, "Saidas": _s, "Ajustes": _a,
                  "SaldoInicial": 0.0, "EstoqueCalc": _e - _s + _a})
calc = pd.DataFrame(_rows) if _rows else pd.DataFrame(
    columns=["KeyID","Entradas","Saidas","Ajustes","SaldoInicial","EstoqueCalc"])

def _last_cost_per_product(comp_df):
    if comp_df.empty: return pd.Series(dtype=float)
    d = comp_df.copy()
    d["_ord"] = range(len(d))
    d = d[d["CustoNum"].apply(lambda x: _to_float(x) > 0)]
    if d.empty: return pd.Series(dtype=float)
    d = d.sort_values(["KeyID","Data_d","_ord"]).groupby("KeyID").tail(1)
    return d.set_index("KeyID")["CustoNum"]

last_cost = _last_cost_per_product(c_all)
prod_calc = prod.copy() if not prod.empty else pd.DataFrame()
if not prod_calc.empty and "KeyID" in prod_calc.columns:
    prod_calc = prod_calc.merge(calc, how="left", on="KeyID", suffixes=("_orig",""))
for col in ["EstoqueCalc","Entradas","Saidas","Ajustes","SaldoInicial","FatorCusto"]:
    if col not in prod_calc.columns: prod_calc[col] = 0.0
prod_calc["FatorCusto"] = prod_calc["FatorCusto"].fillna(1.0)
fator_map = (prod_calc.set_index("KeyID")["FatorCusto"].fillna(1.0) if "FatorCusto" in prod_calc.columns else pd.Series(dtype=float))

def _choose_cost_final(keyid):
    # Usa CustoAtual da aba Produtos (coluna já carregada em prod_calc)
    # Fallback para último custo de compra se não tiver
    custo_prod = float(prod_calc.loc[prod_calc["KeyID"] == str(keyid), "CustoAtual"].values[0]
                       if str(keyid) in prod_calc["KeyID"].values else 0.0) if not prod_calc.empty else 0.0
    if custo_prod > 0:
        return custo_prod
    base  = float(last_cost.get(str(keyid), 0.0) or 0.0)
    fator = float(fator_map.get(str(keyid), 1.0) or 1.0)
    return base * fator

# Usa CustoAtual já presente em prod_calc (vem da aba Produtos)
prod_calc["CustoAtual"] = pd.to_numeric(prod_calc["CustoAtual"], errors="coerce").fillna(0.0)
# Fallback: preenche zeros com último custo de compra
mask_zero = prod_calc["CustoAtual"] <= 0
prod_calc.loc[mask_zero, "CustoAtual"] = prod_calc.loc[mask_zero, "KeyID"].map(
    lambda k: float(last_cost.get(str(k), 0.0) or 0.0)
)
prod_calc["ValorEstoqueCalc"] = prod_calc["CustoAtual"].fillna(0) * prod_calc["EstoqueCalc"].fillna(0)

# =========================
# KPIs
# =========================
if not vendas.empty:
    faturamento = cupom_grp["ReceitaCupom"].sum()
    num_cupons  = cupom_grp["VendaID"].nunique()
    itens_vendidos = vendas["QtdNum"].sum()
else:
    faturamento = 0.0; num_cupons = 0; itens_vendidos = 0.0

if not prod_calc.empty:
    _custo_ref = prod_calc.set_index("KeyID")["CustoAtual"].astype(float)
else:
    _custo_ref = pd.Series(dtype=float)

if not vendas.empty:
    vv = vendas.copy()
    vv["KeyID"] = vv["KeyID"].astype(str)
    vv = vv[vv["KeyID"] != ""]
    vv["QtdNum"] = vv["QtdNum"].astype(float)
    vv["_CustoUnit"]  = vv["KeyID"].map(lambda k: float(_custo_ref.get(str(k), 0.0) or 0.0))
    vv["_CustoLinha"] = vv["QtdNum"] * vv["_CustoUnit"]
    cogs = float(vv["_CustoLinha"].sum())
else:
    cogs = 0.0

lucro_bruto   = max(0.0, faturamento - cogs)
margem_bruta  = (lucro_bruto / faturamento * 100) if faturamento > 0 else 0.0
ticket_medio  = (faturamento / num_cupons) if num_cupons > 0 else 0.0
compras_total = compras_periodo["TotalNum"].sum() if not compras_periodo.empty else 0.0
caixa_periodo = faturamento - compras_total

# =========================
# CARDS KPI — layout bonito
# =========================
c1, c2, c3 = st.columns(3)

def kpi_html(icon, label, value, sub="", sub_color="kpi-neutral"):
    return f"""
    <div class="kpi-card">
        <span class="kpi-icon">{icon}</span>
        <div class="kpi-label">{label}</div>
        <div class="kpi-value">{value}</div>
        {'<div class="kpi-sub ' + sub_color + '">' + sub + '</div>' if sub else ''}
    </div>
    """

with c1:
    st.markdown(kpi_html("💵", "Faturamento", _fmt_brl(faturamento),
                f"de {dt_ini.strftime('%d/%m')} a {dt_fim.strftime('%d/%m')}"), unsafe_allow_html=True)
with c2:
    st.markdown(kpi_html("🧾", "Vendas realizadas", str(num_cupons),
                f"Ticket médio: {_fmt_brl(ticket_medio)}"), unsafe_allow_html=True)
with c3:
    st.markdown(kpi_html("📦", "Itens vendidos", f"{itens_vendidos:.0f}"), unsafe_allow_html=True)

c4, c5 = st.columns(2)
with c4:
    cor = "kpi-positive" if lucro_bruto > 0 else "kpi-negative"
    st.markdown(kpi_html("📊", "Lucro bruto", _fmt_brl(lucro_bruto),
                f"Margem: {margem_bruta:.1f}%", cor), unsafe_allow_html=True)
with c5:
    cor = "kpi-positive" if caixa_periodo >= 0 else "kpi-negative"
    st.markdown(kpi_html("💰", "Caixa do período", _fmt_brl(caixa_periodo),
                "Vendas − Compras", cor), unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# =========================
# GRÁFICO: Vendas vs Compras
# =========================
st.markdown('<div class="secao-titulo">📆 Vendas e Compras por dia</div>', unsafe_allow_html=True)

def _daily(df_in, date_col, val_col, label):
    if df_in is None or df_in.empty: return pd.DataFrame(columns=["Data","Valor","Tipo"])
    d = df_in.copy()
    d[date_col] = d[date_col].apply(_parse_date_any)
    g = d.groupby(date_col)[val_col].sum().reset_index().rename(columns={date_col:"Data", val_col:"Valor"})
    g["Tipo"] = label
    return g

g_v = _daily(cupom_grp if not vendas.empty else pd.DataFrame(), "Data_d", "ReceitaCupom", "💵 Vendas")
g_c = _daily(compras_periodo, "Data_d", "TotalNum", "🛒 Compras")
serie = pd.concat([g_v, g_c], ignore_index=True)

if not serie.empty:
    fig = px.bar(serie, x="Data", y="Valor", color="Tipo", barmode="group",
                 color_discrete_map={"💵 Vendas": "#60a5fa", "🛒 Compras": "#f87171"},
                 template="plotly_dark")
    fig.update_layout(
        yaxis_title="R$", xaxis_title="",
        legend_title="",
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        font_family="DM Sans",
        font_color="rgba(255,255,255,0.75)",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(l=0, r=0, t=10, b=0),
        height=280,
    )
    fig.update_traces(marker_line_width=0, opacity=0.92)
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("Sem dados no período selecionado.")

# =========================
# ALERTAS DE ESTOQUE
# =========================
if not prod_calc.empty:
    estq_min_col = "EstoqueMin" if "EstoqueMin" in prod_calc.columns else None
    m = pd.Series(True, index=prod_calc.index)
    if apenas_ativos and "Ativo" in prod_calc.columns:
        prod_calc["Ativo"] = prod_calc["Ativo"].astype(str).str.lower()
        m &= (prod_calc["Ativo"]=="sim")
    if cat_sel and "Categoria" in prod_calc.columns:
        m &= prod_calc["Categoria"].astype(str).isin(cat_sel)
    if busca:
        s = busca.lower()
        m &= prod_calc.apply(lambda r: s in " ".join([str(x).lower() for x in r.values]), axis=1)
    dfv = prod_calc[m].copy()
    if ocultar_zerados and "EstoqueCalc" in dfv.columns:
        dfv = dfv[dfv["EstoqueCalc"].fillna(0).astype(float) != 0.0]

    if estq_min_col:
        alert = dfv[(dfv[estq_min_col].fillna(0) > 0) & (dfv["EstoqueCalc"].fillna(0) <= dfv[estq_min_col].fillna(0))].copy()
        if not alert.empty:
            st.markdown(f'<div class="secao-titulo">⚠️ Produtos para repor ({len(alert)})</div>', unsafe_allow_html=True)
            alert["SugestaoCompra"] = (alert[estq_min_col].fillna(0)*2 - alert["EstoqueCalc"].fillna(0)).clip(lower=0).round()
            for _, row in alert.head(10).iterrows():
                nome = str(row.get("Nome","")).strip() or "Sem nome"
                est  = row.get("EstoqueCalc", 0)
                emin = row.get(estq_min_col, 0)
                sug  = row.get("SugestaoCompra", 0)
                cat  = str(row.get("Categoria","")).strip()
                st.markdown(f"""
                <div class="alerta-card">
                    <span style="font-size:1.4rem">📦</span>
                    <div>
                        <div class="alerta-nome">{nome}</div>
                        <div class="alerta-info">{cat} · Estoque: {est:.0f} · Mínimo: {emin:.0f}</div>
                    </div>
                    <div class="alerta-badge">Repor {sug:.0f} un.</div>
                </div>
                """, unsafe_allow_html=True)
            if len(alert) > 10:
                st.caption(f"... e mais {len(alert)-10} produtos. Veja a lista completa abaixo.")

    # =========================
    # ESTOQUE — métricas
    # =========================
    st.markdown('<div class="secao-titulo">📦 Situação do Estoque</div>', unsafe_allow_html=True)
    total_produtos = len(dfv)
    valor_estoque  = float(dfv["ValorEstoqueCalc"].fillna(0).sum()) if "ValorEstoqueCalc" in dfv.columns else 0.0
    abaixo_min     = int((dfv["EstoqueCalc"].fillna(0) <= dfv[estq_min_col].fillna(0)).sum()) if estq_min_col else 0

    e1, e2 = st.columns(2)
    with e1:
        st.markdown(kpi_html("🏷️", "Produtos no estoque", str(total_produtos)), unsafe_allow_html=True)
    with e2:
        st.markdown(kpi_html("💰", "Valor em estoque", _fmt_brl(valor_estoque)), unsafe_allow_html=True)
    e3, _ = st.columns([1, 1])
    with e3:
        cor = "kpi-negative" if abaixo_min > 0 else "kpi-positive"
        sub = "Precisam de reposição" if abaixo_min > 0 else "Tudo dentro do limite"
        st.markdown(kpi_html("⚠️", "Abaixo do mínimo", str(abaixo_min), sub, cor), unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # =========================
    # TOP 10 produtos em estoque
    # =========================
    if "ValorEstoqueCalc" in dfv.columns and dfv["ValorEstoqueCalc"].fillna(0).sum() > 0:
        st.markdown('<div class="secao-titulo">🏆 Top 10 — Produtos de maior valor em estoque</div>', unsafe_allow_html=True)
        top = dfv.sort_values("ValorEstoqueCalc", ascending=False).head(10)
        fig2 = px.bar(
            top, x="ValorEstoqueCalc", y="Nome", orientation="h",
            color="ValorEstoqueCalc",
            color_continuous_scale=["#1e3a5f","#60a5fa"],
            template="plotly_dark",
        )
        fig2.update_layout(
            xaxis_title="R$ em estoque", yaxis_title="",
            coloraxis_showscale=False,
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            font_family="DM Sans",
            font_color="rgba(255,255,255,0.75)",
            margin=dict(l=0, r=0, t=10, b=0),
            height=320,
            yaxis={"autorange": "reversed"},
        )
        fig2.update_traces(marker_line_width=0)
        st.plotly_chart(fig2, use_container_width=True)

    # =========================
    # LISTA DE PRODUTOS
    # =========================
    st.markdown('<div class="secao-titulo">📋 Lista completa de produtos</div>', unsafe_allow_html=True)
    cols_show = [c for c in ["Nome","Categoria","EstoqueCalc","EstoqueMin","CustoAtual","ValorEstoqueCalc"] if c in dfv.columns]
    df_show = dfv[cols_show].rename(columns={
        "EstoqueCalc":"Estoque Atual",
        "ValorEstoqueCalc":"Valor em Estoque",
        "CustoAtual":"Custo Unit.",
    }).copy()
    for col in ["Custo Unit.","Valor em Estoque"]:
        if col in df_show.columns:
            df_show[col] = df_show[col].apply(lambda x: _fmt_brl(x) if pd.notna(x) else "—")
    st.dataframe(df_show, use_container_width=True, hide_index=True, height=380)

# =========================
# FERRAMENTAS (escondido, para admin)
# =========================
with st.expander("⚙️ Ferramentas avançadas", expanded=False):
    st.caption("Área técnica — sincronizar custo de produtos na planilha.")
    import unicodedata as _ud, re as _re

    def _norm2(s):
        s = _ud.normalize("NFKD", str(s))
        s = "".join(ch for ch in s if not _ud.combining(ch))
        return _re.sub(r"[\s_]+","", s.lower().strip())

    def _find_col_idx(header, aliases):
        norm_map = {_norm2(h): i+1 for i, h in enumerate(header)}
        for a in aliases:
            idx = norm_map.get(_norm2(a))
            if idx: return idx
        return None

    if st.button("✍️ Atualizar custo dos produtos na planilha"):
        try:
            from gspread.utils import rowcol_to_a1
            sh = conectar_sheets()
            ws = sh.worksheet(ABA_PROD)
            header = ws.row_values(1)
            id_col_idx    = _find_col_idx(header, ["ID","Codigo","Código","ProdutoID","SKU"])
            custo_col_idx = _find_col_idx(header, ["CustoAtual","Custo Atual","Custo_Atual"])
            if not id_col_idx or not custo_col_idx:
                st.error("Cabeçalho precisa ter colunas 'ID' e 'CustoAtual'."); st.stop()
            ids_sheet = ws.col_values(id_col_idx)[1:]
            start_row = 2; end_row = start_row + len(ids_sheet) - 1
            if end_row < start_row:
                st.warning("Não há linhas de produtos para atualizar."); st.stop()
            cell_range = f"{rowcol_to_a1(start_row, custo_col_idx)}:{rowcol_to_a1(end_row, custo_col_idx)}"
            cells = ws.range(cell_range)
            for i, cell in enumerate(cells):
                raw_id = ids_sheet[i] if i < len(ids_sheet) else ""
                keyid  = _canon_id(raw_id)
                val    = float(_choose_cost_final(keyid)) if keyid else 0.0
                cell.value = val
            ws.update_cells(cells, value_input_option="USER_ENTERED")
            st.success("✅ Custos atualizados com sucesso!")
            st.session_state["_force_refresh"] = True
            st.rerun()
        except Exception as e:
            st.error(f"❌ Falha: {e}")
