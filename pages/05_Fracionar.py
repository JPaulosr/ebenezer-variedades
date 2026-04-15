# -*- coding: utf-8 -*-
# pages/05_Fracionar.py — Fracionar granel em fracionados
# Visual idêntico ao Dashboard e Vendas (Ebenezér Variedades)
from __future__ import annotations

import hashlib, json, re, unicodedata
from datetime import date, datetime

import gspread
import pandas as pd
import streamlit as st
from google.oauth2.service_account import Credentials
from gspread_dataframe import get_as_dataframe, set_with_dataframe

# ──────────────────────────────────────────────
#  CONFIG & TEMA
# ──────────────────────────────────────────────
import pathlib
_cfg = pathlib.Path(".streamlit"); _cfg.mkdir(exist_ok=True)
(_cfg / "config.toml").write_text('[theme]\nbase = "dark"\n')

st.set_page_config(
    page_title="Fracionar",
    page_icon="✂️",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Nunito:wght@400;600;700;800;900&family=DM+Sans:wght@300;400;500&display=swap');
html, body, [class*="css"] { font-family: 'DM Sans', sans-serif; }

/* Header */
.page-header {
    background: linear-gradient(135deg, #1a1a2e 0%, #16213e 60%, #0f3460 100%);
    border-radius: 20px; padding: 24px 32px; margin-bottom: 24px;
    display: flex; align-items: center; justify-content: space-between;
    box-shadow: 0 8px 32px rgba(15,52,96,0.25);
}
.page-header h1 { font-family:'Nunito',sans-serif; font-weight:900; font-size:1.7rem; color:#fff; margin:0; }
.page-header .sub { font-size:0.82rem; color:rgba(255,255,255,0.5); margin-top:4px; }
.header-badge {
    background:rgba(255,255,255,0.1); border:1px solid rgba(255,255,255,0.2);
    border-radius:50px; padding:8px 18px; color:#fff; font-size:0.82rem;
    font-weight:600; backdrop-filter:blur(10px);
}

/* Seção título */
.sec-titulo {
    font-family:'Nunito',sans-serif; font-weight:800; font-size:1.05rem;
    color:rgba(255,255,255,0.9); margin:24px 0 14px 0;
    display:flex; align-items:center; gap:8px;
}
.sec-titulo::after {
    content:''; flex:1; height:1px;
    background:linear-gradient(to right,rgba(255,255,255,0.15),transparent);
    margin-left:8px; border-radius:2px;
}

/* Card produto selecionado */
.prod-card {
    background: rgba(255,255,255,0.05);
    border: 1.5px solid rgba(255,255,255,0.12);
    border-radius: 20px;
    padding: 20px 24px;
    margin-bottom: 20px;
    display: flex;
    align-items: center;
    gap: 20px;
}
.prod-card img {
    width: 90px; height: 90px; border-radius: 14px;
    object-fit: contain; background: rgba(255,255,255,0.06);
    border: 1px solid rgba(255,255,255,0.1); flex-shrink: 0;
}
.prod-card-ph {
    width: 90px; height: 90px; border-radius: 14px;
    background: rgba(255,255,255,0.06); border: 1px solid rgba(255,255,255,0.1);
    display: flex; align-items: center; justify-content: center;
    font-size: 2.5rem; flex-shrink: 0;
}
.prod-card-nome { font-family:'Nunito',sans-serif; font-weight:800; font-size:1.1rem; color:#fff; }
.prod-card-sub   { font-size:0.78rem; color:rgba(255,255,255,0.45); margin-top:4px; }
.prod-card-estq  { font-size:1rem; font-weight:700; color:#60a5fa; margin-top:6px; }

/* KPI mini */
.kpi-mini {
    background: rgba(255,255,255,0.06);
    border: 1px solid rgba(255,255,255,0.1);
    border-radius: 14px;
    padding: 16px 18px;
    text-align: center;
}
.kpi-mini .label { font-size:0.72rem; font-weight:600; color:rgba(255,255,255,0.45); text-transform:uppercase; letter-spacing:0.5px; }
.kpi-mini .valor { font-family:'Nunito',sans-serif; font-size:1.6rem; font-weight:800; color:#fff; margin-top:4px; }
.kpi-mini .sub   { font-size:0.72rem; color:rgba(255,255,255,0.35); margin-top:3px; }
.kpi-ok    { color:#4ade80 !important; }
.kpi-warn  { color:#fbbf24 !important; }
.kpi-err   { color:#f87171 !important; }

/* Card de fracionado (preview) */
.frac-card {
    background: rgba(96,165,250,0.08);
    border: 1.5px solid rgba(96,165,250,0.25);
    border-radius: 16px;
    padding: 16px 20px;
    margin-bottom: 10px;
    display: flex;
    align-items: center;
    gap: 16px;
}
.frac-card img { width:60px; height:60px; border-radius:10px; object-fit:contain;
    background:rgba(255,255,255,0.06); flex-shrink:0; }
.frac-card-ph { width:60px; height:60px; border-radius:10px; background:rgba(255,255,255,0.06);
    display:flex; align-items:center; justify-content:center; font-size:1.8rem; flex-shrink:0; }
.frac-nome  { font-weight:700; font-size:0.9rem; color:#fff; }
.frac-qtd   { font-family:'Nunito',sans-serif; font-size:1.1rem; font-weight:800; color:#60a5fa; }
.frac-litros{ font-size:0.75rem; color:rgba(255,255,255,0.4); margin-top:2px; }

/* Caixa de confirmação */
.confirm-box {
    background: linear-gradient(135deg, rgba(74,222,128,0.1), rgba(34,211,238,0.07));
    border: 1.5px solid rgba(74,222,128,0.3);
    border-radius: 18px;
    padding: 22px 26px;
    margin-top: 10px;
}
.confirm-titulo { font-family:'Nunito',sans-serif; font-size:1.1rem; font-weight:800; color:#4ade80; margin-bottom:12px; }
.confirm-linha  { font-size:0.88rem; color:rgba(255,255,255,0.75); margin-bottom:6px; }
.confirm-destaque { color:#fff; font-weight:700; }

/* Aviso litros insuficientes */
.aviso-erro {
    background: rgba(248,113,113,0.1);
    border: 1.5px solid rgba(248,113,113,0.35);
    border-radius: 14px;
    padding: 16px 20px;
    color: #f87171;
    font-weight: 600;
    margin-top: 10px;
}

/* Botão principal */
.stButton > button {
    background: #0f3460 !important;
    color: white !important;
    border: none !important;
    border-radius: 12px !important;
    padding: 12px 28px !important;
    font-weight: 700 !important;
    font-size: 0.95rem !important;
    transition: all 0.15s !important;
}
.stButton > button:hover {
    background: #1a4a7a !important;
    transform: translateY(-1px) !important;
}

/* Navegação */
div[data-testid="stPageLink"] a {
    background: rgba(255,255,255,0.07) !important;
    border-radius: 10px !important;
    padding: 6px 10px !important;
    font-size: 0.78rem !important;
    font-weight: 600 !important;
    color: rgba(255,255,255,0.75) !important;
    text-decoration: none !important;
    white-space: nowrap !important;
}
div[data-testid="stPageLink"] a:hover {
    background: rgba(255,255,255,0.15) !important;
    color: #fff !important;
}

footer { display: none !important; }
#MainMenu { display: none !important; }
[data-testid="stToolbar"] { display: none !important; }
[data-testid="stHeader"] { display: none !important; }
</style>
""", unsafe_allow_html=True)


# ──────────────────────────────────────────────
#  HELPERS SHEETS
# ──────────────────────────────────────────────
def _normalize_private_key(key: str) -> str:
    if not isinstance(key, str): return key
    key = key.replace("\\n", "\n")
    return "".join(ch for ch in key if unicodedata.category(ch)[0] != "C" or ch in ("\n","\r","\t"))

def _load_sa() -> dict:
    svc = st.secrets.get("GCP_SERVICE_ACCOUNT")
    if svc is None: st.error("🛑 GCP_SERVICE_ACCOUNT ausente."); st.stop()
    if isinstance(svc, str): svc = json.loads(svc)
    return {**svc, "private_key": _normalize_private_key(svc["private_key"])}

@st.cache_resource
def _conectar():
    scopes = ["https://www.googleapis.com/auth/spreadsheets",
              "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(_load_sa(), scopes=scopes)
    gc = gspread.authorize(creds)
    url = st.secrets.get("PLANILHA_URL", "")
    if not url: st.error("🛑 PLANILHA_URL ausente."); st.stop()
    return gc.open_by_url(url) if str(url).startswith("http") else gc.open_by_key(url)

@st.cache_data(ttl=15, show_spinner=False)
def _carregar(aba: str) -> pd.DataFrame:
    ws = _conectar().worksheet(aba)
    df = get_as_dataframe(ws, evaluate_formulas=True, dtype=str, header=0).dropna(how="all")
    df.columns = [c.strip() for c in df.columns]
    return df

def _safe_load(aba: str) -> pd.DataFrame:
    try: return _carregar(aba)
    except Exception: return pd.DataFrame()

def _pick(df: pd.DataFrame, *candidates) -> str | None:
    for c in candidates:
        if c in df.columns: return c
    return None

def _strip_txt(s: str) -> str:
    """Remove acentos e normaliza pra lowercase."""
    import unicodedata as _ud
    s = _ud.normalize("NFKD", str(s or ""))
    s = "".join(c for c in s if not _ud.combining(c))
    return s.lower().strip()

def _to_f(x) -> float:
    """Converte string BR para float preservando sinal negativo — mesma lógica da Contagem."""
    if x is None: return 0.0
    s = str(x).strip()
    if s == "" or s.lower() in ("nan", "none"): return 0.0
    neg = False
    s = s.replace("−", "-").replace("\u2212", "-")
    if s.startswith("(") and s.endswith(")"): s = s[1:-1]; neg = True
    s = s.replace("R$", "").replace(" ", "")
    if s.startswith("-"): neg = True; s = s[1:]
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        s = s.replace(",", ".")
    s = re.sub(r"[^0-9.]", "", s)
    if s.count(".") > 1:
        p = s.split(".")
        s = "".join(p[:-1]) + "." + p[-1]
    try: v = float(s)
    except: return 0.0
    return -v if neg else v

def _norm_tipo(t: str) -> str:
    """Classifica o tipo de movimento — mesma lógica da Contagem Estoque."""
    raw = str(t or "")
    low = _strip_txt(raw)
    if "fracion" in low:
        return "entrada" if "+" in raw else "saida" if "-" in raw else "outro"
    lowc = re.sub(r"[^a-z]", "", low)
    if "contagem" in lowc or "inventario" in lowc: return "ajuste"
    if "entrada" in lowc or "compra" in lowc or "estorno" in lowc: return "entrada"
    if "saida"   in lowc or "venda"  in lowc or "baixa"   in lowc: return "saida"
    if "ajuste"  in lowc: return "ajuste"
    return "outro"

def _fmt_brl(v: float) -> str:
    return ("R$ " + f"{v:,.2f}").replace(",","X").replace(".",",").replace("X",".")

def _ensure_ws(name: str, headers: list):
    sh = _conectar()
    try:
        ws = sh.worksheet(name)
    except Exception:
        ws = sh.add_worksheet(title=name, rows=2, cols=max(10, len(headers)))
        set_with_dataframe(ws, pd.DataFrame(columns=headers),
                           include_index=False, include_column_header=True, resize=True)
        return ws
    cur = get_as_dataframe(ws, evaluate_formulas=True, dtype=str, header=0)
    cur.columns = [c.strip() for c in cur.columns]
    miss = [h for h in headers if h not in cur.columns]
    if miss:
        for h in miss: cur[h] = ""
        ws.clear()
        set_with_dataframe(ws, cur.fillna(""), include_index=False, include_column_header=True, resize=True)
    return ws

def _append_row(ws, row: dict):
    cur = get_as_dataframe(ws, evaluate_formulas=True, dtype=str, header=0)
    if cur is None or cur.empty:
        cur = pd.DataFrame(columns=list(row.keys()))
    for c in cur.columns:
        row.setdefault(c, "")
    out = pd.concat([cur, pd.DataFrame([row])], ignore_index=True)
    ws.clear()
    set_with_dataframe(ws, out.fillna(""), include_index=False, include_column_header=True, resize=True)


# ──────────────────────────────────────────────
#  SALDO DE ESTOQUE — mesma lógica da Contagem Estoque
# ──────────────────────────────────────────────
def _saldo(df_mov: pd.DataFrame, prod_id: str, nome: str) -> float:
    """Calcula saldo exatamente como a página Contagem Estoque: entradas - saidas + ajustes."""
    if df_mov.empty: return 0.0
    c_id  = _pick(df_mov, "IDProduto", "ID")
    c_qtd = _pick(df_mov, "Qtd", "Quantidade", "Qtde")
    c_tip = _pick(df_mov, "Tipo", "Movimento", "Mov")
    if not c_qtd or not c_tip: return 0.0

    # filtra pelo produto
    if c_id and prod_id:
        base = df_mov[df_mov[c_id].astype(str) == str(prod_id)]
    else:
        c_nm = _pick(df_mov, "Produto", "Nome")
        if not c_nm: return 0.0
        base = df_mov[df_mov[c_nm].astype(str).str.strip().str.lower() == nome.strip().lower()]
    if base.empty: return 0.0

    tnorm = base[c_tip].apply(_norm_tipo)
    qtds  = base[c_qtd].apply(_to_f)

    ent = qtds[tnorm == "entrada"].sum()
    sai = qtds[tnorm == "saida"].sum()
    adj = qtds[tnorm == "ajuste"].sum()   # ajustes negativos já vêm com sinal

    return round(float(ent) - float(sai) + float(adj), 3)

# ──────────────────────────────────────────────
#  ANTI DUPLICIDADE
# ──────────────────────────────────────────────
def _refid(data_str, produto, qtd, custo) -> str:
    base = "|".join([data_str, str(produto).lower(), f"{float(qtd):.4f}", f"{float(custo):.4f}"])
    return "FRAC-" + hashlib.sha1(base.encode()).hexdigest()[:12]

def _ja_existe(ws, refid: str) -> bool:
    try:
        cur = get_as_dataframe(ws, evaluate_formulas=True, dtype=str, header=0).dropna(how="all")
        if cur.empty or "RefID" not in cur.columns: return False
        return refid in cur["RefID"].astype(str).tolist()
    except Exception:
        return False


# ──────────────────────────────────────────────
#  CARREGA DADOS
# ──────────────────────────────────────────────
df_prod = _safe_load("Produtos")
df_mov  = _safe_load("MovimentosEstoque")

MOV_HEADERS  = ["Data","IDProduto","Produto","Tipo","Qtd","Obs"]
COMP_HEADERS = ["Data","Produto","Unidade","Fornecedor","Qtd","Custo Unitário","Total","IDProduto","Obs","RefID"]

if df_prod.empty:
    st.error("Aba 'Produtos' não encontrada ou vazia."); st.stop()

c_id    = _pick(df_prod, "ID","Id")
c_nome  = _pick(df_prod, "Nome","Produto","Descrição")
c_unid  = _pick(df_prod, "Unidade","Unid")
c_custo = _pick(df_prod, "CustoAtual","Custo Atual","CustoMedio","Custo")
c_preco = _pick(df_prod, "PreçoVenda","PrecoVenda","Preço","Preco","Valor")
c_foto  = _pick(df_prod, "Foto","FotoURL","Imagem","Cloudinary")
c_ativo = _pick(df_prod, "Ativo?","Ativo","Status")



# ──────────────────────────────────────────────
#  BARRA DE NAVEGAÇÃO
# ──────────────────────────────────────────────
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

# ──────────────────────────────────────────────
#  HEADER
# ──────────────────────────────────────────────
hoje = date.today()
st.markdown(f"""
<div class="page-header">
    <div>
        <h1>✂️ Fracionar produto</h1>
        <div class="sub">Divide o galão grande em garrafinhas menores</div>
    </div>
    <div class="header-badge">📅 {hoje.strftime('%d/%m/%Y')}</div>
</div>
""", unsafe_allow_html=True)


# ──────────────────────────────────────────────
#  PASSO 1 — escolhe o produto a fracionar
# ──────────────────────────────────────────────
st.markdown('<div class="sec-titulo">📦 Passo 1 — Qual produto você vai fracionar?</div>', unsafe_allow_html=True)
st.caption("Escolha o galão grande (de 20 L, 5 L etc.) que você quer dividir em pedaços menores.")

# Filtra produtos com unidade L ou nome sugerindo granel
def _e_granel(row) -> bool:
    un = str(row.get(c_unid or "", "") or "").strip().lower()
    nm = str(row.get(c_nome or "", "") or "").strip().lower()
    ativo = str(row.get(c_ativo or "", "sim") or "sim").strip().lower()
    if ativo not in ("sim","s","1","yes","ativo","true",""): return False
    return un in ("l","L","litro","litros","Litro","Litros") or any(x in nm for x in ["20 l","5 l","granel","20l","5l","litro","20 L","5 L"])

df_granel = df_prod[df_prod.apply(_e_granel, axis=1)].copy()

if df_granel.empty:
    st.warning("Nenhum produto granel (em litros) encontrado. Verifique se os produtos têm unidade 'L' ou nome contendo '20 L'.")
    st.stop()

def _label_granel(row) -> str:
    nm = str(row.get(c_nome, "") or "").strip()
    un = str(row.get(c_unid, "") or "").strip()
    return f"{nm}  [{un}]"

opcoes_granel = df_granel.apply(_label_granel, axis=1).tolist()
sel_idx = st.selectbox(
    "Produto a fracionar",
    options=range(len(df_granel)),
    format_func=lambda i: opcoes_granel[i],
    key="sel_granel"
)
row_g = df_granel.iloc[sel_idx].to_dict()
pid_g    = str(row_g.get(c_id, "") or "")
nome_g   = str(row_g.get(c_nome, "") or "")
unid_g   = str(row_g.get(c_unid, "") or "").strip()
custo_g  = _to_f(row_g.get(c_custo, 0))   # custo por litro
foto_g   = str(row_g.get(c_foto, "") or "").strip()
saldo_g  = _saldo(df_mov, pid_g, nome_g)

# Card do produto selecionado
img_tag = f'<img src="{foto_g}" alt="foto">' if foto_g.startswith("http") else '<div class="prod-card-ph">🧴</div>'
cor_est = "kpi-ok" if saldo_g >= 2 else ("kpi-warn" if saldo_g > 0 else "kpi-err")
st.markdown(f"""
<div class="prod-card">
    {img_tag}
    <div>
        <div class="prod-card-nome">{nome_g}</div>
        <div class="prod-card-sub">Unidade: {unid_g} &nbsp;·&nbsp; Custo atual: {_fmt_brl(custo_g)}/{unid_g}</div>
        <div class="prod-card-estq {cor_est}">📦 Estoque disponível: {saldo_g:.1f} {unid_g}</div>
    </div>
</div>
""", unsafe_allow_html=True)

if saldo_g <= 0:
    st.markdown('<div class="aviso-erro">⚠️ Atenção: estoque zerado ou negativo. Verifique se o produto está correto.</div>', unsafe_allow_html=True)


# ──────────────────────────────────────────────
#  PASSO 2 — escolhe o produto fracionado
# ──────────────────────────────────────────────
st.markdown('<div class="sec-titulo">🧴 Passo 2 — Qual garrafinha vai sair?</div>', unsafe_allow_html=True)
st.caption("Escolha o produto menor que vai ser produzido (ex: Maridão 2L).")

# Filtra os que NÃO são granel (unidade un, ml, etc.)
ids_granel = set(df_granel[c_id].astype(str).tolist()) if c_id else set()

def _e_fracionado(row) -> bool:
    ativo = str(row.get(c_ativo or "", "sim") or "sim").strip().lower()
    if ativo not in ("sim","s","1","yes","ativo","true",""): return False
    rid = str(row.get(c_id, "") or "")
    un  = str(row.get(c_unid, "") or "").strip().lower()
    return rid not in ids_granel and un not in ("l","L","litro","litros","Litro","Litros")

df_frac = df_prod[df_prod.apply(_e_fracionado, axis=1)].copy()

def _label_frac(row) -> str:
    nm = str(row.get(c_nome, "") or "").strip()
    un = str(row.get(c_unid, "") or "").strip()
    return f"{nm}  [{un}]"

opcoes_frac = df_frac.apply(_label_frac, axis=1).tolist()

# ── Botão de cadastro ANTES do selectbox ──
if st.button("➕ Cadastrar novo produto fracionado", use_container_width=True, key="btn_abrir_cadastro"):
    st.session_state["mostrar_cadastro_frac"] = True

if st.session_state.get("mostrar_cadastro_frac"):
    st.markdown("""
    <div style="background:rgba(96,165,250,0.08);border:1.5px solid rgba(96,165,250,0.3);
    border-radius:16px;padding:20px 24px;margin:12px 0;">
    <div style="font-family:Nunito;font-weight:800;font-size:1rem;color:#60a5fa;margin-bottom:12px;">
    ➕ Novo produto fracionado</div>
    """, unsafe_allow_html=True)

    cn1, cn2 = st.columns(2)
    with cn1:
        novo_nome  = st.text_input("Nome do produto *", placeholder="Ex: Maridão 2L", key="novo_frac_nome")
        novo_unid  = st.text_input("Unidade *", value="un", placeholder="un / ml / g", key="novo_frac_unid")
    with cn2:
        novo_preco = st.number_input("Preço de venda (R$)", min_value=0.0, value=0.0, step=0.5, format="%.2f", key="novo_frac_preco")
        novo_cat   = st.text_input("Categoria", placeholder="Ex: Higiene", key="novo_frac_cat")

    st.markdown("</div>", unsafe_allow_html=True)

    btn_cadastrar = st.button("💾 Cadastrar e usar este produto", type="primary", use_container_width=True, key="btn_cadastrar_frac")

    if btn_cadastrar:
        if not novo_nome.strip():
            st.error("❌ Informe o nome do produto.")
            st.stop()

        try:
            sh      = _conectar()
            ws_prod = sh.worksheet("Produtos")
            df_cur  = get_as_dataframe(ws_prod, evaluate_formulas=True, dtype=str, header=0).dropna(how="all")
            df_cur.columns = [c.strip() for c in df_cur.columns]

            # Gera novo ID
            col_id_ws = _pick(df_cur, "ID", "Id")
            if col_id_ws:
                ids_existentes = df_cur[col_id_ws].astype(str).str.strip().tolist()
                nums = [int(re.sub(r"\D","",x)) for x in ids_existentes if re.sub(r"\D","",x).isdigit()]
                novo_id = str(max(nums) + 1) if nums else "1"
            else:
                novo_id = str(len(df_cur) + 1)

            nova_linha = {c: "" for c in df_cur.columns}
            for col_cand, val in [
                (["ID","Id"],                                   novo_id),
                (["Nome","Produto","Descrição"],                 novo_nome.strip()),
                (["Unidade","Unid"],                            novo_unid.strip() or "un"),
                (["PreçoVenda","PrecoVenda","Preço","Preco"],   f"{novo_preco:.2f}".replace(".",",")),
                (["Categoria","categoria"],                      novo_cat.strip()),
                (["Ativo?","Ativo","Status"],                   "sim"),
            ]:
                for cand in col_cand:
                    if cand in nova_linha:
                        nova_linha[cand] = val; break

            df_novo = pd.concat([df_cur, pd.DataFrame([nova_linha])], ignore_index=True)
            ws_prod.clear()
            set_with_dataframe(ws_prod, df_novo.fillna(""), include_index=False,
                               include_column_header=True, resize=True)
            st.cache_data.clear()
            _carregar.clear()
            st.session_state["mostrar_cadastro_frac"] = False
            st.session_state["prod_recém_cadastrado"] = novo_nome.strip()
            st.success(f"✅ Produto **{novo_nome.strip()}** cadastrado!")
            import time; time.sleep(1)
            st.rerun()
        except Exception as e:
            st.error(f"Erro ao cadastrar: {e}")
        st.stop()

    st.info("👆 Preencha os dados e clique em **Cadastrar** para continuar.")
    st.stop()

# Se acabou de cadastrar, pré-seleciona o novo produto
_recem = st.session_state.get("prod_recém_cadastrado", "")
_idx_default = 0
if _recem:
    for _i, _op in enumerate(opcoes_frac):
        if _recem.lower() in _op.lower():
            _idx_default = _i + 1  # +1 por causa do (selecione)
            break
    st.session_state.pop("prod_recém_cadastrado", None)

frac_sel = st.selectbox(
    "Produto fracionado que vai ser produzido",
    options=["(selecione)"] + opcoes_frac,
    index=_idx_default,
    key="sel_frac"
)

if frac_sel == "(selecione)":
    st.info("👆 Selecione o produto fracionado para continuar.")
    st.stop()

pos_frac = opcoes_frac.index(frac_sel)
row_f    = df_frac.iloc[pos_frac].to_dict()
pid_f    = str(row_f.get(c_id, "") or "")
nome_f   = str(row_f.get(c_nome, "") or "")
unid_f   = str(row_f.get(c_unid, "") or "").strip()
foto_f   = str(row_f.get(c_foto, "") or "").strip()
preco_f  = _to_f(row_f.get(c_preco, 0))


# ──────────────────────────────────────────────
#  PASSO 3 — quantas unidades e volume por unidade
# ──────────────────────────────────────────────
st.markdown('<div class="sec-titulo">🔢 Passo 3 — Quantidade e volume</div>', unsafe_allow_html=True)
st.caption("Informe quantas garrafinhas vão ser produzidas e quantos litros cada uma leva.")

col1, col2, col3, col4 = st.columns([1, 1, 1, 1])

with col1:
    qtd_prod = st.number_input(
        "Quantas garrafinhas vão sair?",
        min_value=1, max_value=9999, value=10, step=1,
        key="qtd_prod",
        help="Ex: 10 garrafinhas de 2L"
    )

with col2:
    vol_unit = st.number_input(
        "Litros por garrafinha?",
        min_value=0.1, max_value=100.0, value=2.0, step=0.5,
        format="%.1f",
        key="vol_unit",
        help="Ex: 2.0 para uma garrafa de 2 litros"
    )

with col3:
    vol_galao = st.number_input(
        "Litros totais do galão?",
        min_value=0.1, max_value=1000.0, value=20.0, step=1.0,
        format="%.1f",
        key="vol_galao",
        help="Ex: 20.0 para um galão de 20 litros"
    )

with col4:
    data_op = st.date_input(
        "Data da operação",
        value=date.today(),
        key="data_op"
    )

litros_usados = round(float(qtd_prod) * float(vol_unit), 3)


# ──────────────────────────────────────────────
#  RESUMO VISUAL — antes de confirmar
# ──────────────────────────────────────────────
st.markdown('<div class="sec-titulo">📋 Resumo da operação</div>', unsafe_allow_html=True)

litros_restantes = round(saldo_g - litros_usados, 3)
# custo_g = custo total do galão (ex: R$12 para galão de 20L)
# custo por litro = custo_g / vol_galao
# custo por garrafa = custo_por_litro * vol_unit
_vol_galao_safe = float(vol_galao) if float(vol_galao) > 0 else 1.0
_custo_por_litro = custo_g / _vol_galao_safe
custo_unit_f = round(_custo_por_litro * float(vol_unit), 4)  # custo de cada fracionado

k1, k2, k3 = st.columns(3)
with k1:
    st.markdown(f"""
    <div class="kpi-mini">
        <div class="label">Litros disponíveis</div>
        <div class="valor">{saldo_g:.1f} L</div>
        <div class="sub">{nome_g}</div>
    </div>""", unsafe_allow_html=True)

with k2:
    cor = "kpi-err" if litros_usados > saldo_g else "kpi-warn"
    st.markdown(f"""
    <div class="kpi-mini">
        <div class="label">Litros que serão usados</div>
        <div class="valor {cor}">{litros_usados:.1f} L</div>
        <div class="sub">{qtd_prod} × {vol_unit:.1f} L</div>
    </div>""", unsafe_allow_html=True)

with k3:
    cor3 = "kpi-ok" if litros_restantes >= 0 else "kpi-err"
    st.markdown(f"""
    <div class="kpi-mini">
        <div class="label">Vai sobrar</div>
        <div class="valor {cor3}">{litros_restantes:.1f} L</div>
        <div class="sub">no galão</div>
    </div>""", unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# Card de preview do fracionado
img_f = f'<img src="{foto_f}" alt="foto">' if foto_f.startswith("http") else '<div class="frac-card-ph">🧴</div>'
st.markdown(f"""
<div class="frac-card">
    {img_f}
    <div>
        <div class="frac-nome">{nome_f} [{unid_f}]</div>
        <div class="frac-qtd">{qtd_prod} unidades</div>
        <div class="frac-litros">Cada uma usa {vol_unit:.1f} L — custo estimado: {_fmt_brl(custo_unit_f)} / un.</div>
    </div>
</div>
""", unsafe_allow_html=True)


# ──────────────────────────────────────────────
#  ERRO: litros insuficientes
# ──────────────────────────────────────────────
if litros_usados > saldo_g:
    st.markdown(f"""
    <div class="aviso-erro">
        ❌ Você quer usar <strong>{litros_usados:.1f} L</strong> mas só tem <strong>{saldo_g:.1f} L</strong> no galão.
        Reduza a quantidade de garrafinhas ou o volume por unidade.
    </div>
    """, unsafe_allow_html=True)
    st.stop()


# ──────────────────────────────────────────────
#  PASSO 4 — Confirmar
# ──────────────────────────────────────────────
st.markdown('<div class="sec-titulo">✅ Passo 4 — Confirmar e lançar</div>', unsafe_allow_html=True)

st.markdown(f"""
<div class="confirm-box">
    <div class="confirm-titulo">✅ Tudo certo! Confira antes de confirmar:</div>
    <div class="confirm-linha">📤 <span class="confirm-destaque">Saída:</span> {litros_usados:.1f} L de <em>{nome_g}</em></div>
    <div class="confirm-linha">📥 <span class="confirm-destaque">Entrada:</span> {qtd_prod} unidades de <em>{nome_f}</em></div>
    <div class="confirm-linha">📅 <span class="confirm-destaque">Data:</span> {data_op.strftime('%d/%m/%Y')}</div>
    <div class="confirm-linha">💰 <span class="confirm-destaque">Custo por unidade:</span> {_fmt_brl(custo_unit_f)}</div>
</div>
""", unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

btn_confirmar = st.button("✂️ Confirmar fracionamento", type="primary", use_container_width=True)

if btn_confirmar:
    data_str  = data_op.strftime("%d/%m/%Y")
    batch_id  = "FRAC-" + datetime.now().strftime("%Y%m%d%H%M%S")
    refid     = _refid(data_str, nome_f, qtd_prod, custo_unit_f)

    with st.spinner("Registrando... aguarde ⏳"):
        ws_mov  = _ensure_ws("MovimentosEstoque", MOV_HEADERS)
        ws_comp = _ensure_ws("Compras", COMP_HEADERS)

        # 1) Saída do galão (granel) — subtrai do estoque
        _append_row(ws_mov, {
            "Data":      data_str,
            "IDProduto": pid_g,
            "Produto":   nome_g,
            "Tipo":      "saida",
            "Qtd":       str(litros_usados).replace(".", ","),
            "Obs":       f"{batch_id} — saída p/ fracionamento",
        })

        # 2) Entrada do fracionado — adiciona ao estoque
        _append_row(ws_mov, {
            "Data":      data_str,
            "IDProduto": pid_f,
            "Produto":   nome_f,
            "Tipo":      "entrada",
            "Qtd":       str(int(qtd_prod)),
            "Obs":       f"{batch_id} — entrada de fracionado",
        })

        # 3) Compra interna (atualiza CustoAtual do fracionado)
        if not _ja_existe(ws_comp, refid):
            total = round(float(qtd_prod) * float(custo_unit_f), 2)
            _append_row(ws_comp, {
                "Data":           data_str,
                "Produto":        nome_f,
                "Unidade":        unid_f or "un",
                "Fornecedor":     "Produção interna",
                "Qtd":            str(int(qtd_prod)),
                "Custo Unitário": f"{custo_unit_f:.2f}".replace(".", ","),
                "Total":          f"{total:.2f}".replace(".", ","),
                "IDProduto":      pid_f,
                "Obs":            f"{batch_id} — frac {vol_unit:.1f}L/un",
                "RefID":          refid,
            })

        # Limpa o cache pra dashboard atualizar
        st.cache_data.clear()

    st.balloons()
    st.success(f"✅ Fracionamento registrado com sucesso! {qtd_prod} unidades de {nome_f} geradas.")

    st.markdown(f"""
    <div class="confirm-box" style="margin-top:16px;">
        <div class="confirm-titulo">📋 Resumo do que foi feito</div>
        <div class="confirm-linha">✅ Saída de <strong>{litros_usados:.1f} L</strong> do galão de <em>{nome_g}</em></div>
        <div class="confirm-linha">✅ Entrada de <strong>{qtd_prod} un.</strong> de <em>{nome_f}</em> no estoque</div>
        <div class="confirm-linha">✅ Custo atualizado para <strong>{_fmt_brl(custo_unit_f)}</strong> por unidade</div>
        <div class="confirm-linha" style="margin-top:10px; color:rgba(255,255,255,0.4); font-size:0.78rem;">Código da operação: {batch_id}</div>
    </div>
    """, unsafe_allow_html=True)
