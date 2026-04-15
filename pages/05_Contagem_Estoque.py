# pages/05_Contagem_Estoque.py
# -*- coding: utf-8 -*-

import json, re, unicodedata as _ud
from datetime import datetime
import streamlit as st
import pandas as pd
import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from google.oauth2.service_account import Credentials

# ──────────────────────────────────────────────
#  CONFIG & TEMA
# ──────────────────────────────────────────────
import pathlib
_cfg = pathlib.Path(".streamlit"); _cfg.mkdir(exist_ok=True)
(_cfg / "config.toml").write_text('[theme]\nbase = "dark"\n')

st.set_page_config(
    page_title="Contagem de Estoque",
    page_icon="📦",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Nunito:wght@400;600;700;800;900&family=DM+Sans:wght@300;400;500&display=swap');
html, body, [class*="css"] { font-family: 'DM Sans', sans-serif; }

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
.progresso-wrap {
    background:rgba(255,255,255,0.06); border-radius:16px; padding:18px 22px;
    border:1px solid rgba(255,255,255,0.09); margin-bottom:22px;
}
.prog-label { font-size:0.78rem; color:rgba(255,255,255,0.5); font-weight:600;
    text-transform:uppercase; letter-spacing:0.5px; margin-bottom:8px; }
.prog-bar-bg { background:rgba(255,255,255,0.1); border-radius:100px; height:12px; overflow:hidden; }
.prog-bar-fill { height:12px; border-radius:100px;
    background:linear-gradient(90deg,#4ade80,#22d3ee); transition:width 0.4s ease; }
.prog-nums { display:flex; justify-content:space-between; margin-top:8px;
    font-size:0.82rem; color:rgba(255,255,255,0.6); }

.hist-card {
    background:rgba(255,255,255,0.04); border:1px solid rgba(255,255,255,0.09);
    border-radius:14px; padding:14px 18px; margin-bottom:10px;
    display:flex; align-items:center; gap:16px;
}
.hist-icone { font-size:1.6rem; flex-shrink:0; }
.hist-data { font-family:'Nunito',sans-serif; font-weight:800; font-size:0.95rem; color:#fff; }
.hist-detalhe { font-size:0.78rem; color:rgba(255,255,255,0.45); margin-top:3px; }
.hist-badge {
    margin-left:auto; background:rgba(74,222,128,0.15); border:1px solid rgba(74,222,128,0.3);
    color:#4ade80; border-radius:20px; padding:4px 12px; font-size:0.75rem; font-weight:700;
}
.ciclo-ok {
    background:linear-gradient(135deg,rgba(74,222,128,0.12),rgba(34,211,238,0.08));
    border:1.5px solid rgba(74,222,128,0.35); border-radius:18px;
    padding:28px 32px; margin-bottom:20px; text-align:center;
}
.ciclo-ok h2 { font-family:'Nunito',sans-serif; font-size:1.5rem; font-weight:900;
    color:#4ade80; margin:0 0 10px 0; }
.ciclo-ok p { color:rgba(255,255,255,0.7); font-size:0.92rem; margin:0; line-height:1.6; }

.prod-card {
    background:rgba(255,255,255,0.06); border-radius:20px; padding:20px;
    border:1px solid rgba(255,255,255,0.1); margin-bottom:20px;
    display:flex; gap:20px; align-items:center;
}
.prod-foto { width:90px; height:90px; border-radius:14px; object-fit:contain;
    background:rgba(255,255,255,0.08); border:1px solid rgba(255,255,255,0.1); flex-shrink:0; }
.prod-foto-ph {
    width:90px; height:90px; border-radius:14px;
    background:rgba(255,255,255,0.06); border:1px solid rgba(255,255,255,0.1);
    display:flex; align-items:center; justify-content:center; font-size:2.2rem; flex-shrink:0;
}
.prod-nome { font-family:'Nunito',sans-serif; font-weight:800; font-size:1.05rem; color:#fff; margin-bottom:4px; }
.prod-cat  { font-size:0.78rem; color:rgba(255,255,255,0.45); margin-bottom:8px; }
.prod-badge-ok   { display:inline-flex; align-items:center; gap:5px; background:rgba(74,222,128,0.15);
    border:1px solid rgba(74,222,128,0.3); color:#4ade80; border-radius:8px;
    padding:4px 10px; font-size:0.75rem; font-weight:700; }
.prod-badge-pend { display:inline-flex; align-items:center; gap:5px; background:rgba(251,191,36,0.12);
    border:1px solid rgba(251,191,36,0.3); color:#fbbf24; border-radius:8px;
    padding:4px 10px; font-size:0.75rem; font-weight:700; }
.est-item { background:rgba(255,255,255,0.06); border-radius:10px; padding:8px 14px;
    text-align:center; display:inline-block; margin-top:10px; }
.est-val  { font-family:'Nunito',sans-serif; font-size:1.2rem; font-weight:800; color:#fff; }
.est-lab  { font-size:0.68rem; color:rgba(255,255,255,0.4); text-transform:uppercase; letter-spacing:0.5px; }
.sec-titulo {
    font-family:'Nunito',sans-serif; font-weight:800; font-size:1.05rem;
    color:rgba(255,255,255,0.9); margin:24px 0 12px 0;
    display:flex; align-items:center; gap:8px;
}
.sec-titulo::after {
    content:''; flex:1; height:1px;
    background:linear-gradient(to right,rgba(255,255,255,0.15),transparent);
    margin-left:8px; border-radius:2px;
}
.delta-plus  { color:#4ade80; font-weight:700; font-size:0.92rem; margin:6px 0; }
.delta-minus { color:#f87171; font-weight:700; font-size:0.92rem; margin:6px 0; }
.delta-zero  { color:rgba(255,255,255,0.4); font-size:0.92rem; margin:6px 0; }
.info-box {
    background:rgba(96,165,250,0.08); border:1px solid rgba(96,165,250,0.2);
    border-radius:12px; padding:12px 16px; font-size:0.82rem;
    color:rgba(255,255,255,0.65); line-height:1.5; margin-top:10px;
}
footer { display:none !important; }
#MainMenu { display:none !important; }
[data-testid="stToolbar"] { display:none !important; }
[data-testid="stHeader"]  { display:none !important; }
div[data-testid="stPageLink"] a {
    background: rgba(255,255,255,0.07) !important; border-radius: 10px !important;
    padding: 6px 10px !important; font-size: 0.78rem !important; font-weight: 600 !important;
    color: rgba(255,255,255,0.75) !important; text-decoration: none !important;
    white-space: nowrap !important;
}
div[data-testid="stPageLink"] a:hover {
    background: rgba(255,255,255,0.15) !important; color: #fff !important;
}
</style>
""", unsafe_allow_html=True)


# ──────────────────────────────────────────────
#  BARRA DE NAVEGAÇÃO
# ──────────────────────────────────────────────
with st.container():
    nav_cols = st.columns(12)
    _pages = [
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
    for i, (label, path) in enumerate(_pages):
        with nav_cols[i]:
            st.page_link(path, label=label)


# ──────────────────────────────────────────────
#  HELPERS GOOGLE SHEETS
# ──────────────────────────────────────────────
def _normalize_private_key(key):
    if not isinstance(key, str): return key
    key = key.replace("\\n", "\n")
    return "".join(ch for ch in key if _ud.category(ch)[0] != "C" or ch in ("\n","\r","\t"))

def _load_sa():
    svc = st.secrets.get("GCP_SERVICE_ACCOUNT")
    if svc is None: st.error("🛑 GCP_SERVICE_ACCOUNT ausente."); st.stop()
    if isinstance(svc, str): svc = json.loads(svc)
    svc = dict(svc); svc["private_key"] = _normalize_private_key(svc["private_key"])
    return svc

@st.cache_resource
def _sheet():
    scopes = ["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
    creds  = Credentials.from_service_account_info(_load_sa(), scopes=scopes)
    gc     = gspread.authorize(creds)
    url    = st.secrets.get("PLANILHA_URL")
    if not url: st.error("🛑 PLANILHA_URL ausente."); st.stop()
    return gc.open_by_url(url) if str(url).startswith("http") else gc.open_by_key(url)

@st.cache_data(ttl=15, show_spinner=False)
def _aba(nome):
    ws = _sheet().worksheet(nome)
    df = get_as_dataframe(ws, evaluate_formulas=True, dtype=str, header=0).dropna(how="all")
    df.columns = [c.strip() for c in df.columns]
    return df.fillna("")

def _first_col(df, cands):
    for c in cands:
        if c in df.columns: return c
    lower = {c.lower(): c for c in df.columns}
    for c in cands:
        if c.lower() in lower: return lower[c.lower()]
    return None


# ──────────────────────────────────────────────
#  HELPERS NUMÉRICOS
# ──────────────────────────────────────────────
def _strip(s):
    s = _ud.normalize("NFKD", str(s or ""))
    return "".join(ch for ch in s if _ud.category(ch) != "Mn").lower().strip()

def _to_num(x) -> float:
    if x is None: return 0.0
    s = str(x).strip()
    if s == "" or s.lower() in ("nan","none"): return 0.0
    neg = False
    s = s.replace("−","-").replace("\u2212","-")
    if s.startswith("(") and s.endswith(")"): s = s[1:-1]; neg = True
    s = s.replace("R$","").replace(" ","")
    if s.startswith("-"): neg = True; s = s[1:]
    if "," in s and "." in s:
        s = s.replace(".","").replace(",",".")
    elif "," in s:
        s = s.replace(",",".")
    s = re.sub(r"[^0-9.]","",s)
    if s.count(".") > 1:
        p = s.split(".")
        s = "".join(p[:-1]) + "." + p[-1]
    try: v = float(s)
    except: return 0.0
    return -v if neg else v

def _norm_tipo(t) -> str:
    raw = str(t or ""); low = _strip(raw)
    if "fracion" in low:
        return "entrada" if "+" in raw else "saida" if "-" in raw else "outro"
    lowc = re.sub(r"[^a-z]","",low)
    if "contagem" in lowc or "inventario" in lowc: return "ajuste"
    if "entrada" in lowc or "compra" in lowc or "estorno" in lowc: return "entrada"
    if "saida"   in lowc or "venda"  in lowc or "baixa"   in lowc: return "saida"
    if "ajuste"  in lowc: return "ajuste"
    return "outro"

def _nz(x):
    if x is None: return ""
    try:
        if pd.isna(x): return ""
    except: pass
    s = str(x).strip()
    return "" if s.lower() in ("nan","none") else s

def _prod_key(pid, pnome):
    p = _nz(pid)
    return p if p else f"nm:{_strip(_nz(pnome))}"

def _fmt_num(v):
    try:
        f = float(v)
        return str(int(f)) if f == int(f) else f"{f:.2f}"
    except:
        return str(v)


# ──────────────────────────────────────────────
#  CARREGAR PRODUTOS E MOVIMENTOS
# ──────────────────────────────────────────────
try:
    df_prod = _aba("Produtos")
except Exception as e:
    st.error("Erro ao abrir aba Produtos"); st.code(str(e)); st.stop()

try:
    df_mov = _aba("MovimentosEstoque")
except Exception:
    df_mov = pd.DataFrame(columns=["Data","IDProduto","Produto","Tipo","Qtd","Obs"])

col_ativo = _first_col(df_prod, ["Ativo?","Ativo","ativo"])
if col_ativo:
    df_prod = df_prod[df_prod[col_ativo].str.lower().str.strip().isin(["sim","s","1","true","yes","ativo"])]

col_id   = _first_col(df_prod, ["ID","Id","Codigo","Código","SKU"])
col_nome = _first_col(df_prod, ["Nome","Produto","Descrição","Descricao"])
col_cat  = _first_col(df_prod, ["Categoria","categoria"])
col_foto = _first_col(df_prod, ["Foto","foto","Imagem","imagem"])

df_prod["_id"]   = df_prod[col_id]   if col_id   else ""
df_prod["_nome"] = df_prod[col_nome] if col_nome else ""
df_prod["_cat"]  = df_prod[col_cat]  if col_cat  else ""
df_prod["_foto"] = df_prod[col_foto] if col_foto else ""
df_prod["__key"] = df_prod.apply(lambda r: _prod_key(r.get(col_id,""), r.get(col_nome,"")), axis=1)
df_prod.reset_index(drop=True, inplace=True)

for c in ["Tipo","Qtd","IDProduto","Produto"]:
    if c not in df_mov.columns: df_mov[c] = ""

if not df_mov.empty:
    df_mov["_tnorm"] = df_mov["Tipo"].apply(_norm_tipo)
    df_mov["_qtd"]   = df_mov["Qtd"].map(_to_num)
    df_mov["__key"]  = df_mov.apply(lambda r: _prod_key(r.get("IDProduto",""), r.get("Produto","")), axis=1)
else:
    df_mov["_tnorm"] = pd.Series([], dtype=str)
    df_mov["_qtd"]   = pd.Series([], dtype=float)
    df_mov["__key"]  = pd.Series([], dtype=str)

def estoque_atual(ch) -> float:
    if df_mov.empty: return 0.0
    g = df_mov[df_mov["__key"] == ch].groupby("_tnorm")["_qtd"].sum()
    return float(g.get("entrada", 0.0) - g.get("saida", 0.0) + g.get("ajuste", 0.0))


# ──────────────────────────────────────────────
#  PERSISTÊNCIA NA ABA CONFIG
#
#  Chaves:
#    contagem_ciclo_id    → data/hora de início do ciclo, ex: "14/04/2026 09:32"
#    contagem_contados    → JSON list das keys contadas no ciclo atual
#    contagem_ciclo_done  → "1" se concluído, "0" se em andamento
#    contagem_historico   → JSON list de ciclos concluídos
#
#  REGRA FUNDAMENTAL: o progresso NUNCA é apagado automaticamente.
#  Só zera quando o usuário clicar em "Iniciar nova contagem".
# ──────────────────────────────────────────────

@st.cache_data(ttl=10, show_spinner=False)
def _ler_config() -> dict:
    try:
        df_cfg = _aba("Config")
        col_p = _first_col(df_cfg, ["Parametro","parametro","Parâmetro","Chave","chave","Key","key"])
        col_v = _first_col(df_cfg, ["Valor","valor","Value","value"])
        if not col_p or not col_v: return {}
        return dict(zip(df_cfg[col_p].astype(str), df_cfg[col_v].astype(str)))
    except:
        return {}

def _salvar_config(chave: str, valor: str):
    try:
        ws  = _sheet().worksheet("Config")
        cur = get_as_dataframe(ws, evaluate_formulas=True, dtype=str, header=0).dropna(how="all")
        cur.columns = [c.strip() for c in cur.columns]
        col_p = _first_col(cur, ["Parametro","parametro","Parâmetro","Chave","chave","Key","key"])
        col_v = _first_col(cur, ["Valor","valor","Value","value"])
        if not col_p or not col_v:
            col_p, col_v = "Parametro", "Valor"
            cur = pd.DataFrame(columns=[col_p, col_v])
        if chave in cur[col_p].values:
            idx = cur.index[cur[col_p] == chave][0]
            cur.at[idx, col_v] = valor
        else:
            nova = {c: "" for c in cur.columns}
            nova[col_p] = chave; nova[col_v] = valor
            cur = pd.concat([cur, pd.DataFrame([nova])], ignore_index=True)
        ws.clear()
        set_with_dataframe(ws, cur.fillna(""), include_index=False, include_column_header=True, resize=True)
        _ler_config.clear()
        _aba.clear()
    except Exception as e:
        st.warning(f"Aviso: não foi possível salvar configuração: {e}")

def _carregar_estado():
    cfg = _ler_config()
    ciclo_id      = cfg.get("contagem_ciclo_id", "")
    contados_raw  = cfg.get("contagem_contados", "[]")
    historico_raw = cfg.get("contagem_historico", "[]")
    ciclo_done    = cfg.get("contagem_ciclo_done", "0") == "1"

    try: contados = set(json.loads(contados_raw))
    except: contados = set()

    try: historico = json.loads(historico_raw)
    except: historico = []

    return contados, historico, ciclo_id, ciclo_done

def _salvar_contados(contados: set):
    _salvar_config("contagem_contados", json.dumps(list(contados)))

def _iniciar_ciclo() -> str:
    ciclo_id = datetime.now().strftime("%d/%m/%Y %H:%M")
    _salvar_config("contagem_ciclo_id",   ciclo_id)
    _salvar_config("contagem_contados",   "[]")
    _salvar_config("contagem_ciclo_done", "0")
    return ciclo_id

def _concluir_ciclo(total: int, contados: set, ciclo_id: str):
    cfg = _ler_config()
    try: historico = json.loads(cfg.get("contagem_historico", "[]"))
    except: historico = []

    entrada = {
        "ciclo_id":       ciclo_id,
        "data_conclusao": datetime.now().strftime("%d/%m/%Y %H:%M"),
        "total":          total,
        "contados":       len(contados),
    }
    historico.insert(0, entrada)
    historico = historico[:24]

    _salvar_config("contagem_historico",  json.dumps(historico))
    _salvar_config("contagem_ciclo_done", "1")


# ──────────────────────────────────────────────
#  SESSION STATE — carrega da planilha apenas 1x
# ──────────────────────────────────────────────
if "cnt_inicializado" not in st.session_state:
    contados_i, historico_i, ciclo_id_i, ciclo_done_i = _carregar_estado()
    st.session_state["cnt_contados"]   = contados_i
    st.session_state["cnt_historico"]  = historico_i
    st.session_state["cnt_ciclo_id"]   = ciclo_id_i
    st.session_state["cnt_ciclo_done"] = ciclo_done_i
    st.session_state["cnt_inicializado"] = True

if "prod_sel" not in st.session_state:
    st.session_state["prod_sel"] = df_prod["__key"].iloc[0] if not df_prod.empty else None

contados   = st.session_state["cnt_contados"]
historico  = st.session_state["cnt_historico"]
ciclo_id   = st.session_state["cnt_ciclo_id"]
ciclo_done = st.session_state["cnt_ciclo_done"]
total      = len(df_prod)
contados_n = len(contados)
pct        = int(contados_n / total * 100) if total else 0


# ──────────────────────────────────────────────
#  HEADER
# ──────────────────────────────────────────────
ciclo_label = f"iniciada em {ciclo_id}" if ciclo_id else "nenhuma contagem ativa"
st.markdown(f"""
<div class="page-header">
  <div>
    <h1>📦 Contagem de Estoque</h1>
    <div class="sub">Ebenezér Variedades · {ciclo_label}</div>
  </div>
  <div class="header-badge">{contados_n} de {total} contados</div>
</div>
""", unsafe_allow_html=True)


# ──────────────────────────────────────────────
#  TELA A: SEM CICLO ATIVO
# ──────────────────────────────────────────────
if not ciclo_id and not ciclo_done:
    st.markdown("""
    <div style="background:rgba(96,165,250,0.08);border:1.5px solid rgba(96,165,250,0.25);
    border-radius:18px;padding:36px;text-align:center;margin-top:10px">
        <div style="font-size:3rem;margin-bottom:14px">📋</div>
        <div style="font-family:Nunito;font-size:1.3rem;font-weight:800;color:#fff;margin-bottom:10px">
            Nenhuma contagem em andamento
        </div>
        <div style="color:rgba(255,255,255,0.55);font-size:0.9rem;margin-bottom:24px;line-height:1.6">
            Clique abaixo para começar.<br>
            Você vai contar um produto de cada vez e o progresso fica salvo automaticamente.
        </div>
    </div>
    """, unsafe_allow_html=True)
    st.markdown("<br>", unsafe_allow_html=True)
    if st.button("▶️ Iniciar contagem de estoque", type="primary", use_container_width=True):
        novo = _iniciar_ciclo()
        st.session_state["cnt_ciclo_id"]   = novo
        st.session_state["cnt_contados"]   = set()
        st.session_state["cnt_ciclo_done"] = False
        st.cache_data.clear()
        st.rerun()

    if historico:
        st.markdown('<div class="sec-titulo" style="margin-top:32px">📅 Histórico de contagens</div>', unsafe_allow_html=True)
        for h in historico:
            st.markdown(f"""
            <div class="hist-card">
              <div class="hist-icone">✅</div>
              <div>
                <div class="hist-data">Iniciada em {h.get('ciclo_id','?')}</div>
                <div class="hist-detalhe">Concluída em {h.get('data_conclusao','?')} · {h.get('total','?')} produtos</div>
              </div>
              <div class="hist-badge">100% ✓</div>
            </div>
            """, unsafe_allow_html=True)
    st.stop()


# ──────────────────────────────────────────────
#  TELA B: CICLO CONCLUÍDO
# ──────────────────────────────────────────────
if ciclo_done:
    st.balloons()
    st.markdown(f"""
    <div class="ciclo-ok">
      <h2>🎉 Contagem concluída!</h2>
      <p>
        Todos os <strong>{total} produtos</strong> foram verificados e o estoque está atualizado.<br>
        A contagem foi registrada no histórico abaixo.<br><br>
        Quando quiser fazer uma nova contagem, clique em <strong>"Iniciar nova contagem"</strong>.
      </p>
    </div>
    """, unsafe_allow_html=True)

    if historico:
        st.markdown('<div class="sec-titulo">📅 Histórico de contagens</div>', unsafe_allow_html=True)
        for h in historico:
            st.markdown(f"""
            <div class="hist-card">
              <div class="hist-icone">✅</div>
              <div>
                <div class="hist-data">Iniciada em {h.get('ciclo_id','?')}</div>
                <div class="hist-detalhe">Concluída em {h.get('data_conclusao','?')} · {h.get('total','?')} produtos</div>
              </div>
              <div class="hist-badge">100% ✓</div>
            </div>
            """, unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)
    if st.button("▶️ Iniciar nova contagem", type="primary", use_container_width=True):
        novo = _iniciar_ciclo()
        st.session_state["cnt_ciclo_id"]   = novo
        st.session_state["cnt_contados"]   = set()
        st.session_state["cnt_ciclo_done"] = False
        st.cache_data.clear()
        st.rerun()
    st.stop()


# ──────────────────────────────────────────────
#  BARRA DE PROGRESSO
# ──────────────────────────────────────────────
st.markdown(f"""
<div class="progresso-wrap">
  <div class="prog-label">Progresso da contagem</div>
  <div class="prog-bar-bg">
    <div class="prog-bar-fill" style="width:{pct}%"></div>
  </div>
  <div class="prog-nums">
    <span>✅ {contados_n} produtos contados</span>
    <span>⏳ {total - contados_n} pendentes &nbsp;·&nbsp; {pct}%</span>
  </div>
</div>
""", unsafe_allow_html=True)


# ──────────────────────────────────────────────
#  LAYOUT PRINCIPAL
# ──────────────────────────────────────────────
col_left, col_right = st.columns([1.1, 1], gap="large")

# ══════════════════════════════════
#  ESQUERDA — Seleciona e conta
# ══════════════════════════════════
with col_left:
    st.markdown('<div class="sec-titulo">🔍 Selecionar produto</div>', unsafe_allow_html=True)

    busca = st.text_input("", placeholder="🔎  Digite o nome do produto...", label_visibility="collapsed")

    df_filtrado = df_prod.copy()
    if busca.strip():
        b = _strip(busca)
        df_filtrado = df_filtrado[df_filtrado["_nome"].apply(lambda x: b in _strip(x))]

    if df_filtrado.empty:
        st.warning("Nenhum produto encontrado.")
        st.stop()

    opts   = df_filtrado["__key"].tolist()
    labels = {
        r["__key"]: ("✅ " if r["__key"] in contados else "") + r["_nome"]
        for _, r in df_filtrado.iterrows()
    }

    sel = st.session_state["prod_sel"]
    idx = opts.index(sel) if sel in opts else 0

    sel_key = st.selectbox(
        "Produto", options=opts,
        format_func=lambda k: labels.get(k, k),
        index=idx, label_visibility="collapsed",
    )
    st.session_state["prod_sel"] = sel_key

    row_p      = df_prod[df_prod["__key"] == sel_key].iloc[0]
    prod_id    = _nz(row_p.get("_id",""))
    prod_nome  = _nz(row_p.get("_nome",""))
    prod_cat   = _nz(row_p.get("_cat",""))
    prod_foto  = _nz(row_p.get("_foto",""))
    est_atual  = estoque_atual(sel_key)
    ja_contado = sel_key in contados

    status_badge = (
        '<span class="prod-badge-ok">✅ Já contado</span>' if ja_contado
        else '<span class="prod-badge-pend">⏳ Pendente</span>'
    )
    if prod_foto and prod_foto.startswith("http"):
        foto_html = f'<img src="{prod_foto}" class="prod-foto" onerror="this.style.display=\'none\'">'
    else:
        foto_html = '<div class="prod-foto-ph">📦</div>'

    st.markdown(f"""
    <div class="prod-card">
      {foto_html}
      <div style="flex:1">
        <div class="prod-nome">{prod_nome}</div>
        <div class="prod-cat">{prod_cat or "Sem categoria"}</div>
        {status_badge}
        <div class="est-item">
          <div class="est-val">{_fmt_num(est_atual)}</div>
          <div class="est-lab">Estoque no sistema</div>
        </div>
      </div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown('<div class="sec-titulo">✏️ Quantidade contada fisicamente</div>', unsafe_allow_html=True)

    alvo = st.number_input(
        "Quantas unidades você contou na prateleira?",
        min_value=0.0, step=1.0,
        value=max(0.0, float(est_atual)),
        key=f"alvo_{sel_key}",
    )
    delta = alvo - est_atual

    if delta > 0:
        st.markdown(f'<div class="delta-plus">▲ Vai adicionar {delta:.0f} unidades ao estoque</div>', unsafe_allow_html=True)
    elif delta < 0:
        st.markdown(f'<div class="delta-minus">▼ Vai remover {abs(delta):.0f} unidades do estoque</div>', unsafe_allow_html=True)
    else:
        st.markdown('<div class="delta-zero">— Estoque correto, nenhuma alteração necessária</div>', unsafe_allow_html=True)

    responsavel = st.text_input("Responsável (opcional)", placeholder="Seu nome")

    btn_salvar = st.button("💾  Salvar contagem", type="primary", use_container_width=True)

    if btn_salvar:
        data_str  = datetime.now().strftime("%d/%m/%Y")
        resp_txt  = responsavel.strip() if responsavel.strip() else "—"
        obs_final = f"Contagem por {resp_txt}"

        try:
            ws_mov = _sheet().worksheet("MovimentosEstoque")
            hdrs   = ws_mov.row_values(1) or ["Data","IDProduto","Produto","Tipo","Qtd","Obs"]

            # Só grava ajuste se houve diferença
            if delta != 0:
                qtd_str  = str(int(delta) if float(delta).is_integer() else delta).replace(".",",")
                row_data = {
                    "Data": data_str, "IDProduto": prod_id, "Produto": prod_nome,
                    "Tipo": "Ajuste", "Qtd": qtd_str, "Obs": obs_final,
                }
                ws_mov.append_row([row_data.get(h,"") for h in hdrs], value_input_option="USER_ENTERED")

            # Marca como contado e persiste
            contados.add(sel_key)
            st.session_state["cnt_contados"] = contados
            _salvar_contados(contados)
            st.cache_data.clear()

            # Chegou a 100%?
            if len(contados) >= total:
                _concluir_ciclo(total, contados, ciclo_id)
                st.session_state["cnt_ciclo_done"] = True
                _, novo_hist, _, _ = _carregar_estado()
                st.session_state["cnt_historico"] = novo_hist
                st.balloons()
                st.rerun()
            else:
                if delta != 0:
                    sinal = "+" if delta > 0 else ""
                    st.success(f"✅ Salvo! Ajuste de {sinal}{_fmt_num(delta)} → estoque: {_fmt_num(alvo)}")
                else:
                    st.success(f"✅ Contagem registrada! Estoque confirmado: {_fmt_num(alvo)}")
                st.rerun()

        except Exception as e:
            st.error("Falha ao salvar.")
            st.code(str(e))


# ══════════════════════════════════
#  DIREITA — Painel visual
# ══════════════════════════════════
with col_right:
    st.markdown('<div class="sec-titulo">📋 Painel da contagem</div>', unsafe_allow_html=True)

    filtro = st.radio(
        "Mostrar", ["Todos", "✅ Contados", "⏳ Pendentes"],
        horizontal=True, label_visibility="collapsed",
    )

    df_audit = df_prod.copy()
    if busca.strip():
        b = _strip(busca)
        df_audit = df_audit[df_audit["_nome"].apply(lambda x: b in _strip(x))]
    if filtro == "✅ Contados":
        df_audit = df_audit[df_audit["__key"].isin(contados)]
    elif filtro == "⏳ Pendentes":
        df_audit = df_audit[~df_audit["__key"].isin(contados)]

    k1, k2, k3 = st.columns(3)
    for col_k, val, label, cor in [
        (k1, contados_n,           "Contados",  "#4ade80"),
        (k2, total - contados_n,   "Pendentes", "#fbbf24"),
        (k3, f"{pct}%",            "Progresso", "#60a5fa"),
    ]:
        with col_k:
            st.markdown(f"""
            <div style="background:rgba(255,255,255,0.05);border-radius:12px;padding:12px;
            text-align:center;border:1px solid rgba(255,255,255,0.08)">
              <div style="font-family:Nunito;font-size:1.4rem;font-weight:800;color:{cor}">{val}</div>
              <div style="font-size:0.7rem;color:rgba(255,255,255,0.4);text-transform:uppercase">{label}</div>
            </div>""", unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    if df_audit.empty:
        st.info("Nenhum produto neste filtro.")
    else:
        for i in range(0, len(df_audit), 3):
            chunk = df_audit.iloc[i:i+3]
            cols3 = st.columns(3)
            for ci, (_, prod_row) in zip(cols3, chunk.iterrows()):
                with ci:
                    chave   = prod_row["__key"]
                    nome_p  = prod_row["_nome"]
                    foto_p  = prod_row["_foto"]
                    est_p   = estoque_atual(chave)
                    is_done = chave in contados

                    borda = "rgba(74,222,128,0.4)" if is_done else "rgba(255,255,255,0.08)"
                    bg    = "rgba(74,222,128,0.05)" if is_done else "rgba(255,255,255,0.03)"
                    check = "✅ " if is_done else ""

                    if foto_p and foto_p.startswith("http"):
                        foto_tag = f'<img src="{foto_p}" style="width:100%;height:60px;object-fit:contain;border-radius:8px;background:rgba(255,255,255,0.06);margin-bottom:6px" onerror="this.style.display=\'none\'">'
                    else:
                        foto_tag = '<div style="width:100%;height:60px;border-radius:8px;background:rgba(255,255,255,0.06);display:flex;align-items:center;justify-content:center;font-size:1.4rem;margin-bottom:6px">📦</div>'

                    st.markdown(f"""
                    <div style="background:{bg};border:1px solid {borda};border-radius:12px;padding:10px;margin-bottom:4px">
                      {foto_tag}
                      <div style="font-size:0.72rem;font-weight:700;color:rgba(255,255,255,0.85);line-height:1.3">{check}{nome_p}</div>
                      <div style="font-size:0.65rem;color:rgba(255,255,255,0.35);margin-top:3px">Sistema: {_fmt_num(est_p)}</div>
                    </div>
                    """, unsafe_allow_html=True)

                    if st.button("Selecionar", key=f"sel_{chave}", use_container_width=True):
                        st.session_state["prod_sel"] = chave
                        st.rerun()

    if historico:
        st.markdown("<br>", unsafe_allow_html=True)
        st.markdown('<div class="sec-titulo">📅 Contagens anteriores</div>', unsafe_allow_html=True)
        for h in historico:
            st.markdown(f"""
            <div class="hist-card">
              <div class="hist-icone">📋</div>
              <div>
                <div class="hist-data">Iniciada em {h.get('ciclo_id','?')}</div>
                <div class="hist-detalhe">Concluída em {h.get('data_conclusao','?')} · {h.get('total','?')} produtos</div>
              </div>
              <div class="hist-badge">100% ✓</div>
            </div>
            """, unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)
    col_r1, col_r2 = st.columns(2)
    with col_r1:
        if st.button("🔄 Recarregar progresso", use_container_width=True,
                     help="Busca o progresso salvo na planilha — útil se outra pessoa está contando junto"):
            c2, h2, cid2, done2 = _carregar_estado()
            st.session_state["cnt_contados"]   = c2
            st.session_state["cnt_historico"]  = h2
            st.session_state["cnt_ciclo_id"]   = cid2
            st.session_state["cnt_ciclo_done"] = done2
            st.cache_data.clear()
            st.rerun()
    with col_r2:
        if st.button("🆕 Iniciar nova contagem", use_container_width=True,
                     help="Zera o progresso e começa do zero. Use só quando quiser fazer uma nova contagem."):
            novo = _iniciar_ciclo()
            st.session_state["cnt_ciclo_id"]   = novo
            st.session_state["cnt_contados"]   = set()
            st.session_state["cnt_ciclo_done"] = False
            st.cache_data.clear()
            st.rerun()

    st.markdown("""
    <div class="info-box">
        💾 O progresso é salvo automaticamente na planilha <strong>Config</strong>.<br>
        Pode fechar o app, desligar o celular — ao reabrir, continua de onde parou.<br>
        O progresso só zera se você clicar em <strong>"Iniciar nova contagem"</strong>.
    </div>
    """, unsafe_allow_html=True)
