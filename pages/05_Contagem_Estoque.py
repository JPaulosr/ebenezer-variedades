# pages/05_Contagem_Estoque.py — Contagem de estoque com ciclo mensal e progresso persistente
# -*- coding: utf-8 -*-

import json, re, unicodedata as _ud
from datetime import datetime
import streamlit as st
import pandas as pd
import gspread
from gspread_dataframe import get_as_dataframe
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
    background:rgba(255,255,255,0.06); border-radius:16px; padding:16px 22px;
    border:1px solid rgba(255,255,255,0.09); margin-bottom:22px;
}
.prog-label { font-size:0.78rem; color:rgba(255,255,255,0.5); font-weight:600; text-transform:uppercase; letter-spacing:0.5px; margin-bottom:8px; }
.prog-bar-bg { background:rgba(255,255,255,0.1); border-radius:100px; height:10px; overflow:hidden; }
.prog-bar-fill { height:10px; border-radius:100px; background:linear-gradient(90deg,#4ade80,#22d3ee); transition:width 0.4s ease; }
.prog-nums { display:flex; justify-content:space-between; margin-top:6px; font-size:0.8rem; color:rgba(255,255,255,0.6); }

/* Histórico de contagens */
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
    color:#4ade80; border-radius:20px; padding:4px 12px; font-size:0.75rem; font-weight:700; white-space:nowrap;
}

/* Alerta ciclo concluído */
.ciclo-ok {
    background:linear-gradient(135deg,rgba(74,222,128,0.12),rgba(34,211,238,0.08));
    border:1.5px solid rgba(74,222,128,0.35); border-radius:18px;
    padding:22px 26px; margin-bottom:20px; text-align:center;
}
.ciclo-ok h2 { font-family:'Nunito',sans-serif; font-size:1.3rem; font-weight:900; color:#4ade80; margin:0 0 8px 0; }
.ciclo-ok p  { color:rgba(255,255,255,0.7); font-size:0.9rem; margin:0; }

.prod-card {
    background:rgba(255,255,255,0.06); border-radius:20px; padding:20px;
    border:1px solid rgba(255,255,255,0.1); margin-bottom:20px;
    display:flex; gap:20px; align-items:center;
}
.prod-foto { width:90px; height:90px; border-radius:14px; object-fit:contain;
    background:rgba(255,255,255,0.08); border:1px solid rgba(255,255,255,0.1); flex-shrink:0; }
.prod-foto-placeholder {
    width:90px; height:90px; border-radius:14px;
    background:rgba(255,255,255,0.06); border:1px solid rgba(255,255,255,0.1);
    display:flex; align-items:center; justify-content:center; font-size:2.2rem; flex-shrink:0;
}
.prod-info { flex:1; }
.prod-nome { font-family:'Nunito',sans-serif; font-weight:800; font-size:1.05rem; color:#fff; margin-bottom:4px; }
.prod-cat  { font-size:0.78rem; color:rgba(255,255,255,0.45); margin-bottom:8px; }
.prod-badge-ok   { display:inline-flex; align-items:center; gap:5px; background:rgba(74,222,128,0.15); border:1px solid rgba(74,222,128,0.3); color:#4ade80; border-radius:8px; padding:4px 10px; font-size:0.75rem; font-weight:700; }
.prod-badge-pend { display:inline-flex; align-items:center; gap:5px; background:rgba(251,191,36,0.12); border:1px solid rgba(251,191,36,0.3); color:#fbbf24; border-radius:8px; padding:4px 10px; font-size:0.75rem; font-weight:700; }
.estoque-info { display:flex; gap:16px; margin-top:10px; }
.est-item { background:rgba(255,255,255,0.06); border-radius:10px; padding:8px 14px; text-align:center; }
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

.delta-plus  { color:#4ade80; font-weight:700; font-size:0.9rem; }
.delta-minus { color:#f87171; font-weight:700; font-size:0.9rem; }
.delta-zero  { color:rgba(255,255,255,0.4); font-size:0.9rem; }

button[kind="primary"] { border-radius:12px !important; font-weight:700 !important; }
footer { display:none !important; }
#MainMenu { display:none !important; }
[data-testid="stToolbar"] { display:none !important; }
[data-testid="stHeader"]  { display:none !important; }
</style>
""", unsafe_allow_html=True)


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
#  HELPERS NUMÉRICOS / NORMALIZAÇÃO
# ──────────────────────────────────────────────
def _strip(s):
    s = _ud.normalize("NFKD", str(s or ""))
    return "".join(ch for ch in s if _ud.category(ch) != "Mn").lower().strip()

def _to_num(x) -> float:
    """Converte string numérica BR para float, preservando sinal negativo corretamente."""
    if x is None: return 0.0
    s = str(x).strip()
    if s == "" or s.lower() in ("nan", "none"): return 0.0
    # Suporte a parênteses negativos: (38) → -38
    neg = False
    s = s.replace("−", "-").replace("\u2212", "-")
    if s.startswith("(") and s.endswith(")"): s = s[1:-1]; neg = True
    s = s.replace("R$", "").replace(" ", "")
    # Detectar sinal negativo no início ANTES de limpar
    if s.startswith("-"): neg = True; s = s[1:]
    # Formato BR: vírgula como decimal
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        s = s.replace(",", ".")
    # Remover qualquer caractere não numérico restante (exceto ponto)
    s = re.sub(r"[^0-9.]", "", s)
    if s.count(".") > 1:
        p = s.split(".")
        s = "".join(p[:-1]) + "." + p[-1]
    try:
        v = float(s)
    except:
        return 0.0
    return -v if neg else v

def _norm_tipo(t) -> str:
    raw = str(t or ""); low = _strip(raw)
    if "fracion" in low:
        return "entrada" if "+" in raw else "saida" if "-" in raw else "outro"
    lowc = re.sub(r"[^a-z]", "", low)
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
        return str(int(f)) if f == int(f) else str(f)
    except:
        return str(v)


# ──────────────────────────────────────────────
#  CARREGAR DADOS
# ──────────────────────────────────────────────
try:
    df_prod = _aba("Produtos")
except Exception as e:
    st.error("Erro ao abrir aba Produtos"); st.code(str(e)); st.stop()

try:
    df_mov = _aba("MovimentosEstoque")
except Exception:
    df_mov = pd.DataFrame(columns=["Data","IDProduto","Produto","Tipo","Qtd","Obs"])

# Só produtos ativos
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

# Pré-processar movimentos
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
#  Chaves usadas:
#    contagem_ciclo_id        → ex: "2026-04"  (ano-mês do ciclo atual)
#    contagem_contados        → JSON list de keys contados no ciclo atual
#    contagem_historico       → JSON list de {ciclo, data_conclusao, total, produtos}
# ──────────────────────────────────────────────
_CICLO_ATUAL = datetime.now().strftime("%Y-%m")   # ex: "2026-04"

@st.cache_data(ttl=10, show_spinner=False)
def _ler_config() -> dict:
    """Lê a aba Config e retorna dict {parametro: valor}."""
    try:
        df_cfg = _aba("Config")
        col_p = _first_col(df_cfg, ["Parametro","parametro","Parâmetro","chave","Chave","Key","key"])
        col_v = _first_col(df_cfg, ["Valor","valor","Value","value"])
        if not col_p or not col_v: return {}
        return dict(zip(df_cfg[col_p].astype(str), df_cfg[col_v].astype(str)))
    except:
        return {}

def _salvar_config(chave: str, valor: str):
    """Grava ou atualiza uma chave na aba Config."""
    try:
        ws  = _sheet().worksheet("Config")
        cur = get_as_dataframe(ws, evaluate_formulas=True, dtype=str, header=0).dropna(how="all")
        cur.columns = [c.strip() for c in cur.columns]
        col_p = _first_col(cur, ["Parametro","parametro","Parâmetro","chave","Chave","Key","key"])
        col_v = _first_col(cur, ["Valor","valor","Value","value"])
        if not col_p or not col_v:
            # Criar colunas se não existem
            col_p, col_v = "Parametro", "Valor"
        if chave in cur[col_p].values:
            idx = cur.index[cur[col_p] == chave][0]
            cur.at[idx, col_v] = valor
        else:
            nova = {c: "" for c in cur.columns}
            nova[col_p] = chave; nova[col_v] = valor
            cur = pd.concat([cur, pd.DataFrame([nova])], ignore_index=True)
        ws.clear()
        from gspread_dataframe import set_with_dataframe
        set_with_dataframe(ws, cur.fillna(""), include_index=False, include_column_header=True, resize=True)
        _ler_config.clear()
        _aba.clear()
    except Exception as e:
        st.warning(f"Aviso: não foi possível salvar configuração: {e}")

def _carregar_estado():
    """Carrega o estado do ciclo atual da Config."""
    cfg = _ler_config()
    ciclo_salvo  = cfg.get("contagem_ciclo_id", "")
    contados_raw = cfg.get("contagem_contados", "[]")
    historico_raw = cfg.get("contagem_historico", "[]")

    try: contados = set(json.loads(contados_raw))
    except: contados = set()

    try: historico = json.loads(historico_raw)
    except: historico = []

    # Se o ciclo salvo é diferente do mês atual → novo ciclo, zera contados
    if ciclo_salvo != _CICLO_ATUAL:
        contados = set()

    return contados, historico, ciclo_salvo

def _salvar_contados(contados: set):
    """Persiste lista de contados e ciclo atual na Config."""
    _salvar_config("contagem_ciclo_id", _CICLO_ATUAL)
    _salvar_config("contagem_contados", json.dumps(list(contados)))

def _concluir_ciclo(total_produtos: int, contados: set):
    """Registra conclusão do ciclo no histórico e reseta contados."""
    cfg = _ler_config()
    try: historico = json.loads(cfg.get("contagem_historico", "[]"))
    except: historico = []

    entrada = {
        "ciclo":          _CICLO_ATUAL,
        "data_conclusao": datetime.now().strftime("%d/%m/%Y %H:%M"),
        "total":          total_produtos,
        "contados":       len(contados),
    }
    # Evitar duplicata do mesmo ciclo
    historico = [h for h in historico if h.get("ciclo") != _CICLO_ATUAL]
    historico.insert(0, entrada)
    historico = historico[:12]  # Guardar só os últimos 12 meses

    _salvar_config("contagem_historico", json.dumps(historico))
    # Reseta contados para próxima contagem (mantém ciclo marcado como concluído)
    _salvar_config("contagem_contados", json.dumps([]))
    _salvar_config("contagem_ciclo_id", _CICLO_ATUAL + "_done")


# ──────────────────────────────────────────────
#  INICIALIZAR SESSION STATE
# ──────────────────────────────────────────────
if "contagem_inicializada" not in st.session_state:
    contados, historico, ciclo_salvo = _carregar_estado()
    st.session_state["contados"]   = contados
    st.session_state["historico"]  = historico
    st.session_state["ciclo_done"] = ciclo_salvo.endswith("_done")
    st.session_state["contagem_inicializada"] = True

if "prod_sel" not in st.session_state:
    st.session_state["prod_sel"] = df_prod["__key"].iloc[0] if not df_prod.empty else None


# ──────────────────────────────────────────────
#  HEADER
# ──────────────────────────────────────────────
total       = len(df_prod)
contados_n  = len(st.session_state["contados"])
pct         = int(contados_n / total * 100) if total else 0
ciclo_label = datetime.strptime(_CICLO_ATUAL, "%Y-%m").strftime("%B/%Y").capitalize()

st.markdown(f"""
<div class="page-header">
  <div>
    <h1>📦 Contagem de Estoque</h1>
    <div class="sub">Ebenezér Variedades · Ciclo: {ciclo_label}</div>
  </div>
  <div class="header-badge">{contados_n} de {total} contados</div>
</div>
""", unsafe_allow_html=True)

# Barra de progresso
cor_barra = "#4ade80" if pct == 100 else "linear-gradient(90deg,#4ade80,#22d3ee)"
st.markdown(f"""
<div class="progresso-wrap">
  <div class="prog-label">Progresso da contagem — {ciclo_label}</div>
  <div class="prog-bar-bg">
    <div class="prog-bar-fill" style="width:{pct}%"></div>
  </div>
  <div class="prog-nums"><span>{contados_n} produtos contados</span><span>{total - contados_n} pendentes</span></div>
</div>
""", unsafe_allow_html=True)


# ──────────────────────────────────────────────
#  CICLO 100% CONCLUÍDO
# ──────────────────────────────────────────────
if st.session_state.get("ciclo_done"):
    st.markdown(f"""
    <div class="ciclo-ok">
      <h2>🎉 Contagem de {ciclo_label} concluída!</h2>
      <p>Todos os {total} produtos foram contados. O histórico foi salvo.<br>
      O próximo ciclo começa automaticamente no mês que vem.</p>
    </div>
    """, unsafe_allow_html=True)

    # Histórico
    historico = st.session_state.get("historico", [])
    if historico:
        st.markdown('<div class="sec-titulo">📅 Histórico de contagens</div>', unsafe_allow_html=True)
        for h in historico:
            try:
                mes = datetime.strptime(h["ciclo"].replace("_done",""), "%Y-%m").strftime("%B/%Y").capitalize()
            except:
                mes = h.get("ciclo","?")
            st.markdown(f"""
            <div class="hist-card">
              <div class="hist-icone">✅</div>
              <div>
                <div class="hist-data">{mes}</div>
                <div class="hist-detalhe">Concluída em {h.get('data_conclusao','?')} · {h.get('total','?')} produtos</div>
              </div>
              <div class="hist-badge">100% ✓</div>
            </div>
            """, unsafe_allow_html=True)

    if st.button("🔄 Iniciar nova contagem agora", type="primary"):
        st.session_state["ciclo_done"] = False
        st.session_state["contados"]   = set()
        _salvar_config("contagem_ciclo_id", _CICLO_ATUAL)
        _salvar_config("contagem_contados", "[]")
        st.cache_data.clear()
        st.rerun()
    st.stop()


# ──────────────────────────────────────────────
#  LAYOUT PRINCIPAL
# ──────────────────────────────────────────────
col_left, col_right = st.columns([1.1, 1], gap="large")

# ═══════════════════════════════════════════════
#  COLUNA ESQUERDA — Seleção e ajuste
# ═══════════════════════════════════════════════
with col_left:
    st.markdown('<div class="sec-titulo">🔍 Selecionar produto</div>', unsafe_allow_html=True)

    busca = st.text_input("", placeholder="🔎  Digite o nome do produto...", label_visibility="collapsed")

    df_filtrado = df_prod.copy()
    if busca.strip():
        b = _strip(busca)
        df_filtrado = df_filtrado[df_filtrado["_nome"].apply(lambda x: b in _strip(x))]

    if df_filtrado.empty:
        st.warning("Nenhum produto encontrado.")
    else:
        opts   = df_filtrado["__key"].tolist()
        labels = {
            r["__key"]: ("✅ " if r["__key"] in st.session_state["contados"] else "") + r["_nome"]
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

        row       = df_prod[df_prod["__key"] == sel_key].iloc[0]
        prod_id   = _nz(row.get("_id",""))
        prod_nome = _nz(row.get("_nome",""))
        prod_cat  = _nz(row.get("_cat",""))
        prod_foto = _nz(row.get("_foto",""))
        est_atual = estoque_atual(sel_key)
        ja_contado = sel_key in st.session_state["contados"]

        status_badge = (
            '<span class="prod-badge-ok">✅ Já contado</span>' if ja_contado
            else '<span class="prod-badge-pend">⏳ Pendente</span>'
        )
        if prod_foto and prod_foto.startswith("http"):
            foto_html = f'<img src="{prod_foto}" class="prod-foto" onerror="this.style.display=\'none\'">'
        else:
            foto_html = '<div class="prod-foto-placeholder">📦</div>'

        st.markdown(f"""
        <div class="prod-card">
          {foto_html}
          <div class="prod-info">
            <div class="prod-nome">{prod_nome}</div>
            <div class="prod-cat">{prod_cat or "Sem categoria"}</div>
            {status_badge}
            <div class="estoque-info">
              <div class="est-item">
                <div class="est-val">{_fmt_num(est_atual)}</div>
                <div class="est-lab">Estoque atual</div>
              </div>
            </div>
          </div>
        </div>
        """, unsafe_allow_html=True)

        # ── Formulário de ajuste ──
        st.markdown('<div class="sec-titulo">✏️ Definir estoque</div>', unsafe_allow_html=True)

        _val_input = float(est_atual)
        _min_input = min(0.0, _val_input)
        alvo = st.number_input(
            "Quantidade contada (nova quantidade)",
            min_value=_min_input, step=1.0, value=_val_input,
        )
        delta = alvo - est_atual

        if delta > 0:
            st.markdown(f'<div class="delta-plus">▲ Vai adicionar {delta:.0f} unidades</div>', unsafe_allow_html=True)
        elif delta < 0:
            st.markdown(f'<div class="delta-minus">▼ Vai remover {abs(delta):.0f} unidades</div>', unsafe_allow_html=True)
        else:
            st.markdown('<div class="delta-zero">— Sem alteração no estoque</div>', unsafe_allow_html=True)

        col_a, col_b = st.columns(2)
        with col_a:
            responsavel = st.text_input("Responsável", placeholder="Seu nome")
        with col_b:
            motivo = st.text_input("Motivo", value="Contagem")

        obs = st.text_area("Observações (opcional)", placeholder="Alguma nota sobre este produto...", height=68)

        btn_salvar = st.button("💾  Salvar contagem", type="primary", use_container_width=True)

        if btn_salvar:
            data_str  = datetime.now().strftime("%d/%m/%Y")
            obs_final = f"{motivo or 'Contagem'} por {responsavel}".strip(" por").strip()
            if obs: obs_final = (obs_final + " — " + obs) if obs_final else obs

            qtd_str = str(int(delta) if float(delta).is_integer() else delta).replace(".", ",")
            try:
                ws  = _sheet().worksheet("MovimentosEstoque")
                cur = ws.row_values(1) or ["Data","IDProduto","Produto","Tipo","Qtd","Obs"]
                row_map = {
                    "Data": data_str, "IDProduto": prod_id, "Produto": prod_nome,
                    "Tipo": "Ajuste", "Qtd": qtd_str, "Obs": obs_final,
                }
                linha = [row_map.get(h, "") for h in cur]
                ws.append_row(linha, value_input_option="USER_ENTERED")

                # Marca como contado e persiste na Config
                st.session_state["contados"].add(sel_key)
                _salvar_contados(st.session_state["contados"])
                st.cache_data.clear()

                # Verificar se atingiu 100%
                if len(st.session_state["contados"]) >= total:
                    _, historico, _ = _carregar_estado()
                    st.session_state["historico"] = historico
                    _concluir_ciclo(total, st.session_state["contados"])
                    st.session_state["ciclo_done"] = True
                    st.balloons()
                    st.success("🎉 Contagem concluída! Todos os produtos foram verificados.")
                    st.rerun()
                else:
                    sinal = "+" if delta > 0 else ""
                    st.success(
                        f"✅ Ajuste salvo! {sinal}{_fmt_num(delta)} unidades → "
                        f"estoque esperado: {_fmt_num(alvo)}"
                    )
                    st.rerun()

            except Exception as e:
                st.error("Falha ao salvar.")
                st.code(str(e))


# ═══════════════════════════════════════════════
#  COLUNA DIREITA — Auditoria visual + histórico
# ═══════════════════════════════════════════════
with col_right:
    st.markdown('<div class="sec-titulo">📋 Auditoria da contagem</div>', unsafe_allow_html=True)

    filtro = st.radio(
        "Mostrar", ["Todos", "✅ Contados", "⏳ Pendentes"],
        horizontal=True, label_visibility="collapsed",
    )

    df_audit = df_prod.copy()
    if busca.strip():
        b = _strip(busca)
        df_audit = df_audit[df_audit["_nome"].apply(lambda x: b in _strip(x))]

    if filtro == "✅ Contados":
        df_audit = df_audit[df_audit["__key"].isin(st.session_state["contados"])]
    elif filtro == "⏳ Pendentes":
        df_audit = df_audit[~df_audit["__key"].isin(st.session_state["contados"])]

    cont_total = len(df_prod)
    cont_ok    = len(st.session_state["contados"])
    cont_pend  = cont_total - cont_ok

    k1, k2, k3 = st.columns(3)
    with k1:
        st.markdown(f"""<div style="background:rgba(255,255,255,0.05);border-radius:12px;padding:12px;text-align:center;border:1px solid rgba(255,255,255,0.08)">
        <div style="font-family:Nunito;font-size:1.4rem;font-weight:800;color:#fff">{cont_ok}</div>
        <div style="font-size:0.7rem;color:rgba(255,255,255,0.4);text-transform:uppercase">Contados</div></div>""", unsafe_allow_html=True)
    with k2:
        st.markdown(f"""<div style="background:rgba(255,255,255,0.05);border-radius:12px;padding:12px;text-align:center;border:1px solid rgba(255,255,255,0.08)">
        <div style="font-family:Nunito;font-size:1.4rem;font-weight:800;color:#fbbf24">{cont_pend}</div>
        <div style="font-size:0.7rem;color:rgba(255,255,255,0.4);text-transform:uppercase">Pendentes</div></div>""", unsafe_allow_html=True)
    with k3:
        st.markdown(f"""<div style="background:rgba(255,255,255,0.05);border-radius:12px;padding:12px;text-align:center;border:1px solid rgba(255,255,255,0.08)">
        <div style="font-family:Nunito;font-size:1.4rem;font-weight:800;color:#4ade80">{pct}%</div>
        <div style="font-size:0.7rem;color:rgba(255,255,255,0.4);text-transform:uppercase">Progresso</div></div>""", unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # Grade de auditoria
    if df_audit.empty:
        st.info("Nenhum produto neste filtro.")
    else:
        rows = [df_audit.iloc[i:i+3] for i in range(0, len(df_audit), 3)]
        for chunk in rows:
            cols = st.columns(3)
            for col_i, (_, prod_row) in zip(cols, chunk.iterrows()):
                with col_i:
                    chave   = prod_row["__key"]
                    nome_p  = prod_row["_nome"]
                    foto_p  = prod_row["_foto"]
                    est_p   = estoque_atual(chave)
                    is_done = chave in st.session_state["contados"]

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
                      <div style="font-size:0.65rem;color:rgba(255,255,255,0.35);margin-top:3px">Estoque: {_fmt_num(est_p)}</div>
                    </div>
                    """, unsafe_allow_html=True)

                    if st.button("Selecionar", key=f"sel_{chave}", use_container_width=True):
                        st.session_state["prod_sel"] = chave
                        st.rerun()

    # Histórico de contagens passadas
    historico = st.session_state.get("historico", [])
    if historico:
        st.markdown("<br>", unsafe_allow_html=True)
        st.markdown('<div class="sec-titulo">📅 Contagens anteriores</div>', unsafe_allow_html=True)
        for h in historico:
            try:
                mes = datetime.strptime(h["ciclo"].replace("_done",""), "%Y-%m").strftime("%B/%Y").capitalize()
            except:
                mes = h.get("ciclo","?")
            st.markdown(f"""
            <div class="hist-card">
              <div class="hist-icone">📋</div>
              <div>
                <div class="hist-data">{mes}</div>
                <div class="hist-detalhe">Concluída em {h.get('data_conclusao','?')} · {h.get('total','?')} produtos</div>
              </div>
              <div class="hist-badge">100% ✓</div>
            </div>
            """, unsafe_allow_html=True)

    # Botões de controle
    st.markdown("<br>", unsafe_allow_html=True)
    col_r1, col_r2 = st.columns(2)
    with col_r1:
        if st.button("🔄  Recarregar progresso", use_container_width=True,
                     help="Sincroniza com o que foi salvo na planilha"):
            contados, historico, ciclo_salvo = _carregar_estado()
            st.session_state["contados"]  = contados
            st.session_state["historico"] = historico
            st.session_state["ciclo_done"] = ciclo_salvo.endswith("_done")
            st.cache_data.clear()
            st.rerun()
    with col_r2:
        if st.button("⚠️  Limpar sessão", use_container_width=True,
                     help="Limpa apenas a visualização local. Os ajustes salvos na planilha permanecem."):
            st.session_state["contados"] = set()
            st.session_state["contagem_inicializada"] = False
            st.rerun()

    st.caption("ℹ️ O progresso é salvo automaticamente na planilha Config. Feche e abra o app à vontade — ao reabrir, o sistema recupera tudo que foi contado no ciclo atual.")
