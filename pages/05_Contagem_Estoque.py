# pages/05_Contagem_Estoque.py — Contagem de estoque redesenhada
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

/* Header */
.page-header {
    background: linear-gradient(135deg, #1a1a2e 0%, #16213e 60%, #0f3460 100%);
    border-radius: 20px;
    padding: 24px 32px;
    margin-bottom: 24px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    box-shadow: 0 8px 32px rgba(15,52,96,0.25);
}
.page-header h1 {
    font-family: 'Nunito', sans-serif;
    font-weight: 900; font-size: 1.7rem;
    color: #fff; margin: 0;
}
.page-header .sub { font-size: 0.82rem; color: rgba(255,255,255,0.5); margin-top: 4px; }
.header-badge {
    background: rgba(255,255,255,0.1);
    border: 1px solid rgba(255,255,255,0.2);
    border-radius: 50px; padding: 8px 18px;
    color: #fff; font-size: 0.82rem; font-weight: 600;
    backdrop-filter: blur(10px);
}

/* Progress bar contagem */
.progresso-wrap {
    background: rgba(255,255,255,0.06);
    border-radius: 16px; padding: 16px 22px;
    border: 1px solid rgba(255,255,255,0.09);
    margin-bottom: 22px;
}
.prog-label { font-size: 0.78rem; color: rgba(255,255,255,0.5); font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 8px; }
.prog-bar-bg { background: rgba(255,255,255,0.1); border-radius: 100px; height: 10px; overflow: hidden; }
.prog-bar-fill { height: 10px; border-radius: 100px; background: linear-gradient(90deg, #4ade80, #22d3ee); transition: width 0.4s ease; }
.prog-nums { display: flex; justify-content: space-between; margin-top: 6px; font-size: 0.8rem; color: rgba(255,255,255,0.6); }

/* Card do produto selecionado */
.prod-card {
    background: rgba(255,255,255,0.06);
    border-radius: 20px; padding: 20px;
    border: 1px solid rgba(255,255,255,0.1);
    margin-bottom: 20px;
    display: flex; gap: 20px; align-items: center;
}
.prod-foto {
    width: 90px; height: 90px; border-radius: 14px;
    object-fit: contain; background: rgba(255,255,255,0.08);
    border: 1px solid rgba(255,255,255,0.1);
    flex-shrink: 0;
}
.prod-foto-placeholder {
    width: 90px; height: 90px; border-radius: 14px;
    background: rgba(255,255,255,0.06); border: 1px solid rgba(255,255,255,0.1);
    display: flex; align-items: center; justify-content: center;
    font-size: 2.2rem; flex-shrink: 0;
}
.prod-info { flex: 1; }
.prod-nome { font-family: 'Nunito', sans-serif; font-weight: 800; font-size: 1.05rem; color: #fff; margin-bottom: 4px; }
.prod-cat  { font-size: 0.78rem; color: rgba(255,255,255,0.45); margin-bottom: 8px; }
.prod-badge-ok { display: inline-flex; align-items: center; gap: 5px; background: rgba(74,222,128,0.15); border: 1px solid rgba(74,222,128,0.3); color: #4ade80; border-radius: 8px; padding: 4px 10px; font-size: 0.75rem; font-weight: 700; }
.prod-badge-pend { display: inline-flex; align-items: center; gap: 5px; background: rgba(251,191,36,0.12); border: 1px solid rgba(251,191,36,0.3); color: #fbbf24; border-radius: 8px; padding: 4px 10px; font-size: 0.75rem; font-weight: 700; }
.estoque-info { display: flex; gap: 16px; margin-top: 10px; }
.est-item { background: rgba(255,255,255,0.06); border-radius: 10px; padding: 8px 14px; text-align: center; }
.est-val  { font-family: 'Nunito', sans-serif; font-size: 1.2rem; font-weight: 800; color: #fff; }
.est-lab  { font-size: 0.68rem; color: rgba(255,255,255,0.4); text-transform: uppercase; letter-spacing: 0.5px; }

/* Auditoria — grade de produtos */
.audit-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(200px, 1fr)); gap: 12px; margin-top: 12px; }
.audit-item {
    background: rgba(255,255,255,0.04);
    border: 1px solid rgba(255,255,255,0.08);
    border-radius: 14px; padding: 12px;
    cursor: pointer; transition: all 0.15s;
    position: relative;
}
.audit-item:hover { background: rgba(255,255,255,0.08); border-color: rgba(255,255,255,0.2); }
.audit-item.done { border-color: rgba(74,222,128,0.4); background: rgba(74,222,128,0.06); }
.audit-thumb { width: 100%; height: 70px; object-fit: contain; border-radius: 8px; background: rgba(255,255,255,0.06); margin-bottom: 8px; }
.audit-thumb-ph { width: 100%; height: 70px; border-radius: 8px; background: rgba(255,255,255,0.06); display: flex; align-items: center; justify-content: center; font-size: 1.5rem; margin-bottom: 8px; }
.audit-nome { font-size: 0.78rem; font-weight: 700; color: rgba(255,255,255,0.85); line-height: 1.3; }
.audit-status { position: absolute; top: 8px; right: 8px; }
.checkmark { background: rgba(74,222,128,0.9); color: #000; border-radius: 50%; width: 20px; height: 20px; display: flex; align-items: center; justify-content: center; font-size: 0.65rem; font-weight: 900; }
.audit-estoque { font-size: 0.72rem; color: rgba(255,255,255,0.4); margin-top: 4px; }

/* Secao titulo */
.sec-titulo {
    font-family: 'Nunito', sans-serif; font-weight: 800;
    font-size: 1.05rem; color: rgba(255,255,255,0.9);
    margin: 24px 0 12px 0; display: flex; align-items: center; gap: 8px;
}
.sec-titulo::after {
    content: ''; flex: 1; height: 1px;
    background: linear-gradient(to right, rgba(255,255,255,0.15), transparent);
    margin-left: 8px; border-radius: 2px;
}

/* Delta badge */
.delta-plus  { color: #4ade80; font-weight: 700; font-size: 0.9rem; }
.delta-minus { color: #f87171; font-weight: 700; font-size: 0.9rem; }
.delta-zero  { color: rgba(255,255,255,0.4); font-size: 0.9rem; }

/* Filtro barra */
.filtro-bar {
    background: rgba(255,255,255,0.04);
    border: 1px solid rgba(255,255,255,0.08);
    border-radius: 14px; padding: 14px 18px;
    margin-bottom: 16px; display: flex; gap: 12px; align-items: center;
}

button[kind="primary"] { border-radius: 12px !important; font-weight: 700 !important; }
</style>
""", unsafe_allow_html=True)


# ──────────────────────────────────────────────
#  HELPERS GOOGLE SHEETS
# ──────────────────────────────────────────────
def _normalize_private_key(key):
    if not isinstance(key, str): return key
    key = key.replace("\\n", "\n")
    key = "".join(ch for ch in key if _ud.category(ch)[0] != "C" or ch in ("\n","\r","\t"))
    return key

def _load_sa():
    svc = st.secrets.get("GCP_SERVICE_ACCOUNT")
    if svc is None: st.error("🛑 GCP_SERVICE_ACCOUNT ausente."); st.stop()
    if isinstance(svc, str): svc = json.loads(svc)
    svc = dict(svc); svc["private_key"] = _normalize_private_key(svc["private_key"])
    return svc

@st.cache_resource
def _sheet():
    scopes = ["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(_load_sa(), scopes=scopes)
    gc = gspread.authorize(creds)
    url = st.secrets.get("PLANILHA_URL")
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

def _to_num(x):
    if x is None: return 0.0
    s = str(x).strip()
    if s == "" or s.lower() in ("nan","none"): return 0.0
    s = s.replace("−", "-")
    neg = False
    if s.startswith("(") and s.endswith(")"): s = s[1:-1]; neg = True
    s = s.replace("R$","").replace(" ","")
    if "," in s: s = s.replace(".","").replace(",",".")
    s = re.sub(r"(?<!^)-","",s); s = re.sub(r"[^0-9.\-]","",s)
    if s.count("-") > 1: s = "-" + s.replace("-","")
    if s.count(".") > 1:
        p = s.split(".")
        s = "".join(p[:-1]) + "." + p[-1]
    try: v = float(s)
    except: v = 0.0
    return -abs(v) if neg else v

def _norm_tipo(t):
    raw = str(t or ""); low = _strip(raw)
    if "fracion" in low: return "entrada" if "+" in raw else "saida" if "-" in raw else "outro"
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

df_prod["_id"]    = df_prod[col_id]   if col_id   else ""
df_prod["_nome"]  = df_prod[col_nome] if col_nome else ""
df_prod["_cat"]   = df_prod[col_cat]  if col_cat  else ""
df_prod["_foto"]  = df_prod[col_foto] if col_foto else ""
df_prod["__key"]  = df_prod.apply(lambda r: _prod_key(r.get(col_id,""), r.get(col_nome,"")), axis=1)
df_prod.reset_index(drop=True, inplace=True)

# Pré-processar movimentos
for c in ["Tipo","Qtd","IDProduto","Produto"]:
    if c not in df_mov.columns: df_mov[c] = ""

if not df_mov.empty:
    df_mov["_tnorm"] = df_mov["Tipo"].apply(_norm_tipo)
    df_mov["_qtd"]   = df_mov["Qtd"].map(_to_num)
    df_mov["__key"]  = df_mov.apply(lambda r: _prod_key(r.get("IDProduto",""), r.get("Produto","")), axis=1)
else:
    df_mov["_tnorm"] = []
    df_mov["_qtd"]   = []
    df_mov["__key"]  = []

def estoque_atual(ch):
    if df_mov.empty: return 0.0
    g = df_mov[df_mov["__key"] == ch].groupby("_tnorm")["_qtd"].sum()
    return float(g.get("entrada",0.0) - g.get("saida",0.0) + g.get("ajuste",0.0))


# ──────────────────────────────────────────────
#  SESSION STATE — auditoria (quais já contados)
# ──────────────────────────────────────────────
if "contados" not in st.session_state:
    st.session_state["contados"] = set()
if "prod_sel" not in st.session_state:
    st.session_state["prod_sel"] = df_prod["__key"].iloc[0] if not df_prod.empty else None
if "filtro_audit" not in st.session_state:
    st.session_state["filtro_audit"] = "Todos"


# ──────────────────────────────────────────────
#  HEADER
# ──────────────────────────────────────────────
total = len(df_prod)
contados_n = len(st.session_state["contados"])
pct = int(contados_n / total * 100) if total else 0

st.markdown(f"""
<div class="page-header">
  <div>
    <h1>📦 Contagem de Estoque</h1>
    <div class="sub">Ebenezér Variedades · {datetime.now().strftime("%d/%m/%Y")}</div>
  </div>
  <div class="header-badge">{contados_n} de {total} contados</div>
</div>
""", unsafe_allow_html=True)

# Barra de progresso
st.markdown(f"""
<div class="progresso-wrap">
  <div class="prog-label">Progresso da contagem</div>
  <div class="prog-bar-bg">
    <div class="prog-bar-fill" style="width:{pct}%"></div>
  </div>
  <div class="prog-nums"><span>{contados_n} produtos contados</span><span>{total - contados_n} pendentes</span></div>
</div>
""", unsafe_allow_html=True)


# ──────────────────────────────────────────────
#  LAYOUT: coluna esquerda (seleção/ajuste) + direita (auditoria)
# ──────────────────────────────────────────────
col_left, col_right = st.columns([1.1, 1], gap="large")

# ═══════════════════════════════════════════════
#  COLUNA ESQUERDA — Seleção e ajuste
# ═══════════════════════════════════════════════
with col_left:
    st.markdown('<div class="sec-titulo">🔍 Selecionar produto</div>', unsafe_allow_html=True)

    # Busca por texto
    busca = st.text_input("", placeholder="🔎  Digite o nome do produto...", label_visibility="collapsed")

    # Filtrar lista
    df_filtrado = df_prod.copy()
    if busca.strip():
        b = _strip(busca)
        df_filtrado = df_filtrado[df_filtrado["_nome"].apply(lambda x: b in _strip(x))]

    if df_filtrado.empty:
        st.warning("Nenhum produto encontrado.")
    else:
        opts = df_filtrado["__key"].tolist()
        labels = {
            r["__key"]: ("✅ " if r["__key"] in st.session_state["contados"] else "") + r["_nome"]
            for _, r in df_filtrado.iterrows()
        }

        # Manter seleção válida
        sel = st.session_state["prod_sel"]
        idx = opts.index(sel) if sel in opts else 0

        sel_key = st.selectbox(
            "Produto",
            options=opts,
            format_func=lambda k: labels.get(k, k),
            index=idx,
            label_visibility="collapsed",
        )
        st.session_state["prod_sel"] = sel_key

        # Dados do produto selecionado
        row = df_prod[df_prod["__key"] == sel_key].iloc[0]
        prod_id   = _nz(row.get("_id",""))
        prod_nome = _nz(row.get("_nome",""))
        prod_cat  = _nz(row.get("_cat",""))
        prod_foto = _nz(row.get("_foto",""))
        est_atual = estoque_atual(sel_key)
        ja_contado = sel_key in st.session_state["contados"]

        # Card visual do produto
        status_badge = (
            '<span class="prod-badge-ok">✅ Já contado</span>'
            if ja_contado else
            '<span class="prod-badge-pend">⏳ Pendente</span>'
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
                <div class="est-val">{int(est_atual) if float(est_atual).is_integer() else est_atual}</div>
                <div class="est-lab">Estoque atual</div>
              </div>
            </div>
          </div>
        </div>
        """, unsafe_allow_html=True)

        # ── Formulário de ajuste ──
        st.markdown('<div class="sec-titulo">✏️ Definir estoque</div>', unsafe_allow_html=True)

        alvo = st.number_input(
            "Quantidade contada (nova quantidade)",
            min_value=0.0, step=1.0,
            value=float(est_atual),
        )
        delta = alvo - est_atual

        # Mostrar delta visual
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

        # Botão salvar
        btn_salvar = st.button("💾  Salvar contagem", type="primary", use_container_width=True)

        if btn_salvar:
            if delta == 0:
                st.success("✅ Estoque já está correto — marcado como contado!")
                st.session_state["contados"].add(sel_key)
            else:
                data_str = datetime.now().strftime("%d/%m/%Y")
                qtd_str  = str(delta).replace(".", ",")
                obs_final = f"{motivo or 'Contagem'} por {responsavel}".strip(" por").strip()
                if obs: obs_final = (obs_final + " — " + obs) if obs_final else obs
                try:
                    ws = _sheet().worksheet("MovimentosEstoque")
                    cur = ws.row_values(1) or ["Data","IDProduto","Produto","Tipo","Qtd","Obs"]
                    row_map = {"Data": data_str, "IDProduto": prod_id, "Produto": prod_nome,
                               "Tipo": "Ajuste", "Qtd": qtd_str, "Obs": obs_final}
                    linha = [row_map.get(h,"") for h in cur]
                    ws.append_row(linha, value_input_option="USER_ENTERED")
                    st.session_state["contados"].add(sel_key)
                    st.cache_data.clear()
                    sinal = "+" if delta > 0 else ""
                    st.success(f"✅ Ajuste salvo! {sinal}{delta:.0f} unidades → estoque esperado: {alvo:.0f}")
                except Exception as e:
                    st.error("Falha ao salvar.")
                    st.code(str(e))


# ═══════════════════════════════════════════════
#  COLUNA DIREITA — Auditoria visual
# ═══════════════════════════════════════════════
with col_right:
    st.markdown('<div class="sec-titulo">📋 Auditoria da contagem</div>', unsafe_allow_html=True)

    # Filtros rápidos
    filtro = st.radio(
        "Mostrar",
        ["Todos", "✅ Contados", "⏳ Pendentes"],
        horizontal=True,
        label_visibility="collapsed",
    )

    # Filtrar por busca também
    df_audit = df_prod.copy()
    if busca.strip():
        b = _strip(busca)
        df_audit = df_audit[df_audit["_nome"].apply(lambda x: b in _strip(x))]

    if filtro == "✅ Contados":
        df_audit = df_audit[df_audit["__key"].isin(st.session_state["contados"])]
    elif filtro == "⏳ Pendentes":
        df_audit = df_audit[~df_audit["__key"].isin(st.session_state["contados"])]

    cont_total   = len(df_prod)
    cont_ok      = len(st.session_state["contados"])
    cont_pend    = cont_total - cont_ok

    # Mini KPIs
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

    # Grade de auditoria — clicável para selecionar produto
    if df_audit.empty:
        st.info("Nenhum produto neste filtro.")
    else:
        # Renderizar em grade de 3 colunas
        rows = [df_audit.iloc[i:i+3] for i in range(0, len(df_audit), 3)]
        for chunk in rows:
            cols = st.columns(3)
            for col_i, (_, prod_row) in zip(cols, chunk.iterrows()):
                with col_i:
                    chave    = prod_row["__key"]
                    nome_p   = prod_row["_nome"]
                    foto_p   = prod_row["_foto"]
                    est_p    = estoque_atual(chave)
                    is_done  = chave in st.session_state["contados"]

                    borda = "rgba(74,222,128,0.4)" if is_done else "rgba(255,255,255,0.08)"
                    bg    = "rgba(74,222,128,0.05)" if is_done else "rgba(255,255,255,0.03)"
                    check = "✅" if is_done else ""

                    if foto_p and foto_p.startswith("http"):
                        foto_tag = f'<img src="{foto_p}" style="width:100%;height:60px;object-fit:contain;border-radius:8px;background:rgba(255,255,255,0.06);margin-bottom:6px" onerror="this.style.display=\'none\'">'
                    else:
                        foto_tag = '<div style="width:100%;height:60px;border-radius:8px;background:rgba(255,255,255,0.06);display:flex;align-items:center;justify-content:center;font-size:1.4rem;margin-bottom:6px">📦</div>'

                    st.markdown(f"""
                    <div style="background:{bg};border:1px solid {borda};border-radius:12px;padding:10px;margin-bottom:4px">
                      {foto_tag}
                      <div style="font-size:0.72rem;font-weight:700;color:rgba(255,255,255,0.85);line-height:1.3">{check} {nome_p}</div>
                      <div style="font-size:0.65rem;color:rgba(255,255,255,0.35);margin-top:3px">Estoque: {int(est_p) if float(est_p).is_integer() else est_p}</div>
                    </div>
                    """, unsafe_allow_html=True)

                    if st.button("Selecionar", key=f"sel_{chave}", use_container_width=True):
                        st.session_state["prod_sel"] = chave
                        st.rerun()

    # Botão limpar auditoria
    st.markdown("<br>", unsafe_allow_html=True)
    if st.button("🔄  Reiniciar contagem (limpar auditoria)", use_container_width=True):
        st.session_state["contados"] = set()
        st.rerun()
