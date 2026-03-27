# pages/01_Produtos.py — Catálogo de Produtos (redesenhado)
# -*- coding: utf-8 -*-
import json, unicodedata as _ud, re
import streamlit as st
import pandas as pd
import gspread
from gspread_dataframe import get_as_dataframe
from google.oauth2.service_account import Credentials
from streamlit.components.v1 import html as sthtml

# ──────────────────────────────────────────────
#  CONFIG & TEMA
# ──────────────────────────────────────────────
import pathlib
_cfg = pathlib.Path(".streamlit"); _cfg.mkdir(exist_ok=True)
(_cfg / "config.toml").write_text('[theme]\nbase = "dark"\n')

st.set_page_config(page_title="Produtos — Ebenezér", page_icon="🏪",
                   layout="wide", initial_sidebar_state="collapsed")

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

/* KPI mini */
.mini-kpi {
    background:rgba(255,255,255,0.06); border:1px solid rgba(255,255,255,0.09);
    border-radius:14px; padding:14px 18px; text-align:center;
}
.mini-kpi .val { font-family:'Nunito',sans-serif; font-size:1.4rem; font-weight:800; color:#fff; }
.mini-kpi .lbl { font-size:0.68rem; color:rgba(255,255,255,0.4); text-transform:uppercase; letter-spacing:0.5px; margin-top:2px; }

/* Filtro bar */
.filtro-bar {
    background:rgba(255,255,255,0.04); border:1px solid rgba(255,255,255,0.08);
    border-radius:14px; padding:14px 20px; margin-bottom:16px;
}
</style>
""", unsafe_allow_html=True)


# ──────────────────────────────────────────────
#  HELPERS
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

@st.cache_data(ttl=10, show_spinner=False)
def carregar_aba(nome):
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

def _to_num(x):
    if x is None: return 0.0
    s = str(x).strip()
    if s == "" or s.lower() in ("nan","none"): return 0.0
    s = s.replace("−","-")
    neg = False
    if s.startswith("(") and s.endswith(")"): s = s[1:-1]; neg = True
    s = s.replace("R$","").replace(" ","")
    if "," in s: s = s.replace(".","").replace(",",".")
    s = re.sub(r"(?<!^)-","",s); s = re.sub(r"[^0-9.\-]","",s)
    if s.count("-") > 1: s = "-" + s.replace("-","")
    if s.count(".") > 1:
        p = s.split("."); s = "".join(p[:-1]) + "." + p[-1]
    try: v = float(s)
    except: v = 0.0
    return -abs(v) if neg else v

def _nz(x):
    if x is None: return ""
    try:
        if pd.isna(x): return ""
    except: pass
    s = str(x).strip()
    return "" if s.lower() in ("nan","none") else s

def _strip_low(s):
    s = _ud.normalize("NFKD", str(s or ""))
    return "".join(ch for ch in s if _ud.category(ch) != "Mn").lower().strip()

def _norm_tipo(t):
    raw = str(t or ""); low = _strip_low(raw)
    if "fracion" in low: return "entrada" if "+" in raw else "saida" if "-" in raw else "outro"
    lowc = re.sub(r"[^a-z]","",low)
    if "entrada" in lowc or "compra" in lowc or "estorno" in lowc: return "entrada"
    if "saida" in lowc or "venda" in lowc or "baixa" in lowc: return "saida"
    if "ajuste" in lowc or "contagem" in lowc or "inventario" in lowc: return "ajuste"
    return "outro"

def _prod_key(pid, pnome):
    p = _nz(pid)
    return p if p else f"nm:{_strip_low(_nz(pnome))}"

def _brl(v):
    try:
        f = float(v)
        s = f"{abs(f):,.2f}".replace(",","X").replace(".",",").replace("X",".")
        return ("-R$ " if f < 0 else "R$ ") + s
    except: return "R$ 0,00"

def _fmt_num(v):
    try:
        f = float(v)
        return str(int(round(f))) if abs(f - round(f)) < 1e-9 else f"{f:.2f}".rstrip("0").rstrip(".")
    except: return str(v)


# ──────────────────────────────────────────────
#  CARREGAR DADOS
# ──────────────────────────────────────────────
ABA_PROD, ABA_MOV, ABA_COMP = "Produtos", "MovimentosEstoque", "Compras"

df_prod = carregar_aba(ABA_PROD)
try:    df_mov  = carregar_aba(ABA_MOV)
except: df_mov  = pd.DataFrame(columns=["Data","IDProduto","Produto","Tipo","Qtd","Obs"])
try:    df_comp = carregar_aba(ABA_COMP)
except: df_comp = pd.DataFrame(columns=["IDProduto","Qtd","Custo Unitário","Produto"])

col_id       = _first_col(df_prod, ["ID","Codigo","SKU","IDProduto"])
col_nome     = _first_col(df_prod, ["Nome","Produto","Descrição","Descricao"])
col_cat      = _first_col(df_prod, ["Categoria"])
col_forn     = _first_col(df_prod, ["Fornecedor"])
col_emin     = _first_col(df_prod, ["EstoqueMin","Estoque Mínimo"])
col_preco    = _first_col(df_prod, ["PreçoVenda","PrecoVenda","Preço"])
col_img      = _first_col(df_prod, ["Foto","Imagem","URLImagem","ImagemURL"])
col_custo_p  = _first_col(df_prod, ["CustoAtual","CustoMedio"])
col_ativo    = _first_col(df_prod, ["Ativo?","Ativo"])

if not col_nome: st.error("Aba Produtos precisa de coluna Nome/Produto."); st.stop()

# Filtrar só ativos
if col_ativo:
    df_prod = df_prod[df_prod[col_ativo].str.lower().str.strip().isin(["sim","s","1","true","yes","ativo"])]

# ── Movimentos ──
for c in ["Tipo","Qtd","IDProduto","Produto"]:
    if c not in df_mov.columns: df_mov[c] = ""

if not df_mov.empty:
    df_mov["_tnorm"] = df_mov["Tipo"].apply(_norm_tipo)
    df_mov["_qtd"]   = df_mov["Qtd"].map(_to_num)
    df_mov["__key"]  = df_mov.apply(lambda r: _prod_key(r.get("IDProduto",""), r.get("Produto","")), axis=1)
    def _sum_mov(tipo):
        m = df_mov[df_mov["_tnorm"] == tipo]
        return {} if m.empty else m.groupby("__key")["_qtd"].sum().to_dict()
    ent_map = _sum_mov("entrada"); sai_map = _sum_mov("saida"); adj_map = _sum_mov("ajuste")
else:
    ent_map = sai_map = adj_map = {}

# ── Custo ──
custo_prod_map = {}
if col_custo_p:
    tmp = df_prod.copy()
    tmp["__key"] = tmp.apply(lambda r: _prod_key(r.get(col_id,""), r.get(col_nome,"")), axis=1)
    tmp["_c"] = tmp[col_custo_p].map(_to_num)
    custo_prod_map = dict(zip(tmp["__key"], tmp["_c"]))

custo_comp_map = {}
c_pid = _first_col(df_comp, ["IDProduto","ProdutoID"])
c_cu  = _first_col(df_comp, ["Custo Unitário","Custo"])
if not df_comp.empty and c_pid and c_cu:
    df_comp["__key"] = df_comp.apply(lambda r: _prod_key(r.get(c_pid,""), r.get("Produto","")), axis=1)
    df_comp["_c"]    = df_comp[c_cu].map(_to_num)
    last = df_comp.groupby("__key", as_index=False).tail(1)
    custo_comp_map = dict(zip(last["__key"], last["_c"]))

def _custo(key):
    v = float(custo_prod_map.get(key, 0.0))
    return v if v > 0 else float(custo_comp_map.get(key, 0.0))

# ── Consolidar ──
base = df_prod.copy()
base["__key"]      = base.apply(lambda r: _prod_key(r.get(col_id,""), r.get(col_nome,"")), axis=1)
base["_id"]        = base[col_id]   if col_id   else ""
base["_nome"]      = base[col_nome]
base["_img"]       = base[col_img].astype(str).fillna("") if col_img else ""
base["_cat"]       = base[col_cat].astype(str).fillna("") if col_cat else ""
base["_forn"]      = base[col_forn].astype(str).fillna("") if col_forn else ""
base["_preco"]     = base[col_preco].map(_to_num) if col_preco else 0.0
base["_emin"]      = base[col_emin].map(_to_num)  if col_emin  else 0.0
base["_ent"]       = base["__key"].map(lambda k: float(ent_map.get(k,0)))
base["_sai"]       = base["__key"].map(lambda k: float(sai_map.get(k,0)))
base["_adj"]       = base["__key"].map(lambda k: float(adj_map.get(k,0)))
base["_estq"]      = base["_ent"] - base["_sai"] + base["_adj"]
base["_custo"]     = base["__key"].map(_custo)
base["_vtotal"]    = (base["_estq"] * base["_custo"]).round(2)
base["_baixo"]     = base["_estq"] <= base["_emin"]


# ──────────────────────────────────────────────
#  HEADER
# ──────────────────────────────────────────────
n_total  = len(base)
n_baixo  = int(base["_baixo"].sum())
vt_total = base["_vtotal"].sum()

st.markdown(f"""
<div class="page-header">
  <div>
    <h1>🏪 Catálogo de Produtos</h1>
    <div class="sub">Ebenezér Variedades · {n_total} produtos ativos</div>
  </div>
  <div class="header-badge">{"⚠️ " + str(n_baixo) + " com estoque baixo" if n_baixo else "✅ Estoque OK"}</div>
</div>
""", unsafe_allow_html=True)

# Mini KPIs
mk1, mk2, mk3, mk4 = st.columns(4)
for col_k, icon, val, lbl in [
    (mk1, "📦", str(n_total),       "Produtos ativos"),
    (mk2, "⚠️", str(n_baixo),       "Estoque baixo"),
    (mk3, "💰", _brl(vt_total),     "Valor em estoque"),
    (mk4, "🏷️", _brl(base["_preco"].mean()) if n_total else "R$ 0,00", "Preço médio venda"),
]:
    col_k.markdown(f"""
    <div class="mini-kpi">
      <div class="val">{icon} {val}</div>
      <div class="lbl">{lbl}</div>
    </div>
    """, unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)


# ──────────────────────────────────────────────
#  FILTROS
# ──────────────────────────────────────────────
fc1, fc2, fc3, fc4, fc5 = st.columns([2.5, 1.4, 1.4, 0.9, 0.9])

with fc1:
    termo = st.text_input("", placeholder="🔎  Buscar por nome, categoria, fornecedor...",
                          label_visibility="collapsed",
                          value=st.session_state.get("prod_busca",""))
    st.session_state["prod_busca"] = termo

with fc2:
    cats  = ["(todas)"] + sorted({_nz(x) for x in base["_cat"].unique() if _nz(x)})
    cat   = st.selectbox("Categoria", cats, label_visibility="collapsed")

with fc3:
    forns = ["(todos)"] + sorted({_nz(x) for x in base["_forn"].unique() if _nz(x)})
    forn  = st.selectbox("Fornecedor", forns, label_visibility="collapsed")

with fc4:
    only_low = st.toggle("⚠️ Baixo", value=False, help="Mostrar apenas produtos com estoque baixo")

with fc5:
    view_cards = st.toggle("🖼️ Cards", value=True, help="Alternar entre cards e tabela")


# ── Aplicar filtros ──
mask = pd.Series(True, index=base.index)
if termo:
    t = _strip_low(termo)
    mask &= base.apply(lambda r: t in _strip_low(
        " ".join([r["_id"], r["_nome"], r["_cat"], r["_forn"]])), axis=1)
if cat != "(todas)":
    mask &= base["_cat"].astype(str) == cat
if forn != "(todos)":
    mask &= base["_forn"].astype(str) == forn
if only_low:
    mask &= base["_baixo"]

dfv = base[mask].copy().sort_values("_nome", na_position="last")
st.caption(f"Mostrando **{len(dfv)}** de {n_total} produtos")


# ──────────────────────────────────────────────
#  EXIBIÇÃO
# ──────────────────────────────────────────────
PLACEHOLDER = "https://res.cloudinary.com/db8ipmete/image/upload/v1752463905/Logo_sal%C3%A3o_kz9y9c.png"

if view_cards:
    # ── Grade de cards via HTML componente ──
    cards_html = []
    for _, r in dfv.iterrows():
        nome   = _nz(r["_nome"])
        pid    = _nz(r["_id"])
        img    = _nz(r["_img"]) or PLACEHOLDER
        estq   = r["_estq"]
        emin   = r["_emin"]
        baixo  = r["_baixo"]
        preco  = r["_preco"]
        custo  = r["_custo"]
        vtot   = r["_vtotal"]
        cat_t  = _nz(r["_cat"])
        forn_t = _nz(r["_forn"])

        estq_fmt = _fmt_num(estq)
        badge_cor = "#ef4444" if baixo else "#22c55e"
        badge_bg  = "rgba(239,68,68,0.15)" if baixo else "rgba(34,197,94,0.12)"
        badge_ico = "⚠️" if baixo else "✅"

        margem_raw = ((preco - custo) / preco * 100) if preco > 0 and custo > 0 else None
        margem_txt = f"{margem_raw:.0f}%" if margem_raw is not None else ""
        margem_cor = "#4ade80" if (margem_raw or 0) >= 30 else ("#fbbf24" if (margem_raw or 0) >= 0 else "#f87171")

        tags = " ".join([
            f'<span style="background:rgba(255,255,255,0.08);border-radius:6px;padding:2px 7px;font-size:.68rem;color:#aaa">{x}</span>'
            for x in [cat_t, forn_t] if x
        ])

        cards_html.append(f"""
        <div class="card {"card-low" if baixo else ""}">
          <div class="img-wrap">
            <img src="{img}" alt="{nome}" onerror="this.src='{PLACEHOLDER}'">
            <span class="est-badge" style="background:{badge_bg};color:{badge_cor};border:1px solid {badge_cor}40">
              {badge_ico} {estq_fmt}
            </span>
          </div>
          <div class="body">
            <div class="tags">{tags}</div>
            <div class="nome">{nome}</div>
            <div style="display:flex;justify-content:space-between;align-items:flex-end;margin-top:8px">
              <div>
                <div class="preco">{"R$ " + _fmt_num(preco).replace(".",",") if preco else "—"}</div>
                <div class="custo">Custo: {"R$ " + _fmt_num(custo).replace(".",",") if custo else "—"}</div>
              </div>
              {"<div class='margem' style='color:" + margem_cor + "'>" + margem_txt + "<br><span style='font-size:.6rem;color:#666'>margem</span></div>" if margem_txt else ""}
            </div>
            <div class="vtotal">Estoque val.: {"R$ " + _fmt_num(vtot).replace(".",",") if vtot else "—"}</div>
          </div>
        </div>
        """)

    full_html = f"""
    <!DOCTYPE html>
    <html>
    <head>
    <style>
      * {{ box-sizing: border-box; margin: 0; padding: 0; }}
      body {{ background: transparent; font-family: 'DM Sans', sans-serif; color: #fff; padding: 4px; }}
      .grid {{
        display: grid;
        grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
        gap: 14px;
      }}
      .card {{
        background: rgba(255,255,255,0.05);
        border: 1px solid rgba(255,255,255,0.1);
        border-radius: 16px; overflow: hidden;
        transition: transform 0.15s, box-shadow 0.15s;
      }}
      .card:hover {{
        transform: translateY(-3px);
        box-shadow: 0 8px 28px rgba(0,0,0,0.4);
        border-color: rgba(255,255,255,0.2);
      }}
      .card-low {{
        border-color: rgba(239,68,68,0.35) !important;
        background: rgba(239,68,68,0.04) !important;
      }}
      .img-wrap {{
        position: relative; width: 100%; height: 180px;
        background: rgba(255,255,255,0.04);
      }}
      .img-wrap img {{
        width: 100%; height: 100%; object-fit: contain; padding: 8px;
      }}
      .est-badge {{
        position: absolute; bottom: 8px; right: 8px;
        border-radius: 8px; padding: 3px 8px;
        font-size: .7rem; font-weight: 700;
      }}
      .body {{ padding: 12px; }}
      .tags {{ margin-bottom: 6px; display: flex; flex-wrap: wrap; gap: 4px; }}
      .nome {{
        font-weight: 700; font-size: .88rem; line-height: 1.3; color: #fff;
        display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical; overflow: hidden;
      }}
      .preco {{ font-size: 1.05rem; font-weight: 800; color: #4ade80; margin-top: 2px; }}
      .custo {{ font-size: .72rem; color: rgba(255,255,255,0.35); margin-top: 1px; }}
      .margem {{ font-size: .82rem; font-weight: 800; text-align: right; }}
      .vtotal {{ font-size: .7rem; color: rgba(255,255,255,0.3); margin-top: 6px;
                 border-top: 1px solid rgba(255,255,255,0.06); padding-top: 6px; }}
    </style>
    </head>
    <body>
      <div class="grid">
        {"".join(cards_html)}
      </div>
    </body>
    </html>
    """

    altura = max(600, (len(dfv) // 5 + 2) * 320)
    sthtml(full_html, height=min(altura, 2400), scrolling=True)

else:
    # ── Tabela ──
    df_show = dfv[["_id","_nome","_cat","_forn","_estq","_emin","_preco","_custo","_vtotal"]].copy()
    df_show.columns = ["ID","Produto","Categoria","Fornecedor","Estoque","Est. Mín","Preço Venda","Custo","Val. Estoque"]
    df_show["Preço Venda"]  = df_show["Preço Venda"].apply(_brl)
    df_show["Custo"]        = df_show["Custo"].apply(_brl)
    df_show["Val. Estoque"] = df_show["Val. Estoque"].apply(_brl)
    df_show["Estoque"]      = df_show["Estoque"].apply(_fmt_num)
    st.dataframe(df_show, use_container_width=True, hide_index=True)

    st.download_button(
        "⬇️ Exportar CSV",
        dfv[["_id","_nome","_cat","_forn","_estq","_emin","_preco","_custo","_vtotal"]]
           .rename(columns={"_id":"ID","_nome":"Produto","_cat":"Categoria","_forn":"Fornecedor",
                             "_estq":"Estoque","_emin":"EstMin","_preco":"PrecoVenda",
                             "_custo":"Custo","_vtotal":"ValEstoque"})
           .to_csv(index=False).encode("utf-8"),
        file_name="produtos.csv", mime="text/csv", use_container_width=True,
    )

# ── Rodapé ──
st.caption("• EstoqueAtual = Entradas − Saídas ± Ajustes (aba MovimentosEstoque) · CustoAtual usa Produtos.CustoAtual, fallback última compra")
