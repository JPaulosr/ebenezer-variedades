# pages/01_produtos.py — Catálogo de Produtos (estoque via MovimentosEstoque) — versão com Cards de Fotos (HTML componente)
# -*- coding: utf-8 -*-
import json, unicodedata as _ud, re
import streamlit as st
import pandas as pd
import gspread
from gspread_dataframe import get_as_dataframe
from google.oauth2.service_account import Credentials

st.set_page_config(page_title="Produtos — Ebenezér Variedades", page_icon="📦", layout="wide")
st.markdown("<h1 style='margin:0 0 .4rem 0'>📦 Produtos — Catálogo & Busca</h1>", unsafe_allow_html=True)

# =========================
# Estilos (dark-friendly)
# =========================
st.markdown("""
<style>
:root{
  --card-bg: rgba(255,255,255,.04);
  --card-bd: rgba(255,255,255,.12);
  --muted: rgba(255,255,255,.65);
  --fg: rgba(255,255,255,.92);
  --accent: #7c3aed;
  --ok:#22c55e; --warn:#f59e0b; --err:#ef4444; --info:#06b6d4;
}
@media (prefers-color-scheme: light){
  :root{
    --card-bg: #ffffff;
    --card-bd: #e5e7eb;
    --muted: #6b7280;
    --fg: #111827;
  }
}
.card-grid{
  display:grid;
  grid-template-columns: repeat(auto-fill, minmax(240px, 1fr));
  gap: 16px;
  margin-top:.25rem;
}
.card{
  background: var(--card-bg);
  border: 1px solid var(--card-bd);
  border-radius: 16px;
  overflow: hidden;
  display:flex; flex-direction:column;
  transition: transform .08s ease, box-shadow .08s ease, border-color .1s ease;
}
.card:hover{ transform: translateY(-2px); box-shadow: 0 8px 24px rgba(0,0,0,.18); border-color: rgba(124,58,237,.45) }
.card-img{
  width:100%; aspect-ratio: 4 / 3; object-fit: cover; background:#111; display:block;
}
.card-body{ padding: 12px 14px; color: var(--fg); }
.card-title{
  font-weight: 700; font-size: 1rem; line-height: 1.3; margin: 0 0 4px 0;
  display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical; overflow: hidden;
}
.card-sub{ font-size:.85rem; color: var(--muted); margin-bottom: 8px; display:flex; gap:8px; flex-wrap:wrap }
.badge{
  display:inline-flex; align-items:center; gap:6px; font-size:.75rem; padding:.2rem .5rem; border-radius:999px;
  border:1px solid var(--card-bd); color:var(--muted);
}
.badge.warn{ border-color: rgba(245,158,11,.4); color:#f59e0b; }
.badge.info{ border-color: rgba(6,182,212,.4); color:#06b6d4; }
.badge.ok{ border-color: rgba(34,197,94,.4); color:#22c55e; }
.badge.err{ border-color: rgba(239,68,68,.4); color:#ef4444; }
.price-row{
  display:flex; align-items:baseline; justify-content:space-between; margin-top: 6px;
}
.price{ font-size:1.05rem; font-weight:800 }
.meta{ font-size:.8rem; color: var(--muted) }
.kpis{
  display:flex; gap:8px; flex-wrap:wrap; margin-top:8px;
}
.kpi{
  font-variant-numeric: tabular-nums; font-size:.78rem; padding:.25rem .5rem; border-radius:8px; border:1px dashed var(--card-bd); color:var(--muted)
}
.hr{ height:1px; background:var(--card-bd); margin:8px 0 }
</style>
""", unsafe_allow_html=True)

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
    """Converte string/num para float preservando sinal negativo."""
    if x is None: return 0.0
    s = str(x).strip()
    if s == "" or s.lower() in ("nan","none"): return 0.0
    s = s.replace("−", "-")
    neg_paren = False
    if s.startswith("(") and s.endswith(")"):
        s = s[1:-1]; neg_paren = True
    s = s.replace("R$", "").replace(" ", "")
    if "," in s:
        s = s.replace(".", "").replace(",", ".")
    s = re.sub(r"(?<!^)-", "", s)
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
    except:
        return str(v)

# =========================
# Abas
# =========================
ABA_PRODUTOS = "Produtos"
ABA_MOV      = "MovimentosEstoque"
ABA_COMPRAS  = "Compras"

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
col_id        = _first_col(df_prod, ["ID","Id","Codigo","Código","SKU"])
col_nome      = _first_col(df_prod, ["Nome","Produto","Descrição","Descricao"])
col_cat       = _first_col(df_prod, ["Categoria"])
col_forn      = _first_col(df_prod, ["Fornecedor"])
col_estq_min  = _first_col(df_prod, ["EstoqueMin","Estoque Mínimo","EstqMin","Estoque Mínimo (un)"])
col_preco     = _first_col(df_prod, ["PreçoVenda","PrecoVenda","Preço","Preco","Preço Venda"])
col_img       = _first_col(df_prod, ["Imagem","Foto","URLImagem","LinkImagem","ImagemURL","FotoURL"])

if not col_nome:
    st.error("Aba **Produtos** precisa ter uma coluna de nome (ex.: Nome/Produto)."); st.stop()

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
# Compras → custo atual (última)
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
# Consolidação
# =========================
df = base[["__key","IDProduto","Produto","ImagemURL"]].copy()
def _get(m, k): return float(m.get(k, 0.0))

df["Entradas"]     = df["__key"].apply(lambda k: _get(entradas_mov, k))
df["Saidas"]       = df["__key"].apply(lambda k: _get(saidas_mov,   k))
df["Ajustes"]      = df["__key"].apply(lambda k: _get(ajustes_mov,  k))
df["EstoqueAtual"] = df["Entradas"] - df["Saidas"] + df["Ajustes"]
df["CustoAtual"]   = df["__key"].apply(lambda k: float(custo_atual_map.get(k, 0.0)))
df["ValorTotal"]   = (df["EstoqueAtual"] * df["CustoAtual"]).round(2)

extra_cols = [c for c in [col_id, col_nome, col_cat, col_forn, col_preco, col_estq_min] if c]
df = df.merge(df_prod[extra_cols], left_on="IDProduto",
              right_on=col_id if col_id else col_nome, how="left")

# =========================
# Filtros
# =========================
col_filtros = st.columns([2.2, 1.4, 1.3, 1.2, 1.1])
with col_filtros[0]:
    termo = st.text_input("🔎 Buscar", placeholder="ID, nome, fornecedor, categoria...").strip()
with col_filtros[1]:
    if col_cat and col_cat in df.columns:
        cats = ["(todas)"] + sorted(pd.Series(df[col_cat].dropna().astype(str).unique()).tolist())
        cat = st.selectbox("Categoria", cats)
    else:
        cat = "(todas)"
with col_filtros[2]:
    if col_forn and col_forn in df.columns:
        forns = ["(todos)"] + sorted(pd.Series(df[col_forn].dropna().astype(str).unique()).tolist())
        forn = st.selectbox("Fornecedor", forns)
    else:
        forn = "(todos)"
with col_filtros[3]:
    only_low = st.checkbox("⚠️ Baixo estoque", value=False, help="Itens com EstoqueAtual ≤ Estoque Mínimo (se existir).")
with col_filtros[4]:
    view_cards = st.toggle("🖼️ Cards", value=True, help="Alterna entre cards e tabela")

mask = pd.Series(True, index=df.index)
if termo:
    t = termo.lower()
    mask &= df.apply(
        lambda r: t in " ".join([str(x).lower() for x in [
            r.get("IDProduto",""), r.get("Produto",""), r.get(col_cat,""), r.get(col_forn,"")
        ]]),
        axis=1
    )
if col_cat and cat != "(todas)" and col_cat in df.columns:
    mask &= (df[col_cat].astype(str) == cat)
if col_forn and forn != "(todos)" and col_forn in df.columns:
    mask &= (df[col_forn].astype(str) == forn)

if only_low and col_estq_min and col_estq_min in df.columns:
    estq_min_series = df[col_estq_min].map(_to_num).fillna(0)
    mask &= (df["EstoqueAtual"] <= estq_min_series)

dfv = df[mask].copy()

# =========================
# Exibição — Cards via HTML componente
# =========================
PLACEHOLDER = "https://res.cloudinary.com/db8ipmete/image/upload/v1752463905/Logo_sal%C3%A3o_kz9y9c.png"

if view_cards:
    st.caption(f"{len(dfv)} item(ns) encontrado(s).")
    try:
        dfv = dfv.sort_values("Produto", na_position="last")
    except:
        pass

    cards_html_parts = ['<div class="card-grid">']
    for _, r in dfv.iterrows():
        nome   = _nz(r.get("Produto",""))
        pid    = _nz(r.get("IDProduto",""))
        cat_   = _nz(r.get(col_cat,"")) if col_cat else ""
        forn_  = _nz(r.get(col_forn,"")) if col_forn else ""
        preco  = r.get(col_preco, "")
        preco_ = _fmt_money_br(_to_num(preco)) if str(preco) != "" else ""
        img    = _nz(r.get("ImagemURL","")) or PLACEHOLDER

        ent    = _fmt_num(r.get("Entradas",0))
        sai    = _fmt_num(r.get("Saidas",0))
        aj     = _fmt_num(r.get("Ajustes",0))
        estq   = _to_num(r.get("EstoqueAtual",0))
        estq_s = _fmt_num(estq)
        custo  = _fmt_money_br(r.get("CustoAtual",0))
        vtot   = _fmt_money_br(r.get("ValorTotal",0))

        badge_low = ""
        if col_estq_min and col_estq_min in dfv.columns:
            try:
                estq_min = _to_num(r.get(col_estq_min, 0))
                if estq <= estq_min:
                    badge_low = '<span class="badge warn">⚠️ Baixo estoque</span>'
            except:
                pass

        subs = []
        if cat_: subs.append(f"🏷️ {cat_}")
        if forn_: subs.append(f"🚚 {forn_}")
        subs_html = " • ".join(subs) if subs else ""

        kpis = f'''
          <div class="kpis">
            <div class="kpi">⬆️ Entradas: <b>{ent}</b></div>
            <div class="kpi">⬇️ Saídas: <b>{sai}</b></div>
            <div class="kpi">♻️ Ajustes: <b>{aj}</b></div>
          </div>
        '''

        price_row = f'''
          <div class="price-row">
            <div>
              <div class="price">{preco_ if preco_ else ""}</div>
              <div class="meta">{custo} custo • {vtot} total</div>
            </div>
            <div class="badge {'ok' if estq>0 else 'err'}">📦 Estoque: <b>{estq_s}</b></div>
          </div>
        '''

        cards_html_parts.append(f"""
<div class="card">
  <img class="card-img" src="{img}" alt="{nome}">
  <div class="card-body">
    <div class="card-title">{nome}</div>
    <div class="card-sub">
      {'<span class="badge info">#'+pid+'</span>' if pid else ''}
      {badge_low}
    </div>
    <div class="meta">{subs_html}</div>
    <div class="hr"></div>
    {kpis}
    {price_row}
  </div>
</div>
""")
    cards_html_parts.append("</div>")
    html_cards = "".join(cards_html_parts)

    # Renderiza como HTML real (não markdown)
    try:
        # Streamlit >= 1.37
        st.html(html_cards, scrolling=True, height=900)
    except Exception:
        import streamlit.components.v1 as components
        components.html(html_cards, height=900, scrolling=True)

else:
    # =========================
    # Exibição — Tabela
    # =========================
    cols_show = ["IDProduto","Produto","Entradas","Saidas","Ajustes","EstoqueAtual","CustoAtual","ValorTotal"]
    if col_cat and col_cat in dfv.columns: cols_show.insert(2, col_cat)
    if col_forn and col_forn in dfv.columns: cols_show.insert(3, col_forn)
    if col_estq_min and col_estq_min in dfv.columns and col_estq_min not in cols_show:
        cols_show.append(col_estq_min)
    if col_preco and col_preco in dfv.columns and col_preco not in cols_show:
        cols_show.insert(2, col_preco)
    if "ImagemURL" in dfv.columns and "ImagemURL" not in cols_show:
        cols_show.append("ImagemURL")

    df_show = dfv.loc[:, [c for c in cols_show if c in dfv.columns]].copy()
    if "CustoAtual" in df_show: df_show["CustoAtual"] = df_show["CustoAtual"].map(_fmt_money_br)
    if "ValorTotal" in df_show: df_show["ValorTotal"] = df_show["ValorTotal"].map(_fmt_money_br)
    if col_preco and col_preco in df_show: df_show[col_preco] = df_show[col_preco].map(lambda x: _fmt_money_br(_to_num(x)))

    st.dataframe(df_show.sort_values("Produto"), use_container_width=True, hide_index=True)

# =========================
# Rodapé / Ajuda
# =========================
st.caption("""
• **EstoqueAtual** = Entradas − Saídas ± Ajustes (aba **MovimentosEstoque**).
• **CustoAtual** = último custo de compra (aba **Compras**).
• Para fotos, adicione uma coluna **Imagem** (ou **Foto**, **URLImagem**, **LinkImagem**) na aba **Produtos**.
• Use **Compras** / **Fracionar** / **Ajustes** para movimentar estoque.
""")
