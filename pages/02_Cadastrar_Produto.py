# pages/03_Compras_Produtos_Entradas.py — Compras + Fracionamento (redesenhado)
# -*- coding: utf-8 -*-
from __future__ import annotations
import json, unicodedata, re, time
import streamlit as st
import pandas as pd
import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from google.oauth2.service_account import Credentials
from datetime import date

# ──────────────────────────────────────────────
#  CONFIG & TEMA
# ──────────────────────────────────────────────
import pathlib
_cfg = pathlib.Path(".streamlit"); _cfg.mkdir(exist_ok=True)
(_cfg / "config.toml").write_text('[theme]\nbase = "dark"\n')

st.set_page_config(page_title="Compras / Entradas", page_icon="📥",
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
.sec-titulo {
    font-family:'Nunito',sans-serif; font-weight:800; font-size:1.05rem;
    color:rgba(255,255,255,0.9); margin:24px 0 14px 0;
    display:flex; align-items:center; gap:8px;
}
.sec-titulo::after {
    content:''; flex:1; height:1px;
    background:linear-gradient(to right,rgba(255,255,255,0.15),transparent);
    margin-left:8px;
}
/* Card info */
.info-card {
    background:rgba(255,255,255,0.05); border:1px solid rgba(255,255,255,0.1);
    border-radius:16px; padding:16px 20px; margin-bottom:16px;
}
.info-card .titulo { font-family:'Nunito',sans-serif; font-weight:800; font-size:0.95rem; color:#fff; margin-bottom:6px; }
.info-card .detalhe { font-size:0.78rem; color:rgba(255,255,255,0.5); }

/* Cálculo fracionamento */
.calc-box {
    background:rgba(74,222,128,0.08); border:1px solid rgba(74,222,128,0.25);
    border-radius:14px; padding:16px 20px; margin:12px 0;
}
.calc-titulo { font-size:0.75rem; color:rgba(255,255,255,0.45); text-transform:uppercase; letter-spacing:0.5px; margin-bottom:8px; font-weight:600; }
.calc-linha  { display:flex; justify-content:space-between; margin:4px 0; font-size:0.88rem; color:rgba(255,255,255,0.8); }
.calc-linha b { color:#4ade80; font-family:'Nunito',sans-serif; }
.calc-aviso  { background:rgba(251,191,36,0.12); border:1px solid rgba(251,191,36,0.3);
    border-radius:10px; padding:10px 14px; font-size:0.82rem; color:#fbbf24; margin-top:8px; }
.calc-erro   { background:rgba(248,113,113,0.12); border:1px solid rgba(248,113,113,0.3);
    border-radius:10px; padding:10px 14px; font-size:0.82rem; color:#f87171; margin-top:8px; }

/* Histórico */
.hist-item {
    background:rgba(255,255,255,0.04); border:1px solid rgba(255,255,255,0.07);
    border-radius:12px; padding:12px 16px; margin-bottom:8px;
}
.hist-prod { font-weight:700; font-size:0.9rem; color:#fff; }
.hist-det  { font-size:0.75rem; color:rgba(255,255,255,0.4); margin-top:3px; }

button[kind="primary"] { border-radius:12px !important; font-weight:700 !important; }
</style>
""", unsafe_allow_html=True)


# ──────────────────────────────────────────────
#  HELPERS SHEETS
# ──────────────────────────────────────────────
#  CONEXÃO / HELPERS  (centralizados em utils/sheets.py)
# ──────────────────────────────────────────────
from utils.sheets import (
    sheet as _sheet_obj, carregar_aba, garantir_aba, append_rows,
    to_num, brl, safe_cost, first_col, fmt_num,
    norm_tipo_mov, calcular_estoque, tg_send, tg_media, gerar_id, parse_date,
    ABA_PROD, ABA_VEND, ABA_COMP, ABA_MOVS, ABA_CLIEN, ABA_FIADO, ABA_FPAGT,
)
# Aliases de compatibilidade
_to_num = to_num; _to_float = to_num; _brl = brl; _fmt_brl = brl
_first_col = first_col; _fmt_num = fmt_num; _parse_date_any = parse_date
_tg_send = tg_send; _tg_media = tg_media; _norm_tipo_mov = norm_tipo_mov
_gerar_id = gerar_id; _parse_date = parse_date; _norm_tipo = norm_tipo_mov
def _canon_id(x):
    import re as _re; return _re.sub(r"[^0-9]", "", str(x or ""))
def conectar_sheets(): return _sheet_obj()

# BUMP = token de cache — força recarregamento quando cache_data é limpo
BUMP = st.session_state.get("_bump", 0)

@st.cache_data(ttl=30, show_spinner=False)
def _load_df(aba: str, _bump: int = 0):
    return carregar_aba(aba)

def _pick(df, candidates):
    if df is None or df.empty: return None
    for c in candidates:
        if c in df.columns: return c
    return None

def _nz(x) -> str:
    """Converte para string limpa, retorna '' se None/NaN.""";
    if x is None: return ""
    s = str(x).strip()
    return "" if s.lower() in ("nan","none","<na>") else s

def _ensure_ws(nome, headers=None):
    return garantir_aba(nome, headers or [])

def _append_row(ws, row: dict):
    append_rows(ws, [row])

def _refresh():
    st.cache_data.clear()
    st.rerun()

ABA_PROD = "Produtos"
ABA_COMP = "Compras"
ABA_MOVS = "MovimentosEstoque"

COMPRAS_HDR = ["Data","Produto","Unidade","Fornecedor","Qtd","Custo Unitário","Total","IDProduto","Obs"]
MOV_HDR     = ["Data","IDProduto","Produto","Tipo","Qtd","Obs","ID","Documento/NF","Origem","SaldoApós"]

# ──────────────────────────────────────────────
#  CARREGAR DADOS
# ──────────────────────────────────────────────
try:    prod_df = _load_df(ABA_PROD, BUMP)
except: st.error("Erro ao abrir aba Produtos."); st.stop()

COL_ID   = _pick(prod_df, ["ID","Codigo","SKU","IDProduto"])
COL_NOME = _pick(prod_df, ["Nome","Produto","Descrição"])
COL_UNID = _pick(prod_df, ["Unidade","Unid","Und"])
COL_FORN = _pick(prod_df, ["Fornecedor","FornecedorNome"])
COL_FOTO = _pick(prod_df, ["Foto","Imagem","URLImagem"])

# ──────────────────────────────────────────────
#  ESTOQUE ATUAL
# ──────────────────────────────────────────────
def _estoque_atual(pid, nome):
    try: mov = _load_df(ABA_MOVS, BUMP)
    except: return 0.0
    if mov.empty: return 0.0
    c_id  = _pick(mov, ["IDProduto","ProdutoID","ID"])
    c_nom = _pick(mov, ["Produto","Nome"])
    c_qtd = _pick(mov, ["Qtd","Quantidade"])
    c_tip = _pick(mov, ["Tipo"])
    if not c_qtd or not c_tip: return 0.0

    df = mov.copy()
    if pid and c_id: df = df[df[c_id].astype(str).str.strip() == str(pid).strip()]
    elif nome and c_nom: df = df[df[c_nom].astype(str).str.strip() == str(nome).strip()]
    if df.empty: return 0.0

    def _n(x):
        s = str(x or "").strip().replace(" ","").replace(".","").replace(",",".")
        try: return float(s)
        except: return 0.0

    def _sign(t):
        t = (t or "").lower()
        if "+" in t or "entrada" in t or (t.startswith("b") and "sai" not in t): return 1
        if "-" in t or "saida" in t or "saída" in t or "venda" in t: return -1
        if "ajuste" in t: return 1 if "+" in t else (-1 if "-" in t else 0)
        return 0

    return sum(_n(r[c_qtd]) * _sign(r[c_tip]) for _, r in df.iterrows())

# ──────────────────────────────────────────────
#  ÚLTIMA COMPRA
# ──────────────────────────────────────────────
def _ultima_compra(pid, nome):
    try: comp = _load_df(ABA_COMP, BUMP)
    except: return None
    if comp.empty: return None
    c_id  = _pick(comp, ["IDProduto","ProdutoID","ID"])
    c_nom = _pick(comp, ["Produto","Nome"])
    c_cu  = _pick(comp, ["Custo Unitário","CustoUnit","Custo"])
    c_dat = "Data" if "Data" in comp.columns else None
    c_qtd = _pick(comp, ["Qtd","Quantidade"])
    c_uni = _pick(comp, ["Unidade","Unid"])

    df = comp.copy()
    if pid and c_id: df = df[df[c_id].astype(str).str.strip() == str(pid).strip()]
    elif nome and c_nom: df = df[df[c_nom].astype(str).str.strip() == str(nome).strip()]
    if df.empty or not c_cu: return None

    if c_dat:
        try:
            df["_d"] = pd.to_datetime(df[c_dat], format="%d/%m/%Y", errors="coerce")
            df = df.sort_values("_d", ascending=False)
        except: pass

    r = df.iloc[0]
    cu = _to_float(r.get(c_cu,""))
    return {
        "custo_unit": cu,
        "unidade":    _nz(r.get(c_uni,"")) if c_uni else "",
        "qtd":        _nz(r.get(c_qtd,"")) if c_qtd else "",
        "data":       _nz(r.get("Data","")) if "Data" in df.columns else "",
    }


# ══════════════════════════════════════════════
#  HEADER
# ══════════════════════════════════════════════
st.markdown("""
<div class="page-header">
  <div>
    <h1>📥 Compras / Entradas</h1>
    <div class="sub">Ebenezér Variedades · Registro de estoque</div>
  </div>
  <div class="header-badge">📦 Estoque</div>
</div>
""", unsafe_allow_html=True)

# Tabs principais
aba_compra, aba_frac, aba_corrigir = st.tabs([
    "📥 Nova compra", "🧪 Fracionar granel", "🛠️ Corrigir lançamento"
])


# ══════════════════════════════════════════════
#  ABA 1 — NOVA COMPRA
# ══════════════════════════════════════════════
with aba_compra:
    st.markdown('<div class="sec-titulo">📦 Registrar entrada de produto</div>', unsafe_allow_html=True)

    # Selecionar produto
    if prod_df.empty:
        st.warning("Nenhum produto cadastrado."); st.stop()

    def _fmt_prod(r):
        n = _nz(r.get(COL_NOME,"")) or "(sem nome)"
        f = _nz(r.get(COL_FORN,"") if COL_FORN else "")
        return n + (f" — {f}" if f else "")

    labels_prod = prod_df.apply(_fmt_prod, axis=1).tolist()
    idx_sel = st.selectbox("🔍 Produto", options=range(len(prod_df)),
                           format_func=lambda i: labels_prod[i], key="comp_sel_prod")

    row_sel  = prod_df.iloc[idx_sel]
    prod_id  = _nz(row_sel.get(COL_ID,"") if COL_ID else "")
    prod_nom = _nz(row_sel.get(COL_NOME,"") if COL_NOME else "")
    prod_uni = _nz(row_sel.get(COL_UNID,"") if COL_UNID else "")
    prod_forn= _nz(row_sel.get(COL_FORN,"") if COL_FORN else "")
    prod_foto= _nz(row_sel.get(COL_FOTO,"") if COL_FOTO else "")

    # Info do produto selecionado
    estq_atual = _estoque_atual(prod_id, prod_nom)
    ult_comp   = _ultima_compra(prod_id, prod_nom)

    ci1, ci2 = st.columns([1, 2])
    with ci1:
        if prod_foto and prod_foto.startswith("http"):
            st.image(prod_foto, width=100)
        else:
            st.markdown('<div style="width:100px;height:100px;background:rgba(255,255,255,0.06);border-radius:12px;display:flex;align-items:center;justify-content:center;font-size:2rem">📦</div>', unsafe_allow_html=True)
    with ci2:
        st.markdown(f"""
        <div class="info-card">
          <div class="titulo">{prod_nom}</div>
          <div class="detalhe">
            📦 Estoque atual: <b style="color:#4ade80">{int(estq_atual) if float(estq_atual).is_integer() else estq_atual} {prod_uni}</b><br>
            {f'🧾 Última compra: <b>{_brl(ult_comp["custo_unit"])}/{prod_uni}</b> em {ult_comp["data"]}' if ult_comp and ult_comp.get("custo_unit") else '🧾 Sem histórico de compra'}
          </div>
        </div>
        """, unsafe_allow_html=True)

    # Formulário de compra
    with st.form("form_compra", clear_on_submit=True):
        f1, f2, f3 = st.columns([1, 1, 1])
        with f1:
            data_c = st.date_input("📅 Data da compra", value=date.today())
            qtd    = st.number_input(f"📦 Quantidade ({prod_uni or 'un'})",
                                     min_value=0.01, step=1.0, value=1.0, format="%.2f")
        with f2:
            # Sugerir custo da última compra
            custo_sug = ult_comp["custo_unit"] if ult_comp and ult_comp.get("custo_unit") else 0.0
            custo = st.number_input("💰 Custo unitário (R$)",
                                    min_value=0.0, step=0.10, value=float(custo_sug or 0.0), format="%.2f")
            fornecedor = st.text_input("🚚 Fornecedor", value=prod_forn)
        with f3:
            unidades_opt = ["un","L","kg","g","ml","cx","pct","Outro…"]
            idx_u = unidades_opt.index(prod_uni) if prod_uni in unidades_opt else 0
            unid_sel = st.selectbox("📏 Unidade", unidades_opt, index=idx_u)
            unid_out = ""
            if unid_sel == "Outro…":
                unid_out = st.text_input("Qual unidade?", placeholder="Ex: rolo, m, par")
            obs = st.text_input("💬 Observações (opcional)")

        unid_final = unid_out.strip() if unid_sel == "Outro…" else unid_sel

        # Preview do total
        total_prev = qtd * custo
        st.markdown(f"""
        <div style="background:rgba(255,255,255,0.04);border:1px solid rgba(255,255,255,0.08);
        border-radius:12px;padding:12px 16px;margin:8px 0;display:flex;justify-content:space-between">
          <span style="color:rgba(255,255,255,0.6)">Total da compra</span>
          <span style="font-family:Nunito;font-weight:800;font-size:1.1rem;color:#4ade80">{_brl(total_prev)}</span>
        </div>
        """, unsafe_allow_html=True)

        salvar = st.form_submit_button("✅  Registrar entrada", type="primary", use_container_width=True)

    if salvar:
        if qtd <= 0:  st.error("Quantidade deve ser maior que zero."); st.stop()
        if custo <= 0: st.error("Informe o custo unitário."); st.stop()

        est_antes  = _estoque_atual(prod_id, prod_nom)
        est_depois = est_antes + qtd
        total      = round(qtd * custo, 2)
        data_str   = data_c.strftime("%d/%m/%Y")
        qtd_str    = str(int(qtd)) if qtd == int(qtd) else f"{qtd:.3f}".replace(".",",")

        ws_c = _ensure_ws(ABA_COMP, COMPRAS_HDR)
        ws_m = _ensure_ws(ABA_MOVS, MOV_HDR)

        _append_row(ws_c, {
            "Data": data_str, "Produto": prod_nom, "Unidade": unid_final,
            "Fornecedor": fornecedor, "Qtd": qtd_str,
            "Custo Unitário": f"{custo:.2f}".replace(".",","),
            "Total": f"{total:.2f}".replace(".",","),
            "IDProduto": prod_id, "Obs": obs
        })
        _append_row(ws_m, {
            "Data": data_str, "IDProduto": prod_id, "Produto": prod_nom,
            "Tipo": "B entrada", "Qtd": qtd_str,
            "Obs": ("Compra — " + obs).strip(" —"),
            "ID": "", "Documento/NF": "", "Origem": "Compras / Entradas",
            "SaldoApós": str(int(est_depois)) if est_depois == int(est_depois) else str(round(est_depois,2))
        })

        _tg_send(
            f"🧾 <b>Entrada registrada</b>\n{data_str}\n"
            f"Produto: <b>{prod_nom}</b>\nQtd: <b>{qtd_str} {unid_final}</b>\n"
            f"Custo unit.: <b>{_brl(custo)}</b>\nTotal: <b>{_brl(total)}</b>\n"
            + (f"Fornecedor: {fornecedor}\n" if fornecedor else "")
            + f"📦 Estoque: {int(est_antes)} → <b>{int(est_depois)}</b>"
        )

        st.success(f"✅ Entrada registrada! Estoque de **{prod_nom}**: {int(est_antes)} → **{int(est_depois)} {unid_final}**")
        _refresh()


# ══════════════════════════════════════════════
#  ABA 2 — FRACIONAR GRANEL
# ══════════════════════════════════════════════
with aba_frac:
    st.markdown('<div class="sec-titulo">🧪 Fracionar granel em embalagens menores</div>', unsafe_allow_html=True)

    st.markdown("""
    <div style="background:rgba(96,165,250,0.08);border:1px solid rgba(96,165,250,0.2);
    border-radius:12px;padding:12px 16px;font-size:0.85rem;color:rgba(255,255,255,0.75);margin-bottom:16px">
    💡 <b>Como funciona:</b> Você comprou um produto a granel (ex: 1 galão de 5L por R$29,71)
    e vai dividir em frascos menores (ex: 10 frascos de 500ml).
    O custo de cada frasco é calculado automaticamente: <b>custo total ÷ número de frascos</b>.
    </div>
    """, unsafe_allow_html=True)

    if prod_df.empty:
        st.info("Cadastre produtos primeiro.")
    else:
        # Filtrar apenas produtos com estoque > 0 para a lista do granel
        def _tem_estoque(r):
            pid  = _nz(r.get(COL_ID,"")   if COL_ID   else "")
            pnom = _nz(r.get(COL_NOME,"") if COL_NOME else "")
            return _estoque_atual(pid, pnom) > 0

        prod_com_estoque = prod_df[prod_df.apply(_tem_estoque, axis=1)].reset_index(drop=True)

        if prod_com_estoque.empty:
            st.info("Nenhum produto com estoque disponível para fracionar. Registre uma compra primeiro.")
            st.stop()

        labels_g    = prod_com_estoque.apply(_fmt_prod, axis=1).tolist()
        labels_todos = prod_df.apply(_fmt_prod, axis=1).tolist()  # todos os produtos como destino do frasco

        st.markdown("**1️⃣ Qual produto granel você vai fracionar?**")
        idx_g = st.selectbox("Produto granel (apenas com estoque)", options=range(len(prod_com_estoque)),
                             format_func=lambda i: labels_g[i], key="frac_granel")

        row_g = prod_com_estoque.iloc[idx_g]
        gid   = _nz(row_g.get(COL_ID,"")   if COL_ID   else "")
        gnome = _nz(row_g.get(COL_NOME,"") if COL_NOME else "")
        gunid = _nz(row_g.get(COL_UNID,"") if COL_UNID else "un")

        estq_g = _estoque_atual(gid, gnome)
        ult_g  = _ultima_compra(gid, gnome)
        custo_unit_granel = ult_g["custo_unit"] if ult_g and ult_g.get("custo_unit") else None

        cor_estq = "#4ade80" if estq_g > 0 else "#f87171"
        # Mostrar custo de forma clara: quanto custou cada unidade
        if custo_unit_granel:
            info_custo = f"&nbsp;·&nbsp; 💰 Cada {gunid} custou: <b>{_brl(custo_unit_granel)}</b>"
        else:
            info_custo = "&nbsp;·&nbsp; ⚠️ Sem histórico de compra"
        estq_fmt = str(int(estq_g)) if estq_g == int(estq_g) else f"{estq_g:.2f}"
        st.markdown(f'''
        <div class="info-card">
          <div class="titulo">{gnome}</div>
          <div class="detalhe">
            📦 Estoque disponível: <b style="color:{cor_estq}">{estq_fmt} {gunid}</b>
            {info_custo}
          </div>
        </div>
        ''', unsafe_allow_html=True)

        st.markdown("**2️⃣ Quantas unidades do granel vai usar agora?**")
        max_granel = float(max(estq_g, 0.01))
        qtd_granel_usar = st.number_input(
            f"Quantidade de {gunid} a fracionar",
            min_value=0.01, max_value=max_granel,
            value=min(1.0, max_granel),
            step=1.0, format="%.2f", key="frac_qtd_granel"
        )

        if custo_unit_granel:
            custo_total_lote = custo_unit_granel * qtd_granel_usar
            qtd_str = str(int(qtd_granel_usar)) if qtd_granel_usar == int(qtd_granel_usar) else f"{qtd_granel_usar:.2f}"
            st.markdown(f'''
            <div style="background:rgba(74,222,128,0.07);border:1px solid rgba(74,222,128,0.2);
            border-radius:10px;padding:12px 16px;font-size:0.85rem;">
            💰 <span style="color:rgba(255,255,255,0.6)">Vai fracionar</span>
            <b style="color:#fff">{qtd_str} {gunid}</b>
            <span style="color:rgba(255,255,255,0.6)"> que custou </span>
            <b style="color:#4ade80;font-size:1rem">{_brl(custo_total_lote)}</b>
            <span style="color:rgba(255,255,255,0.35);font-size:0.75rem">
             ({_brl(custo_unit_granel)}/un × {qtd_str})
            </span>
            </div>
            ''', unsafe_allow_html=True)
        else:
            st.warning("⚠️ Custo não encontrado. Informe o custo total do lote:")
            custo_total_lote = st.number_input(
                "Custo total do lote que vai fracionar (R$)",
                min_value=0.0, step=0.01, format="%.2f", key="frac_custo_manual"
            )

        st.markdown("---")
        st.markdown("**3️⃣ Em quantos frascos vai dividir?**")

        fa1, fa2 = st.columns(2)
        with fa1:
            st.markdown("**Frasco A**")
            idx_a = st.selectbox("Produto do frasco A", options=range(len(prod_df)),
                                 format_func=lambda i: labels_todos[i], key="frac_a_prod")
            qtd_a = st.number_input("Quantos frascos A?", min_value=0, step=1, value=0, key="frac_a_qtd")
            emb_a = st.number_input("Custo embalagem A (R$)", min_value=0.0, step=0.01,
                                    value=0.0, format="%.2f", key="frac_a_emb",
                                    help="Custo do frasco/pote vazio. Deixe 0 se não tiver.")
        with fa2:
            usar_b = st.checkbox("Adicionar frasco B (tamanho diferente)", value=False, key="frac_usar_b")
            if usar_b:
                st.markdown("**Frasco B**")
                idx_b = st.selectbox("Produto do frasco B", options=range(len(prod_df)),
                                     format_func=lambda i: labels_todos[i], key="frac_b_prod")
                qtd_b = st.number_input("Quantos frascos B?", min_value=0, step=1, value=0, key="frac_b_qtd")
                emb_b = st.number_input("Custo embalagem B (R$)", min_value=0.0, step=0.01,
                                        value=0.0, format="%.2f", key="frac_b_emb")
            else:
                idx_b = qtd_b = 0
                emb_b = 0.0

        total_frascos = qtd_a + (qtd_b if usar_b else 0)
        saldo_granel  = estq_g - qtd_granel_usar

        # CALCULO CORRETO: custo total do lote / total de frascos = custo do granel por frasco
        # Cada frasco recebe uma fatia igual do custo do granel + custo da própria embalagem
        if total_frascos > 0 and custo_total_lote > 0:
            custo_granel_por_frasco = custo_total_lote / total_frascos
            custo_a = round(custo_granel_por_frasco + emb_a, 4) if qtd_a > 0 else 0.0
            custo_b = round(custo_granel_por_frasco + emb_b, 4) if (usar_b and qtd_b > 0) else 0.0
        else:
            custo_granel_por_frasco = custo_a = custo_b = 0.0

        if total_frascos > 0:
            ok_estoque = saldo_granel >= -0.001
            box_cor = "" if ok_estoque else "background:rgba(248,113,113,0.08);border-color:rgba(248,113,113,0.3)"
            cor_saldo = "#4ade80" if ok_estoque else "#f87171"
            linha_a = f'<div class="calc-linha">Custo frasco A (+emb {_brl(emb_a)}): <b>{_brl(custo_a)}</b></div>' if qtd_a > 0 else ""
            linha_b = f'<div class="calc-linha">Custo frasco B (+emb {_brl(emb_b)}): <b>{_brl(custo_b)}</b></div>' if usar_b and qtd_b > 0 else ""
            st.markdown(f'''
            <div class="calc-box" style="{box_cor}">
              <div class="calc-titulo">📊 Resumo do fracionamento</div>
              <div class="calc-linha">Granel usado: <b>{qtd_granel_usar:.2f} {gunid}</b></div>
              <div class="calc-linha">Custo total do lote: <b>{_brl(custo_total_lote)}</b></div>
              <div class="calc-linha">Total de frascos: <b>{total_frascos}</b></div>
              <div class="calc-linha">Custo do granel por frasco: <b>{_brl(custo_granel_por_frasco)}</b></div>
              {linha_a}
              {linha_b}
              <div class="calc-linha" style="margin-top:8px;padding-top:8px;border-top:1px solid rgba(255,255,255,0.1)">
                Estoque granel após: <b style="color:{cor_saldo}">{saldo_granel:.2f} {gunid}</b>
              </div>
            </div>
            ''', unsafe_allow_html=True)

            if not ok_estoque:
                st.error(f"❌ Estoque insuficiente! Disponível: {estq_g:.2f} {gunid}, necessário: {qtd_granel_usar:.2f} {gunid}.")

        btn_frac = st.button("✅  Registrar fracionamento", type="primary",
                             use_container_width=True, key="btn_frac",
                             disabled=(total_frascos == 0 or custo_total_lote == 0))

        if btn_frac:
            if total_frascos == 0: st.error("Informe quantidade de frascos."); st.stop()
            if estq_g < qtd_granel_usar - 1e-9:
                st.error(f"Estoque insuficiente: {estq_g:.2f} {gunid} disponíveis."); st.stop()
            if custo_total_lote <= 0: st.error("Informe o custo do lote."); st.stop()

            ws_m     = _ensure_ws(ABA_MOVS, MOV_HDR)
            data_str = date.today().strftime("%d/%m/%Y")
            qtd_g_str = f"{qtd_granel_usar:.4f}".replace(".",",").rstrip("0").rstrip(",")

            _append_row(ws_m, {
                "Data": data_str, "IDProduto": gid, "Produto": gnome,
                "Tipo": "C fracionamento -", "Qtd": qtd_g_str,
                "Obs": f"Fracionamento → {total_frascos} frascos (custo lote {_brl(custo_total_lote)})",
                "ID":"", "Documento/NF":"", "Origem":"Fracionamento",
                "SaldoApós": f"{saldo_granel:.4f}".replace(".",",")
            })

            linhas_tg = []

            def _gravar_frasco(idx_prod, qtd_f, custo_f, emb_f, letra):
                r     = prod_df.iloc[idx_prod]
                fid   = _nz(r.get(COL_ID,"")   if COL_ID   else "")
                fnome = _nz(r.get(COL_NOME,"") if COL_NOME else "")
                _append_row(ws_m, {
                    "Data": data_str, "IDProduto": fid, "Produto": fnome,
                    "Tipo": "C fracionamento +", "Qtd": str(int(qtd_f)),
                    "Obs": f"Fracionamento de {gnome}: {qtd_granel_usar:.2f}{gunid} → {total_frascos} frascos",
                    "ID":"", "Documento/NF":"", "Origem":"Fracionamento", "SaldoApós":""
                })
                # Atualiza CustoAtual no produto
                try:
                    ws_p = _ensure_ws(ABA_PROD)
                    df_p = get_as_dataframe(ws_p, evaluate_formulas=True, dtype=str, header=0).fillna("")
                    c_id_p = _pick(df_p, ["ID","Codigo","SKU","IDProduto"])
                    c_no_p = _pick(df_p, ["Nome","Produto","Descrição"])
                    if "CustoAtual" not in df_p.columns: df_p["CustoAtual"] = ""
                    mask = pd.Series([False]*len(df_p))
                    if fid and c_id_p: mask |= (df_p[c_id_p].astype(str).str.strip() == fid.strip())
                    if not mask.any() and fnome and c_no_p:
                        mask |= (df_p[c_no_p].astype(str).str.strip() == fnome.strip())
                    if mask.any():
                        df_p.at[df_p.index[mask][0], "CustoAtual"] = f"{custo_f:.4f}".replace(".",",")
                        ws_p.clear()
                        set_with_dataframe(ws_p, df_p, include_index=False, include_column_header=True, resize=True)
                except Exception as e:
                    st.warning(f"Fracionamento OK mas não atualizei custo do frasco {letra}: {e}")
                linhas_tg.append(f"• {fnome}: {int(qtd_f)} frascos → custo {_brl(custo_f)}/frasco")

            if qtd_a > 0: _gravar_frasco(idx_a, qtd_a, custo_a, emb_a, "A")
            if usar_b and qtd_b > 0: _gravar_frasco(idx_b, qtd_b, custo_b, emb_b, "B")

            _tg_send(
                f"🧪 <b>Fracionamento registrado</b>\n{data_str}\n"
                f"Granel: <b>{gnome}</b> ↓ {qtd_granel_usar:.2f} {gunid} (custo lote {_brl(custo_total_lote)})\n"
                + "\n".join(linhas_tg)
                + f"\n📦 Granel: {estq_g:.2f} → <b>{saldo_granel:.2f} {gunid}</b>"
            )
            st.cache_data.clear()
            st.success(
                f"✅ Pronto! {total_frascos} frascos criados. "
                f"Custo do granel por frasco: {_brl(custo_granel_por_frasco)}"
                + (f" | Frasco A total: {_brl(custo_a)}" if emb_a > 0 and qtd_a > 0 else "")
            )
            _refresh()


# ══════════════════════════════════════════════
#  ABA 3 — CORRIGIR LANÇAMENTO
# ══════════════════════════════════════════════
with aba_corrigir:
    st.markdown('<div class="sec-titulo">🛠️ Editar ou apagar lançamento</div>', unsafe_allow_html=True)

    tipo_op = st.radio("Qual registro?", ["📦 Compra / Entrada", "📋 Movimento de Estoque"],
                       horizontal=True, key="corr_tipo")

    if "Compra" in tipo_op:
        aba_corr, hdrs_corr = ABA_COMP, COMPRAS_HDR
    else:
        aba_corr, hdrs_corr = ABA_MOVS, MOV_HDR

    # Carregar com número de linha
    try:
        ws_corr = _ensure_ws(aba_corr, hdrs_corr)
        df_corr = get_as_dataframe(ws_corr, evaluate_formulas=True, dtype=str, header=0).dropna(how="all")
        if df_corr.empty: df_corr = pd.DataFrame(columns=hdrs_corr)
        df_corr = df_corr.fillna("")
        df_corr["__ln"] = (df_corr.index + 2).astype(int)
    except Exception as e:
        st.error(f"Erro ao carregar dados: {e}"); st.stop()

    # Busca e filtro
    c_busca, c_lim = st.columns([2.5, 1])
    with c_busca:
        busca_corr = st.text_input("🔍 Buscar (produto, tipo, data...)", key="corr_busca")
    with c_lim:
        lim_corr = st.number_input("Mostrar últimos", min_value=5, max_value=200, value=30, step=5)

    df_v = df_corr.copy()
    if busca_corr.strip():
        t = busca_corr.strip().lower()
        cols_b = [c for c in ["Produto","Tipo","Data","Nome","Obs"] if c in df_v.columns]
        if cols_b:
            mask_b = df_v[cols_b[0]].astype(str).str.lower().str.contains(re.escape(t), na=False)
            for c in cols_b[1:]:
                mask_b |= df_v[c].astype(str).str.lower().str.contains(re.escape(t), na=False)
            df_v = df_v[mask_b]

    # Ordenar por data
    if "Data" in df_v.columns:
        try:
            df_v["_ds"] = pd.to_datetime(df_v["Data"], format="%d/%m/%Y", errors="coerce")
            df_v = df_v.sort_values("_ds", ascending=False)
        except: df_v = df_v.sort_index(ascending=False)
    else:
        df_v = df_v.sort_index(ascending=False)

    df_v = df_v.head(int(lim_corr)).reset_index(drop=True)

    if df_v.empty:
        st.info("Nenhum lançamento encontrado.")
    else:
        def _rot(row):
            d    = _nz(row.get("Data",""))
            prod = _nz(row.get("Produto", row.get("Nome","")))
            tipo = _nz(row.get("Tipo",""))
            qtd  = _nz(row.get("Qtd",""))
            ln   = row.get("__ln","?")
            return f"Linha {ln} · {d} · {prod}" + (f" · {tipo}" if tipo else "") + (f" · Qtd {qtd}" if qtd else "")

        escolha = st.selectbox("Selecione o lançamento", options=range(len(df_v)),
                               format_func=lambda i: _rot(df_v.iloc[i]), key="corr_escolha")
        rec = df_v.iloc[int(escolha)].to_dict()
        ln_real = int(rec["__ln"])

        # Card visual do registro
        prod_c = _nz(rec.get("Produto", rec.get("Nome","")))
        tipo_c = _nz(rec.get("Tipo",""))
        qtd_c  = _nz(rec.get("Qtd",""))
        data_c_v = _nz(rec.get("Data",""))
        obs_c  = _nz(rec.get("Obs",""))
        ico_c  = "➕" if ("entrada" in tipo_c.lower() or "+" in tipo_c) else ("➖" if ("saida" in tipo_c.lower() or "-" in tipo_c or "venda" in tipo_c.lower()) else "🔧")

        st.markdown(f"""
        <div class="hist-item">
          <div style="display:flex;gap:12px;align-items:flex-start">
            <div style="font-size:1.4rem">{ico_c}</div>
            <div>
              <div class="hist-prod">{prod_c}</div>
              <div class="hist-det">{tipo_c} · Qtd: <b>{qtd_c or "—"}</b> · Data: {data_c_v}</div>
              {f'<div class="hist-det" style="margin-top:3px">📝 {obs_c}</div>' if obs_c else ""}
            </div>
          </div>
        </div>
        """, unsafe_allow_html=True)

        # Formulário de edição
        with st.expander("✏️ Editar campos", expanded=False):
            if "Compra" in tipo_op:
                e1, e2, e3 = st.columns(3)
                with e1:
                    ed = {"Data":    st.text_input("Data",    value=_nz(rec.get("Data","")))}
                    ed["Produto"] =  st.text_input("Produto", value=_nz(rec.get("Produto","")))
                    ed["Unidade"] =  st.text_input("Unidade", value=_nz(rec.get("Unidade","")))
                with e2:
                    ed["Fornecedor"] = st.text_input("Fornecedor", value=_nz(rec.get("Fornecedor","")))
                    ed["Qtd"]        = st.text_input("Qtd",        value=_nz(rec.get("Qtd","")))
                    ed["Custo Unitário"] = st.text_input("Custo Unitário", value=_nz(rec.get("Custo Unitário","")))
                with e3:
                    ed["Total"]     = st.text_input("Total",     value=_nz(rec.get("Total","")))
                    ed["IDProduto"] = st.text_input("IDProduto", value=_nz(rec.get("IDProduto","")))
                    ed["Obs"]       = st.text_input("Obs",       value=_nz(rec.get("Obs","")))
            else:
                e1, e2, e3 = st.columns(3)
                with e1:
                    ed = {"Data":      st.text_input("Data",      value=_nz(rec.get("Data","")))}
                    ed["IDProduto"] =  st.text_input("IDProduto", value=_nz(rec.get("IDProduto","")))
                    ed["Produto"]   =  st.text_input("Produto",   value=_nz(rec.get("Produto","")))
                with e2:
                    ed["Tipo"] = st.text_input("Tipo", value=_nz(rec.get("Tipo","")))
                    ed["Qtd"]  = st.text_input("Qtd",  value=_nz(rec.get("Qtd","")))
                    ed["Obs"]  = st.text_input("Obs",  value=_nz(rec.get("Obs","")))
                with e3:
                    ed["ID"]           = st.text_input("ID",           value=_nz(rec.get("ID","")))
                    ed["Documento/NF"] = st.text_input("Documento/NF", value=_nz(rec.get("Documento/NF","")))
                    ed["Origem"]       = st.text_input("Origem",       value=_nz(rec.get("Origem","")))
                    ed["SaldoApós"]    = st.text_input("SaldoApós",    value=_nz(rec.get("SaldoApós","")))

            ok_salvar = st.checkbox("✔️ Confirmar salvar alterações", key="corr_chk_save")
            if st.button("💾 Salvar alterações", use_container_width=True,
                         disabled=not ok_salvar, key="corr_btn_save"):
                df2 = df_corr.copy()
                pos = df2.index[df2["__ln"] == ln_real]
                if len(pos) != 1:
                    st.error("Linha não encontrada.")
                else:
                    for k, v in ed.items():
                        if k in df2.columns: df2.at[pos[0], k] = v
                    df3 = df2.drop(columns=["__ln"], errors="ignore")
                    ws_corr.clear()
                    set_with_dataframe(ws_corr, df3.fillna(""), include_index=False,
                                       include_column_header=True, resize=True)
                    st.success("✅ Alterações salvas!")
                    _refresh()

        # Apagar
        st.markdown("<br>", unsafe_allow_html=True)
        ok_del = st.checkbox("🗑️ Confirmar exclusão deste lançamento", key="corr_chk_del")
        if st.button("🗑️ Apagar lançamento", use_container_width=True,
                     disabled=not ok_del, key="corr_btn_del"):
            df2 = df_corr.copy()
            pos = df2.index[df2["__ln"] == ln_real]
            if len(pos) != 1:
                st.error("Linha não encontrada.")
            else:
                df2 = df2.drop(index=pos[0]).reset_index(drop=True)
                df3 = df2.drop(columns=["__ln"], errors="ignore")
                ws_corr.clear()
                set_with_dataframe(ws_corr, df3.fillna(""), include_index=False,
                                   include_column_header=True, resize=True)
                st.success("✅ Lançamento apagado!")
                _refresh()
