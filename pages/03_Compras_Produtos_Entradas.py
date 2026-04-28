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
    sheet, carregar_aba, garantir_aba, append_rows,
    to_num, brl, safe_cost, first_col, fmt_num,
    norm_tipo_mov, calcular_estoque, tg_send, tg_media, gerar_id, parse_date,
    ABA_PROD, ABA_VEND, ABA_COMP, ABA_MOVS, ABA_CLIEN, ABA_FIADO, ABA_FPAGT,
)
# Aliases de compatibilidade
_to_num = to_num; _to_float = to_num; _brl = brl; _fmt_brl = brl
_first_col = first_col; _fmt_num = fmt_num; _parse_date_any = parse_date
_tg_send = tg_send; _tg_media = tg_media; _norm_tipo_mov = norm_tipo_mov
_gerar_id = gerar_id; _parse_date = parse_date; _norm_tipo = norm_tipo_mov
_to_date = parse_date

def _canon_id(x):
    import re as _re; return _re.sub(r"[^0-9]", "", str(x or ""))
def conectar_sheets(): return sheet()
def _sheet(): return sheet()

BUMP = st.session_state.get("_refresh_ts", 0)

@st.cache_data(ttl=60)
def _load_df(aba, _bump=0):
    return carregar_aba(aba)

def _safe_load(aba):
    try: return _load_df(aba)
    except: return pd.DataFrame()

def _nz(x):
    if x is None: return ""
    try:
        if pd.isna(x): return ""
    except: pass
    s = str(x).strip()
    return "" if s.lower() in ("nan","none") else s

def _pick(df, cands):
    if df is None or df.empty: return None
    for c in cands:
        if c in df.columns: return c
    return None

def _refresh():
    st.session_state["_refresh_ts"] = __import__("time").time()
    st.cache_data.clear()
    st.rerun()

def _append_row(ws, row):
    hdrs = [h.strip() for h in ws.row_values(1)]
    ws.append_rows([[row.get(h, "") for h in hdrs]], value_input_option="USER_ENTERED")

def _ensure_ws(name, headers=None):
    headers = headers or []
    return garantir_aba(name, headers)

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
aba_compra, aba_corrigir = st.tabs([
    "📥 Nova compra", "🛠️ Corrigir lançamento"
])


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
