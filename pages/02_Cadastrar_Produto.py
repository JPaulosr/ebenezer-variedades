# pages/02_Cadastrar_Produto.py — Cadastro/edição de produto (redesenhado)
# -*- coding: utf-8 -*-
import json, unicodedata, math, re
from datetime import datetime, timedelta, date

import streamlit as st
import pandas as pd
import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from google.oauth2.service_account import Credentials
import requests

# ──────────────────────────────────────────────
#  CONFIG & TEMA
# ──────────────────────────────────────────────
import pathlib
_cfg = pathlib.Path(".streamlit"); _cfg.mkdir(exist_ok=True)
(_cfg / "config.toml").write_text('[theme]\nbase = "dark"\n')

st.set_page_config(page_title="Cadastrar Produto", page_icon="➕",
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

/* Seção */
.sec-titulo {
    font-family:'Nunito',sans-serif; font-weight:800; font-size:1rem;
    color:rgba(255,255,255,0.85); margin:20px 0 12px 0;
    display:flex; align-items:center; gap:8px;
}
.sec-titulo::after {
    content:''; flex:1; height:1px;
    background:linear-gradient(to right,rgba(255,255,255,0.12),transparent);
    margin-left:8px;
}

/* Card de seção */
.sec-card {
    background:rgba(255,255,255,0.04); border:1px solid rgba(255,255,255,0.08);
    border-radius:16px; padding:20px 22px; margin-bottom:16px;
}

/* Preview produto */
.prev-card {
    background:rgba(255,255,255,0.05); border:1px solid rgba(255,255,255,0.1);
    border-radius:16px; padding:18px; display:flex; gap:16px; align-items:center;
    margin-bottom:16px;
}
.prev-card img { width:80px; height:80px; border-radius:12px; object-fit:contain;
    background:rgba(255,255,255,0.06); border:1px solid rgba(255,255,255,0.1); flex-shrink:0; }
.prev-card-ph { width:80px; height:80px; border-radius:12px;
    background:rgba(255,255,255,0.06); border:1px solid rgba(255,255,255,0.1);
    display:flex; align-items:center; justify-content:center; font-size:2rem; flex-shrink:0; }
.prev-nome { font-family:'Nunito',sans-serif; font-weight:800; font-size:1rem; color:#fff; }
.prev-sub  { font-size:0.78rem; color:rgba(255,255,255,0.4); margin-top:3px; }
.prev-badge-edit { display:inline-block; background:rgba(251,191,36,0.15);
    border:1px solid rgba(251,191,36,0.3); color:#fbbf24;
    border-radius:8px; padding:3px 10px; font-size:0.72rem; font-weight:700; margin-top:6px; }
.prev-badge-new  { display:inline-block; background:rgba(74,222,128,0.12);
    border:1px solid rgba(74,222,128,0.3); color:#4ade80;
    border-radius:8px; padding:3px 10px; font-size:0.72rem; font-weight:700; margin-top:6px; }

/* Resumo salvar */
.resumo-box {
    background:rgba(74,222,128,0.07); border:1px solid rgba(74,222,128,0.2);
    border-radius:14px; padding:14px 18px; margin:12px 0;
    font-size:0.82rem; color:rgba(255,255,255,0.7);
}
.resumo-box b { color:#4ade80; }

button[kind="primary"] { border-radius:12px !important; font-weight:700 !important; }
</style>
""", unsafe_allow_html=True)


# ──────────────────────────────────────────────
#  HELPERS SHEETS
# ──────────────────────────────────────────────
def _normalize_private_key(key):
    if not isinstance(key, str): return key
    key = key.replace("\\n", "\n")
    return "".join(ch for ch in key if unicodedata.category(ch)[0] != "C" or ch in ("\n","\r","\t"))

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

@st.cache_data
def _load_df(aba):
    ws = _sheet().worksheet(aba)
    df = get_as_dataframe(ws, evaluate_formulas=True, dtype=str, header=0).dropna(how="all")
    df.columns = [c.strip() for c in df.columns]
    return df

def _safe_load(aba):
    try: return _load_df(aba)
    except: return pd.DataFrame()

def _to_float(x):
    if x is None or str(x).strip() == "": return ""
    s = str(x).strip().replace("R$","").replace(".","").replace(",",".")
    try: return float(s)
    except: return ""

def _to_int(x):
    if x is None or str(x).strip() == "": return ""
    try: return int(float(str(x).strip().replace(",",".")))
    except: return ""

def _gen_id(): return "P-" + datetime.now().strftime("%Y%m%d%H%M%S")

def _msg_ok(msg):
    st.success(msg)
    try: st.cache_data.clear()
    except: pass

def _ensure_ws(name, headers):
    sh = _sheet()
    try:
        ws = sh.worksheet(name)
        cur = get_as_dataframe(ws, evaluate_formulas=False, header=0)
        if cur.empty or any(h not in cur.columns for h in headers):
            cols = list(dict.fromkeys(headers + cur.columns.tolist()))
            ws.clear()
            set_with_dataframe(ws, pd.DataFrame(columns=cols), include_index=False,
                               include_column_header=True, resize=True)
        return ws
    except:
        ws = sh.add_worksheet(title=name, rows=2, cols=max(10,len(headers)))
        set_with_dataframe(ws, pd.DataFrame(columns=headers), include_index=False,
                           include_column_header=True, resize=True)
        return ws

def _append_row(ws, row):
    cur = get_as_dataframe(ws, evaluate_formulas=False, header=0)
    for col in cur.columns: row.setdefault(col,"")
    out = pd.concat([cur, pd.DataFrame([row])], ignore_index=True)
    ws.clear()
    set_with_dataframe(ws, out.fillna(""), include_index=False, include_column_header=True, resize=True)


# ──────────────────────────────────────────────
#  TELEGRAM
# ──────────────────────────────────────────────
def _tg_on():
    try: return str(st.secrets.get("TELEGRAM_ENABLED","0")) == "1"
    except: return False

def _tg_send(msg):
    if not _tg_on(): return
    token   = st.secrets.get("TELEGRAM_TOKEN","")
    chat_id = st.secrets.get("TELEGRAM_CHAT_ID_LOJINHA","") or st.secrets.get("TELEGRAM_CHAT_ID","")
    if not token or not chat_id: return
    try: requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
        json={"chat_id":str(chat_id),"text":msg,"parse_mode":"HTML",
              "disable_web_page_preview":True}, timeout=6)
    except: pass


# ──────────────────────────────────────────────
#  MAPEAMENTO DE COLUNAS
# ──────────────────────────────────────────────
def _pick(df, cands):
    for c in cands:
        if c in df.columns: return c
    low = {c.lower(): c for c in df.columns}
    for c in cands:
        if c.lower() in low: return low[c.lower()]
    return None

def _map_prod(df):
    return {
        "id":        _pick(df, ["ID","Id","Codigo","Código"]),
        "nome":      _pick(df, ["Nome","Produto","Descrição","Descricao"]),
        "categoria": _pick(df, ["Categoria","Grupo"]),
        "unidade":   _pick(df, ["Unidade","Unid"]),
        "forn":      _pick(df, ["Fornecedor","FornecedorNome"]),
        "custo":     _pick(df, ["CustoAtual","Custo","CustoMedio"]),
        "preco":     _pick(df, ["PreçoVenda","PrecoVenda","Preço","Valor"]),
        "estoque":   _pick(df, ["EstoqueAtual","Estoque","QtdEstoque"]),
        "est_min":   _pick(df, ["EstoqueMin","Estoque Min","Minimo"]),
        "lead":      _pick(df, ["LeadTimeDias","LeadTime","Lead Time"]),
        "ativo":     _pick(df, ["Ativo?","Ativo","Status"]),
        "foto":      _pick(df, ["Foto","Imagem","URLImagem","ImagemURL"]),
        "atualizado":_pick(df, ["AtualizadoEm","Atualizado Em","Atualizado"]),
    }

def _map_comp(df):
    return {
        "data": _pick(df, ["Data"]), "nome": _pick(df, ["Produto","Nome"]),
        "unid": _pick(df, ["Unidade"]), "forn": _pick(df, ["Fornecedor"]),
        "qtd":  _pick(df, ["Qtd","Quantidade"]),
        "cu":   _pick(df, ["Custo Unitário","CustoUnit","Custo"]),
        "total":_pick(df, ["Total","ValorTotal"]),
        "id":   _pick(df, ["IDProduto","ID"]),
    }

def _map_mov(df):
    return {
        "data": _pick(df, ["Data"]), "id": _pick(df, ["IDProduto","ID"]),
        "nome": _pick(df, ["Produto","Nome"]),
        "tipo": _pick(df, ["Tipo","Movimento"]), "qtd": _pick(df, ["Qtd","Quantidade"]),
    }

def _map_ven(df):
    return {
        "data": _pick(df, ["Data"]), "id": _pick(df, ["IDProduto","ID"]),
        "nome": _pick(df, ["Produto","Nome"]), "qtd": _pick(df, ["Qtd","Quantidade"]),
    }

def _map_forn(df):
    return {"forn": _pick(df, ["Fornecedor","Nome"]),
            "lead": _pick(df, ["LeadTimeDias","Lead Time","Lead"])}


# ──────────────────────────────────────────────
#  CARREGAR DADOS
# ──────────────────────────────────────────────
ABA = "Produtos"
try:    df = _load_df(ABA)
except Exception as e:
    st.error("Erro ao abrir a aba Produtos."); st.code(str(e)); st.stop()

COL = _map_prod(df)
compras_df = _safe_load("Compras")
movest_df  = _safe_load("MovimentosEstoque")
vendas_df  = _safe_load("Vendas")
forn_df    = _safe_load("Fornecedores")

CMP = _map_comp(compras_df) if not compras_df.empty else {}
MOV = _map_mov(movest_df)   if not movest_df.empty  else {}
VEN = _map_ven(vendas_df)   if not vendas_df.empty  else {}
FD  = _map_forn(forn_df)    if not forn_df.empty    else {}

COMPRAS_HEADERS = ["Data","Produto","Unidade","Fornecedor","Qtd","Custo Unitário","Total","IDProduto","Obs"]
MOV_HEADERS     = ["Data","IDProduto","Produto","Tipo","Qtd","Obs"]


# ──────────────────────────────────────────────
#  FUNÇÕES DE CÁLCULO
# ──────────────────────────────────────────────
def _norm(s):
    return re.sub(r"\s+"," ",(s or "").strip())

def _strip_acc(s):
    return "".join(ch for ch in unicodedata.normalize("NFD",str(s)) if unicodedata.category(ch) != "Mn")

def _key_nf(nome, forn):
    return f"{_strip_acc(_norm(nome)).lower()}|{_strip_acc(_norm(forn)).lower()}"

def _buscar_existente(dfp, nome, forn):
    if dfp is None or dfp.empty or not COL["nome"]: return None, None
    dfc = dfp.copy()
    dfc["_k"] = dfc.apply(lambda r: _key_nf(
        str(r.get(COL["nome"],"")), str(r.get(COL["forn"],"") if COL["forn"] else "")), axis=1)
    k = _key_nf(nome, forn if COL["forn"] else "")
    if k in set(dfc["_k"].tolist()):
        pos = dfc.index[dfc["_k"]==k].tolist()[0]
        return int(pos), dfp.iloc[int(pos)].to_dict()
    return None, None

def _last_cost_unit(nome, forn):
    if compras_df.empty or not CMP: return None, None
    base = compras_df.copy()
    if CMP.get("nome"): base = base[base[CMP["nome"]].astype(str).str.strip().str.lower() == nome.strip().lower()]
    if forn and CMP.get("forn"): base = base[base[CMP["forn"]].astype(str).str.strip().str.lower() == forn.strip().lower()]
    if base.empty: return None, None
    row = base.assign(_d=pd.to_datetime(base.get(CMP.get("data",""),""), errors="coerce")).sort_values("_d", ascending=False).iloc[0]
    c = _to_float(row.get(CMP.get("cu",""),""))
    u = str(row.get(CMP.get("unid",""),"")).strip() or None
    return (c if c not in ("",None) else None), u

def _stock_balance(prod_id, nome):
    saldo = 0; has = False
    if not movest_df.empty and MOV:
        base = movest_df.copy()
        if prod_id and MOV.get("id"): base = base[base[MOV["id"]].astype(str) == str(prod_id)]
        elif MOV.get("nome"): base = base[base[MOV["nome"]].astype(str).str.strip().str.lower() == nome.strip().lower()]
        if not base.empty:
            has = True
            ent = base[base[MOV["tipo"]].astype(str).str.lower().isin(["entrada","compra","ajuste+","in","b entrada"])][MOV["qtd"]].apply(_to_float).sum()
            sai = base[base[MOV["tipo"]].astype(str).str.lower().isin(["saida","venda","ajuste-","out","b saída","b saida"])][MOV["qtd"]].apply(_to_float).sum()
            saldo = (ent or 0) - (sai or 0)
    try: return int(round(saldo,0))
    except: return 0

def _avg30(prod_id, nome):
    if vendas_df.empty or not VEN or not VEN.get("data"): return 0.0
    base = vendas_df.copy()
    base["_d"] = pd.to_datetime(base[VEN["data"]], errors="coerce")
    mx = base["_d"].max()
    if pd.isna(mx): return 0.0
    base = base[base["_d"] >= mx - timedelta(days=30)]
    if prod_id and VEN.get("id"): base = base[base[VEN["id"]].astype(str) == str(prod_id)]
    elif VEN.get("nome"): base = base[base[VEN["nome"]].astype(str).str.strip().str.lower() == nome.strip().lower()]
    if base.empty: return 0.0
    return float(base[VEN["qtd"]].apply(_to_float).sum()) / max((mx - (mx-timedelta(days=30))).days, 1)

def _lead_forn(forn):
    if forn_df.empty or not FD or not forn: return None
    base = forn_df[forn_df[FD["forn"]].astype(str).str.strip().str.lower() == forn.strip().lower()]
    if base.empty: return None
    v = _to_int(base.iloc[0].get(FD["lead"],""))
    return v if v != "" else None

def _calc_emin(avg, lead):
    lt = lead if lead not in (None,"",0) else 7
    return max(math.ceil(avg * lt * 1.2), 5)

def _brl(v):
    try:
        f = float(v)
        return f"R$ {f:,.2f}".replace(",","X").replace(".",",").replace("X",".")
    except: return "—"


# ──────────────────────────────────────────────
#  HEADER
# ──────────────────────────────────────────────
st.markdown("""
<div class="page-header">
  <div>
    <h1>➕ Cadastrar / Editar Produto</h1>
    <div class="sub">Ebenezér Variedades · Gestão de catálogo</div>
  </div>
  <div class="header-badge">📦 Catálogo</div>
</div>
""", unsafe_allow_html=True)

# Modo
col_modo, col_info = st.columns([1.2, 2])
with col_modo:
    modo = st.radio("", ["➕ Cadastrar novo", "✏️ Editar existente"],
                    horizontal=True, label_visibility="collapsed")
with col_info:
    st.markdown("""
    <div style="background:rgba(255,255,255,0.04);border:1px solid rgba(255,255,255,0.08);
    border-radius:12px;padding:10px 16px;font-size:0.8rem;color:rgba(255,255,255,0.5);margin-top:6px">
    💡 Campos como <b style="color:#fff">CustoAtual</b>, <b style="color:#fff">EstoqueMin</b> e
    <b style="color:#fff">LeadTime</b> são calculados automaticamente com base no histórico de compras e vendas.
    </div>
    """, unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)


# ══════════════════════════════════════════════
#  EDITAR EXISTENTE
# ══════════════════════════════════════════════
if "Editar" in modo:
    cc1, cc2, cc3 = st.columns([1, 1, 1])
    with cc1: apenas_ativos = st.toggle("Apenas ativos", value=True)
    with cc2: recalc_auto  = st.toggle("Recalcular campos automáticos", value=True)

    base_edit = df.copy()
    if COL["ativo"] and apenas_ativos:
        base_edit = base_edit[base_edit[COL["ativo"]].astype(str).str.strip().str.lower()
                               .isin(["1","true","sim","ativo","yes"])]

    if base_edit.empty:
        st.info("Nenhum produto ativo encontrado."); st.stop()

    def _fmt_row(r):
        nome = str(r.get(COL["nome"],"(sem nome)"))
        forn = str(r.get(COL["forn"],"")).strip()
        preco= str(r.get(COL["preco"],"")).strip()
        return nome + (f" — {forn}" if forn else "") + (f" · {_brl(preco)}" if preco else "")

    labels = base_edit.apply(_fmt_row, axis=1).tolist()
    escolha = st.selectbox("🔍 Buscar produto (digite para filtrar)", ["(selecione)"] + labels)
    if escolha == "(selecione)": st.stop()

    sel = base_edit.iloc[labels.index(escolha)].to_dict()
    prod_id = str(sel.get(COL["id"],"") if COL["id"] else "")
    foto_url = str(sel.get(COL["foto"],"") if COL["foto"] else "")

    # Preview do produto selecionado
    if foto_url and foto_url.startswith("http"):
        foto_tag = f'<img src="{foto_url}" onerror="this.style.display=\'none\'">'
    else:
        foto_tag = '<div class="prev-card-ph">📦</div>'

    nome_sel  = str(sel.get(COL["nome"],"")).strip()
    forn_sel  = str(sel.get(COL["forn"],"") if COL["forn"] else "").strip()
    preco_sel = str(sel.get(COL["preco"],"") if COL["preco"] else "").strip()
    estq_sel  = _stock_balance(prod_id, nome_sel)

    st.markdown(f"""
    <div class="prev-card">
      {foto_tag}
      <div>
        <div class="prev-nome">{nome_sel}</div>
        <div class="prev-sub">{forn_sel or "Sem fornecedor"} · Estoque: {estq_sel}</div>
        <div class="prev-badge-edit">✏️ Editando</div>
      </div>
    </div>
    """, unsafe_allow_html=True)

    # ── Formulário de edição ──
    st.markdown('<div class="sec-titulo">📋 Dados do produto</div>', unsafe_allow_html=True)
    with st.form("editar_produto"):
        e1, e2, e3 = st.columns([1.6, 1, 1])
        with e1: nome      = st.text_input("Nome", value=nome_sel)
        with e2: categoria = st.text_input("Categoria", value=str(sel.get(COL["categoria"],"") if COL["categoria"] else "").strip())
        with e3: fornecedor= st.text_input("Fornecedor", value=forn_sel)

        e4, e5, e6 = st.columns([1, 1, 1])
        with e4: preco  = st.text_input("Preço venda (R$)", value=preco_sel)
        with e5: foto_i = st.text_input("URL da foto (opcional)", value=foto_url)
        with e6:
            ativo_flag = str(sel.get(COL["ativo"],"") if COL["ativo"] else "").strip().lower() in ["1","true","sim","ativo","yes"]
            ativo = st.toggle("Produto ativo", value=ativo_flag)

        st.markdown('<div style="height:8px"></div>', unsafe_allow_html=True)
        st.markdown('<div class="sec-titulo">🧾 Lançar nova compra (opcional)</div>', unsafe_allow_html=True)

        c1, c2, c3, c4 = st.columns([1, 1, 1, 1])
        with c1: data_comp = st.date_input("Data", value=date.today(), key="dc_e")
        with c2: qtd_comp  = st.text_input("Qtd comprada", placeholder="Ex: 10", key="qc_e")
        with c3: cst_comp  = st.text_input("Custo unit. (R$)", placeholder="Ex: 12,50", key="cu_e")
        with c4: unid_comp = st.text_input("Unidade", value=str(sel.get(COL["unidade"],"") if COL["unidade"] else "un"), key="un_e")
        forn_comp = st.text_input("Fornecedor (compra)", value=fornecedor, key="fo_e")
        obs_comp  = st.text_input("Observações", key="ob_e")

        salvar = st.form_submit_button("💾  Salvar alterações", type="primary", use_container_width=True)

    if salvar:
        if not nome.strip(): st.error("Informe o Nome."); st.stop()
        pf = _to_float(preco)
        if pf == "": st.error("Preço inválido."); st.stop()

        updates = {}
        if COL["nome"]:      updates[COL["nome"]]      = nome.strip()
        if COL["categoria"]: updates[COL["categoria"]]  = categoria.strip()
        if COL["forn"]:      updates[COL["forn"]]       = fornecedor.strip()
        if COL["preco"]:     updates[COL["preco"]]      = f"{pf:.2f}".replace(".",",")
        if COL["foto"]:      updates[COL["foto"]]       = foto_i.strip()
        if COL["ativo"]:     updates[COL["ativo"]]      = "sim" if ativo else "não"
        if COL["atualizado"]:updates[COL["atualizado"]] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")

        qtd_f = _to_float(qtd_comp); cst_f = _to_float(cst_comp)
        fazer_compra = (qtd_f not in ("",None,0)) and (cst_f not in ("",None,0))

        if fazer_compra:
            ws_c = _ensure_ws("Compras", COMPRAS_HEADERS)
            total = round(float(qtd_f)*float(cst_f),2)
            _append_row(ws_c, {"Data":data_comp.strftime("%d/%m/%Y"),"Produto":nome.strip(),
                "Unidade":(unid_comp or "un").strip(),"Fornecedor":(forn_comp or fornecedor or "").strip(),
                "Qtd":str(int(qtd_f)) if float(qtd_f).is_integer() else str(qtd_f).replace(".",","),
                "Custo Unitário":f"{float(cst_f):.2f}".replace(".",","),
                "Total":f"{total:.2f}".replace(".",","),"IDProduto":prod_id,"Obs":obs_comp or ""})
            ws_m = _ensure_ws("MovimentosEstoque", MOV_HEADERS)
            _append_row(ws_m, {"Data":data_comp.strftime("%d/%m/%Y"),"IDProduto":prod_id,
                "Produto":nome.strip(),"Tipo":"entrada",
                "Qtd":str(int(qtd_f)) if float(qtd_f).is_integer() else str(qtd_f).replace(".",","),
                "Obs":f"Compra — {obs_comp or ''}".strip()})
            if COL["custo"]:  updates[COL["custo"]]  = f"{float(cst_f):.2f}".replace(".",",")

        if recalc_auto or fazer_compra:
            custo_h, unid_h = _last_cost_unit(nome, fornecedor)
            lead_h = _lead_forn(fornecedor)
            avg    = _avg30(prod_id, nome)
            emin   = _calc_emin(avg, lead_h)
            if custo_h and COL["custo"]:  updates.setdefault(COL["custo"], f"{custo_h:.2f}".replace(".",","))
            if unid_h and COL["unidade"]: updates.setdefault(COL["unidade"], unid_h)
            if lead_h and COL["lead"]:    updates.setdefault(COL["lead"], str(lead_h))
            saldo = _stock_balance(prod_id, nome)
            if COL["estoque"]:  updates.setdefault(COL["estoque"], str(saldo or 0))
            if COL["est_min"]:  updates[COL["est_min"]] = str(emin)

        id_col = COL["id"] or "ID"
        if id_col not in df.columns: st.error("Coluna ID não encontrada."); st.stop()
        ws  = _sheet().worksheet(ABA)
        df2 = _load_df(ABA)
        ids = df2[id_col].tolist()
        i   = ids.index(sel.get(id_col,"")) if sel.get(id_col,"") in ids else None
        if i is None: st.error("Linha não encontrada."); st.stop()
        for col, val in updates.items():
            if col in df2.columns: df2.loc[i, col] = val
        ws.clear()
        set_with_dataframe(ws, df2.fillna(""), include_index=False, include_column_header=True, resize=True)
        _msg_ok("✅ Produto atualizado com sucesso!")
        _tg_send(f"✏️ <b>Produto atualizado</b>\n• <b>{nome.strip()}</b>\nFornecedor: {fornecedor or '—'}\nPreço: <b>R$ {pf:.2f}</b>".replace(".",","))


# ══════════════════════════════════════════════
#  CADASTRAR NOVO
# ══════════════════════════════════════════════
else:
    st.markdown('<div class="sec-titulo">📋 Dados do produto</div>', unsafe_allow_html=True)

    with st.form("cadastrar_produto"):
        n1, n2, n3 = st.columns([1.6, 1, 1])
        with n1: nome       = st.text_input("Nome *", placeholder="Ex: Detergente Ypê 500ml")
        with n2: categoria  = st.text_input("Categoria", placeholder="Ex: Limpeza")
        with n3: fornecedor = st.text_input("Fornecedor", placeholder="Ex: Ypê Distribuidora")

        n4, n5a, n5b = st.columns([1, 1, 1])
        with n4:  preco = st.text_input("Preço venda (R$) *", placeholder="Ex: 4,90")
        unidades = ["un","L","kg","g","ml","cx","pct","Outro…"]
        with n5a: un_sel   = st.selectbox("Unidade", unidades)
        with n5b: un_outro = st.text_input("Se 'Outro…', qual?", placeholder="Ex: rolo, par")
        unidade_final = (un_outro.strip() if un_sel == "Outro…" else un_sel)

        n6, n7 = st.columns([2, 1])
        with n6: foto_url_n = st.text_input("URL da foto (opcional)", placeholder="https://...")
        with n7: ativo = st.toggle("Produto ativo", value=True)

        st.markdown('<div style="height:4px"></div>', unsafe_allow_html=True)
        st.markdown('<div class="sec-titulo">📦 Estoque inicial (recomendado)</div>', unsafe_allow_html=True)

        s1, s2, s3, s4 = st.columns([1, 1, 1, 1])
        with s1: qtd_comp  = st.text_input("Qtd comprada", placeholder="Ex: 10")
        with s2: cst_comp  = st.text_input("Custo unit. (R$)", placeholder="Ex: 2,50")
        with s3: unid_comp = st.text_input("Unidade (compra)", value=unidade_final or "un")
        with s4: data_comp = st.date_input("Data da compra", value=date.today())
        forn_comp = st.text_input("Fornecedor (compra)", value="")
        obs_comp  = st.text_input("Observações (opcional)")

        dedup = st.checkbox("Se já existir, atualizar ao invés de duplicar", value=True)

        salvar = st.form_submit_button("➕  Cadastrar produto", type="primary", use_container_width=True)

    # Preview em tempo real da foto (fora do form)
    if foto_url_n if "foto_url_n" in dir() else False:
        pass  # só mostra após submit

    if salvar:
        if not nome.strip(): st.error("Informe o Nome."); st.stop()
        pf = _to_float(preco)
        if pf == "": st.error("Preço inválido. Use números (ex: 4,90)."); st.stop()

        idx_ex, row_ex = _buscar_existente(df, nome, fornecedor)
        custo_h, unid_h = _last_cost_unit(nome, fornecedor)
        lead_h = _lead_forn(fornecedor)
        saldo  = _stock_balance(None, nome)
        avg    = _avg30(None, nome)
        emin   = _calc_emin(avg, lead_h)

        ws      = _sheet().worksheet(ABA)
        df_atual= _load_df(ABA)

        qtd_f = _to_float(qtd_comp); cst_f = _to_float(cst_comp)
        tem_estoque = (qtd_f not in ("",None,0)) and (cst_f not in ("",None,0))

        def _gravar_compra_mov(novo_id, nome_p, forn_p):
            ws_c = _ensure_ws("Compras", COMPRAS_HEADERS)
            total = round(float(qtd_f)*float(cst_f), 2)
            _append_row(ws_c, {"Data":data_comp.strftime("%d/%m/%Y"),"Produto":nome_p,
                "Unidade":(unid_comp or unidade_final or "un").strip(),
                "Fornecedor":(forn_comp or forn_p or "").strip(),
                "Qtd":str(int(qtd_f)) if float(qtd_f).is_integer() else str(qtd_f).replace(".",","),
                "Custo Unitário":f"{float(cst_f):.2f}".replace(".",","),
                "Total":f"{total:.2f}".replace(".",","),"IDProduto":novo_id,"Obs":obs_comp or ""})
            ws_m = _ensure_ws("MovimentosEstoque", MOV_HEADERS)
            _append_row(ws_m, {"Data":data_comp.strftime("%d/%m/%Y"),"IDProduto":novo_id,
                "Produto":nome_p,"Tipo":"entrada",
                "Qtd":str(int(qtd_f)) if float(qtd_f).is_integer() else str(qtd_f).replace(".",","),
                "Obs":f"Compra inicial — {obs_comp or ''}".strip()})

        if dedup and idx_ex is not None:
            # ── Atualizar existente ──
            novo_id = str(row_ex.get(COL["id"] or "ID",""))
            up = {}
            if COL["nome"]:      up[COL["nome"]]      = _norm(nome)
            if COL["categoria"]: up[COL["categoria"]]  = _norm(categoria)
            if COL["forn"]:      up[COL["forn"]]       = _norm(fornecedor)
            if COL["preco"]:     up[COL["preco"]]      = f"{float(pf):.2f}".replace(".",",")
            if COL["foto"]:      up[COL["foto"]]       = foto_url_n.strip()
            if COL["unidade"]:   up[COL["unidade"]]    = unidade_final or (unid_h or "")
            if custo_h and COL["custo"]: up.setdefault(COL["custo"], f"{custo_h:.2f}".replace(".",","))
            if COL["estoque"]:   up.setdefault(COL["estoque"], str(saldo or 0))
            if COL["est_min"]:   up[COL["est_min"]]    = str(emin)
            if lead_h and COL["lead"]: up.setdefault(COL["lead"], str(lead_h))
            if COL["ativo"]:     up[COL["ativo"]]      = "sim" if ativo else "não"
            if COL["atualizado"]:up[COL["atualizado"]] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")

            for col, val in up.items():
                if col in df_atual.columns: df_atual.loc[idx_ex, col] = val

            if tem_estoque:
                _gravar_compra_mov(novo_id, nome.strip(), fornecedor)
                if COL["custo"]:   df_atual.loc[idx_ex, COL["custo"]]  = f"{float(cst_f):.2f}".replace(".",",")
                if COL["estoque"]: df_atual.loc[idx_ex, COL["estoque"]] = str(_stock_balance(novo_id, nome.strip()) or 0)

            ws.clear()
            set_with_dataframe(ws, df_atual.fillna(""), include_index=False, include_column_header=True, resize=True)
            _msg_ok("🔄 Produto já existia — dados atualizados sem duplicar!")
            _tg_send(f"🆙 <b>Produto atualizado (sem duplicar)</b>\n• <b>{nome.strip()}</b>\nPreço: <b>R$ {pf:.2f}</b>".replace(".",","))

        else:
            # ── Cadastrar novo ──
            novo_id  = _gen_id()
            new_row  = {}
            if COL["id"]:        new_row[COL["id"]]       = novo_id
            if COL["nome"]:      new_row[COL["nome"]]     = _norm(nome)
            if COL["categoria"]: new_row[COL["categoria"]]= _norm(categoria)
            if COL["forn"]:      new_row[COL["forn"]]     = _norm(fornecedor)
            if COL["preco"]:     new_row[COL["preco"]]    = f"{float(pf):.2f}".replace(".",",")
            if COL["foto"]:      new_row[COL["foto"]]     = foto_url_n.strip()
            if COL["unidade"]:   new_row[COL["unidade"]]  = unidade_final or (unid_h or "")
            if custo_h and COL["custo"]: new_row[COL["custo"]] = f"{custo_h:.2f}".replace(".",",")
            if COL["estoque"]:   new_row[COL["estoque"]]  = str(saldo or 0)
            if COL["est_min"]:   new_row[COL["est_min"]]  = str(emin)
            if lead_h and COL["lead"]: new_row[COL["lead"]] = str(lead_h)
            if COL["ativo"]:     new_row[COL["ativo"]]    = "sim" if ativo else "não"
            if COL["atualizado"]:new_row[COL["atualizado"]]= datetime.now().strftime("%d/%m/%Y %H:%M:%S")

            if tem_estoque:
                _gravar_compra_mov(novo_id, nome.strip(), fornecedor)
                if COL["custo"]:   new_row[COL["custo"]]   = f"{float(cst_f):.2f}".replace(".",",")
                if COL["estoque"]: new_row[COL["estoque"]]  = str(_stock_balance(None, nome.strip()) or 0)

            for col in df_atual.columns: new_row.setdefault(col,"")
            df_out = pd.concat([df_atual, pd.DataFrame([new_row])], ignore_index=True)
            set_with_dataframe(ws, df_out.fillna(""), include_index=False, include_column_header=True, resize=True)

            # Mostrar resumo do que foi cadastrado
            st.markdown(f"""
            <div class="resumo-box">
              ✅ <b>{nome.strip()}</b> cadastrado com sucesso!<br>
              Preço venda: <b>{_brl(pf)}</b>
              {" · Custo: <b>" + _brl(cst_f) + "</b>" if tem_estoque and cst_f else ""}
              {" · Estoque inicial: <b>" + str(int(float(qtd_f))) + " " + (unidade_final or "un") + "</b>" if tem_estoque else ""}
            </div>
            """, unsafe_allow_html=True)

            if foto_url_n and foto_url_n.startswith("http"):
                st.image(foto_url_n, width=120, caption=nome.strip())

            st.balloons()
            _tg_send(
                f"➕ <b>Novo produto cadastrado</b>\n• <b>{nome.strip()}</b>\n"
                f"Unidade: {unidade_final or '—'}\nFornecedor: {fornecedor or '—'}\n"
                f"Preço: <b>R$ {pf:.2f}</b>".replace(".",","))

st.markdown("<br>", unsafe_allow_html=True)
st.markdown("---")
st.caption("💡 Campos automáticos: EstoqueMin calculado via média 30 dias × lead time × 1,2 de segurança. CustoAtual vem da última compra.")
