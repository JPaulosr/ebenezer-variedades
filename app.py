# -*- coding: utf-8 -*-
# Dashboard — Ebenezér Variedades (página completa)
import json, unicodedata, re
from collections.abc import Mapping
from datetime import datetime, date, timedelta

import streamlit as st
import pandas as pd
import plotly.express as px
import gspread
from gspread_dataframe import get_as_dataframe
from google.oauth2.service_account import Credentials

# =========================
# Setup
# =========================
st.set_page_config(page_title="Ebenezér Variedades — Dashboard", page_icon="🧮", layout="wide")
st.title("🧮 Dashboard — Ebenezér Variedades")

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
        st.error("🛑 private_key inválida. Cole a chave completa (BEGIN/END)."); st.stop()
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
if prod.empty:
    st.warning("Aba Produtos está vazia.")
else:
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
# Filtros
# =========================
st.sidebar.header("Filtros")
preset = st.sidebar.selectbox("Período", ["Hoje","Últimos 7 dias","Últimos 30 dias","Mês atual","Personalizado"], index=2)
hoje = date.today()
if preset == "Hoje":
    dt_ini, dt_fim = hoje, hoje
elif preset == "Últimos 7 dias":
    dt_ini, dt_fim = hoje - timedelta(days=6), hoje
elif preset == "Últimos 30 dias":
    dt_ini, dt_fim = hoje - timedelta(days=29), hoje
elif preset == "Mês atual":
    dt_ini, dt_fim = hoje.replace(day=1), hoje
else:
    c1, c2 = st.sidebar.columns(2)
    with c1: dt_ini = st.date_input("De:", value=hoje - timedelta(days=29))
    with c2: dt_fim = st.date_input("Até:", value=hoje)

inclui_estornos = st.sidebar.checkbox("Incluir estornos (CN-/ESTORNO)", value=False)
cats  = sorted(pd.Series(prod["Categoria"].dropna().astype(str).unique()).tolist()) if not prod.empty else []
forns = sorted(pd.Series(prod["Fornecedor"].dropna().astype(str).unique()).tolist()) if not prod.empty else []
cat_sel  = st.sidebar.multiselect("Categoria", cats)
forn_sel = st.sidebar.multiselect("Fornecedor", forns)
apenas_ativos   = st.sidebar.checkbox("Somente ativos", value=True)
ocultar_zerados = st.sidebar.checkbox("Ocultar itens com estoque zerado", value=True)
busca = st.sidebar.text_input("Buscar por nome/ID")

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
        "TotalCupom":v[col_totcup]if col_totcup else None,
        "CupomStatus":v[col_stat] if col_stat else None
    })

    out["Data_d"]       = out["Data"].apply(_parse_date_any)
    out["QtdNum"]       = out["Qtd"].apply(_to_float)
    out["PrecoNum"]     = out["PrecoUnit"].apply(_to_float)
    out["TotalNum"]     = out["TotalLinha"].apply(_to_float)
    out["DescNum"]      = out["Desconto"].apply(_to_float)
    out["TotalCupomNum"]= out["TotalCupom"].apply(_to_float)
    out["VendaID"]      = out["VendaID"].astype(str).fillna("")
    out["is_estorno"]   = out["VendaID"].str.startswith("CN-") | (out["CupomStatus"].astype(str).str.upper()=="ESTORNO")
    out["KeyID"]        = out["IDProduto"].apply(_canon_id)

    out = out[(out["Data_d"]>=dt_ini) & (out["Data_d"]<=dt_fim)]
    if not inclui_estornos:
        out = out[~out["is_estorno"]]

    cupom_grp = out.groupby("VendaID", dropna=True).agg({
        "Data_d":"first","Forma":"first","TotalNum":"sum","DescNum":"max","TotalCupomNum":"max"
    }).reset_index()
    cupom_grp["ReceitaCupom"] = cupom_grp.apply(
        lambda r: r["TotalCupomNum"] if r["TotalCupomNum"]>0 else max(0.0, r["TotalNum"] - r["DescNum"]), axis=1
    )
    return out, cupom_grp

vendas, cupom_grp = _normalize_vendas_period(vend_raw)

# =========================
# Compras (histórico) — SOMENTE Custo Unitário
# =========================
def _normalize_compras_all_with_date(c: pd.DataFrame) -> pd.DataFrame:
    """
    Retorna KeyID, QtdNum, CustoNum (custo unitário efetivo) e Data_d.
    - Aceita cabeçalhos variados (IDProduto/ID/Código/SKU; Custo Unitário/CustoUnit/PrecoCusto/etc).
    - Limpa "R$" e formata número pt-BR.
    - Se não houver custo unitário, usa Total/Qtd.
    - Se houver FreteRateado (ou Frete), adiciona ao custo unitário.
    - Ignora linhas com Qtd <= 0 ou ID vazio.
    """
    if c is None or c.empty:
        return pd.DataFrame(columns=["KeyID", "QtdNum", "CustoNum", "Data_d"])

    d = c.copy()
    d.columns = [str(x).strip() for x in d.columns]

    def _norm(s: str) -> str:
        import unicodedata, re
        s = unicodedata.normalize("NFKD", str(s))
        s = "".join(ch for ch in s if not unicodedata.combining(ch))
        s = s.lower().strip()
        s = re.sub(r"[\s_]+", "", s)
        return s

    # mapa normalizado -> coluna original
    norm2orig = {_norm(col): col for col in d.columns}

    def _pick(*aliases):
        for a in aliases:
            col = norm2orig.get(_norm(a))
            if col:
                return col
        return None

    # Colunas possíveis
    col_idp = _pick("IDProduto", "ID do Produto", "ProdutoID", "Produto Id", "SKU", "COD", "Código", "Codigo", "ID")
    col_qtd = _pick("Qtd", "Quantidade", "Qtde", "Qde", "QTD")
    col_cu  = _pick("Custo Unitário", "CustoUnitário", "Custo Unit", "CustoUnit",
                    "Preço de Custo", "PrecoCusto", "Preço Custo", "Custo")
    col_tot = _pick("Total", "Total da Linha", "TotalLinha", "Valor Total")
    col_dat = _pick("Data", "Emissao", "Emissão")
    col_fre = _pick("FreteRateado", "Frete Rateado", "Frete")

    def to_f(x): 
        return _to_float(x, default=0.0)

    out = pd.DataFrame({
        "KeyID":   d[col_idp].apply(_canon_id) if col_idp in d else "",
        "QtdNum":  d[col_qtd].apply(to_f)      if col_qtd in d else 0.0,
        "Data_d":  d[col_dat].apply(_parse_date_any) if col_dat in d else None,
    })

    # custo unitário direto
    if col_cu in d:
        out["CustoNum"] = d[col_cu].apply(to_f)
    else:
        out["CustoNum"] = 0.0

    # fallback: Total / Qtd
    if col_tot in d:
        total_num = d[col_tot].apply(to_f)
        mask_fb = (out["CustoNum"] <= 0) & (out["QtdNum"] > 0)
        out.loc[mask_fb, "CustoNum"] = (total_num[mask_fb] / out.loc[mask_fb, "QtdNum"]).astype(float)

    # frete rateado por item (se existir)
    if col_fre in d:
        frete = d[col_fre].apply(to_f)
        out["CustoNum"] = (out["CustoNum"].fillna(0.0) + frete.fillna(0.0)).astype(float)

    # filtros finais
    out = out[(out["KeyID"] != "") & (out["QtdNum"] > 0)]
    out.loc[abs(out["CustoNum"]) > 1e6, "CustoNum"] = 0.0
    return out[["KeyID", "QtdNum", "CustoNum", "Data_d"]]

c_all = _normalize_compras_all_with_date(comp_raw)

# =========================
# Ajustes (opcional) — entradas/saídas manuais
# =========================
def _normalize_ajustes(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["KeyID","QtdNum"])
    d = df.copy(); d.columns = [x.strip() for x in d.columns]
    col_idp = _first_col(d, ["IDProduto","ID do Produto","ProdutoID","Produto Id","SKU","COD","Código","Codigo","ID"])
    col_qtd = _first_col(d, ["Qtd","Quantidade","Qtde","Qde","Ajuste","Delta"])
    if not col_idp or not col_qtd:
        return pd.DataFrame(columns=["KeyID","QtdNum"])
    out = pd.DataFrame({"KeyID": d[col_idp].apply(_canon_id), "QtdNum": d[col_qtd].apply(_to_float)})
    out = out[(out["KeyID"]!="") & (out["QtdNum"]!=0)]
    return out

try:
    ajustes_raw = carregar_aba("Ajustes")
except Exception:
    ajustes_raw = pd.DataFrame()
a_all = _normalize_ajustes(ajustes_raw)

# =========================
# Contagem/Estoque Inicial (opcional)
# =========================
def _normalize_contagem_inicial(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty: return pd.DataFrame(columns=["KeyID", "SaldoInicial"])
    d = df.copy(); d.columns = [x.strip() for x in d.columns]
    col_idp = _first_col(d, ["IDProduto","ID do Produto","ProdutoID","Produto Id","SKU","COD","Código","Codigo","ID"])
    col_qtd = _first_col(d, ["Qtd","Quantidade","Qtde","Qde","EstoqueInicial","SaldoInicial"])
    if not col_idp or not col_qtd: return pd.DataFrame(columns=["KeyID","SaldoInicial"])
    out = pd.DataFrame({"KeyID": d[col_idp].apply(_canon_id), "SaldoInicial": d[col_qtd].apply(_to_float)})
    out = out[out["KeyID"]!=""]; return out

try:
    cont_raw = carregar_aba("ContagemEstoque")
except Exception:
    try: cont_raw = carregar_aba("EstoqueInicial")
    except Exception: cont_raw = pd.DataFrame()

cont = _normalize_contagem_inicial(cont_raw)
saldo_inicial = cont.groupby("KeyID")["SaldoInicial"].sum() if not cont.empty else pd.Series(dtype=float)

# =========================
# Estoque: Entradas/Saídas/Ajustes + SaldoInicial
# =========================
v_all = vendas.copy() if not vendas.empty else pd.DataFrame(columns=["KeyID","QtdNum"])
entradas = c_all.groupby("KeyID")["QtdNum"].sum() if not c_all.empty else pd.Series(dtype=float)
saidas   = v_all.groupby("KeyID")["QtdNum"].sum() if not v_all.empty else pd.Series(dtype=float)
ajustes  = a_all.groupby("KeyID")["QtdNum"].sum() if not a_all.empty else pd.Series(dtype=float)

calc = pd.DataFrame({"Entradas": entradas, "Saidas": saidas, "Ajustes": ajustes}).fillna(0.0)
saldo_inicial_df = saldo_inicial.to_frame("SaldoInicial") if not saldo_inicial.empty else pd.DataFrame(columns=["SaldoInicial"])
calc = calc.join(saldo_inicial_df, how="left")
calc["SaldoInicial"] = calc["SaldoInicial"].fillna(0.0)
calc["EstoqueCalc"]  = calc["SaldoInicial"] + calc["Entradas"] - calc["Saidas"] + calc["Ajustes"]

# =========================
# Custos: último Custo Unitário + FatorCusto
# =========================
def _last_cost_per_product(comp_df: pd.DataFrame) -> pd.Series:
    if comp_df.empty: return pd.Series(dtype=float)
    d = comp_df.copy()
    d["_ord"] = range(len(d))
    d = d[d["CustoNum"].apply(lambda x: _to_float(x) > 0)]
    if d.empty: return pd.Series(dtype=float)
    d = d.sort_values(["KeyID","Data_d","_ord"]).groupby("KeyID").tail(1)
    return d.set_index("KeyID")["CustoNum"]

last_cost = _last_cost_per_product(c_all)  # custos (float) por KeyID

calc = calc.reset_index().rename(columns={"index":"KeyID"})
prod_calc = prod.copy() if not prod.empty else pd.DataFrame()
if not prod_calc.empty and "KeyID" in prod_calc.columns:
    prod_calc = prod_calc.merge(calc, how="left", on="KeyID", suffixes=("_orig", ""))

for col in ["EstoqueCalc","Entradas","Saidas","Ajustes","SaldoInicial","FatorCusto"]:
    if col not in prod_calc.columns: prod_calc[col] = 0.0
prod_calc["FatorCusto"] = prod_calc["FatorCusto"].fillna(1.0)

fator_map = (prod_calc.set_index("KeyID")["FatorCusto"].fillna(1.0)
             if "FatorCusto" in prod_calc.columns else pd.Series(dtype=float))

def _choose_cost_base(keyid: str) -> float:
    """Último Custo Unitário (compras)."""
    v = float(last_cost.get(str(keyid), 0.0) or 0.0)
    return v

def _choose_cost_final(keyid: str) -> float:
    base  = _choose_cost_base(keyid)
    fator = float(fator_map.get(str(keyid), 1.0) or 1.0)
    return base * fator

prod_calc["CustoAtual"] = prod_calc["KeyID"].astype(str).map(_choose_cost_final).astype(float)
prod_calc["ValorEstoqueCalc"] = prod_calc["CustoAtual"].fillna(0) * prod_calc["EstoqueCalc"].fillna(0)

# =========================
# KPIs (faturamento / cupons / itens)
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

if not vendas.empty:
    faturamento = cupom_grp["ReceitaCupom"].sum()
    num_cupons  = cupom_grp["VendaID"].nunique()
    itens_vendidos = vendas["QtdNum"].sum()
else:
    faturamento = 0.0; num_cupons = 0; itens_vendidos = 0.0

# COGS + lucro
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
# KPIs (cards)
# =========================
k1,k2,k3,k4,k5 = st.columns(5)
k1.metric("💵 Faturamento (período)", _fmt_brl(faturamento))
k2.metric("🧾 Cupons", f"{num_cupons}", f"Ticket {_fmt_brl(ticket_medio)}")
k3.metric("📦 Itens vendidos", f"{itens_vendidos:.0f}")
k4.metric("📊 Lucro bruto (aprox.)", _fmt_brl(lucro_bruto), f"{margem_bruta:.1f}% margem")
k5.metric("🧮 Caixa (Vendas - Compras)", _fmt_brl(caixa_periodo))
st.caption(f"Período: {dt_ini.strftime('%d/%m/%Y')} a {dt_fim.strftime('%d/%m/%Y')}  •  Estornos {'INCLUÍDOS' if inclui_estornos else 'EXCLUÍDOS'}")

# =========================
# Vendas vs Compras por dia
# =========================
st.subheader("📆 Vendas vs Compras por dia")
def _daily(df_in, date_col, val_col, label):
    if df_in is None or df_in.empty: return pd.DataFrame(columns=["Data","Valor","Tipo"])
    d = df_in.copy()
    d[date_col] = d[date_col].apply(_parse_date_any)
    g = d.groupby(date_col)[val_col].sum().reset_index().rename(columns={date_col:"Data", val_col:"Valor"})
    g["Tipo"] = label
    return g

g_v = _daily(cupom_grp if not vendas.empty else pd.DataFrame(), "Data_d", "ReceitaCupom", "Vendas")
g_c = _daily(compras_periodo, "Data_d", "TotalNum", "Compras")
serie = pd.concat([g_v, g_c], ignore_index=True)

if not serie.empty:
    fig = px.bar(serie, x="Data", y="Valor", color="Tipo", barmode="group")
    fig.update_layout(yaxis_title="R$", xaxis_title="")
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("Sem dados no período selecionado.")
st.divider()

# =========================
# Estoque — visão geral
# =========================
st.subheader("📦 Estoque — visão geral")
if prod_calc.empty:
    st.info("Sem produtos para exibir.")
else:
    # Botão de sincronizar custo atual (linha a linha, numérico)
    with st.expander("⚙️ Sincronizar 'CustoAtual' na aba Produtos", expanded=False):
        st.caption("Atualiza 'CustoAtual' com o último Custo Unitário × FatorCusto (linha a linha por ID).")

        import unicodedata, re
        def _norm(s: str) -> str:
            s = unicodedata.normalize("NFKD", str(s))
            s = "".join(ch for ch in s if not unicodedata.combining(ch))
            s = s.lower().strip()
            s = re.sub(r"[\s_]+", "", s)
            return s

        def _find_col_idx(header, aliases):
            norm_map = {_norm(h): i+1 for i, h in enumerate(header)}  # 1-based index
            for a in aliases:
                idx = norm_map.get(_norm(a))
                if idx:
                    return idx
            return None

        if st.button("✍️ Atualizar coluna CustoAtual na planilha"):
            try:
                from gspread.utils import rowcol_to_a1
                sh = conectar_sheets()
                ws = sh.worksheet(ABA_PROD)
                header = ws.row_values(1)

                id_col_idx    = _find_col_idx(header, ["ID","Codigo","Código","ProdutoID","SKU"])
                custo_col_idx = _find_col_idx(header, ["CustoAtual","Custo Atual","Custo_Atual"])

                if not id_col_idx or not custo_col_idx:
                    st.error("⚠️ Cabeçalho precisa ter colunas equivalentes a 'ID' e 'CustoAtual' na aba Produtos.")
                    st.stop()

                ids_sheet = ws.col_values(id_col_idx)[1:]  # pula header
                start_row = 2
                end_row   = start_row + len(ids_sheet) - 1
                if end_row < start_row:
                    st.warning("Não há linhas de produtos para atualizar.")
                    st.stop()

                cell_range = f"{rowcol_to_a1(start_row, custo_col_idx)}:{rowcol_to_a1(end_row, custo_col_idx)}"
                cells = ws.range(cell_range)

                for i, cell in enumerate(cells):
                    raw_id = ids_sheet[i] if i < len(ids_sheet) else ""
                    keyid  = _canon_id(raw_id)
                    val    = float(_choose_cost_final(keyid)) if keyid else 0.0
                    cell.value = val

                ws.update_cells(cells, value_input_option="USER_ENTERED")
                st.success("✅ CustoAtual sincronizado com sucesso")
                st.session_state["_force_refresh"] = True
                st.rerun()

            except Exception as e:
                st.error(f"❌ Falha ao atualizar CustoAtual: {e}")

    # Filtros visuais
    m = pd.Series(True, index=prod_calc.index)
    if cat_sel and "Categoria" in prod_calc.columns:  m &= prod_calc["Categoria"].astype(str).isin(cat_sel)
    if forn_sel and "Fornecedor" in prod_calc.columns: m &= prod_calc["Fornecedor"].astype(str).isin(forn_sel)
    if apenas_ativos and "Ativo" in prod_calc.columns:
        prod_calc["Ativo"] = prod_calc["Ativo"].astype(str).str.lower()
        m &= (prod_calc["Ativo"]=="sim")
    if busca:
        s = busca.lower()
        m &= prod_calc.apply(lambda r: s in " ".join([str(x).lower() for x in r.values]), axis=1)

    dfv = prod_calc[m].copy()
    if "EstoqueCalc" in dfv.columns and ocultar_zerados:
        dfv = dfv[dfv["EstoqueCalc"].fillna(0).astype(float) != 0.0]

    estq_min_col = "EstoqueMin" if "EstoqueMin" in dfv.columns else None
    total_produtos = len(dfv)
    valor_estoque  = float(dfv["ValorEstoqueCalc"].fillna(0).sum()) if "ValorEstoqueCalc" in dfv.columns else 0.0
    abaixo_min = int((dfv["EstoqueCalc"].fillna(0) <= dfv[estq_min_col].fillna(0)).sum()) if estq_min_col else 0

    k1,k2,k3 = st.columns(3)
    k1.metric("Produtos exibidos", f"{total_produtos}")
    k2.metric("💰 Valor em estoque", _fmt_brl(valor_estoque))
    k3.metric("⚠️ Abaixo do mínimo", f"{abaixo_min}")

    st.markdown("**⚠️ Itens abaixo do mínimo / sugestão de compra**")
    if estq_min_col:
        alert = dfv[(dfv[estq_min_col].fillna(0) > 0) & (dfv["EstoqueCalc"].fillna(0) <= dfv[estq_min_col].fillna(0))].copy()
        if not alert.empty:
            alert["SugestaoCompra"] = (alert[estq_min_col].fillna(0)*2 - alert["EstoqueCalc"].fillna(0)).clip(lower=0).round()
            cols_alert = [c for c in ["ID","Nome","Categoria","Fornecedor","EstoqueCalc",estq_min_col,"SugestaoCompra","LeadTimeDias"] if c in alert.columns]
            st.dataframe(alert[cols_alert].rename(columns={"EstoqueCalc":"EstoqueAtual", estq_min_col:"EstoqueMin"}),
                         use_container_width=True, hide_index=True)
        else:
            st.info("Nenhum item abaixo do mínimo.")
    st.divider()

    st.markdown("**🏆 Top 10 — Valor em estoque**")
    if "ValorEstoqueCalc" in dfv.columns and dfv["ValorEstoqueCalc"].fillna(0).sum() > 0:
        top = dfv.sort_values("ValorEstoqueCalc", ascending=False).head(10)
        c1,c2 = st.columns([1.2,1])
        with c1:
            fig = px.bar(top, x="Nome", y="ValorEstoqueCalc",
                         hover_data=[c for c in ["EstoqueCalc","CustoAtual","Categoria"] if c in top.columns])
            fig.update_layout(xaxis_title="", yaxis_title="R$ em estoque")
            st.plotly_chart(fig, use_container_width=True)
        with c2:
            cols = [c for c in ["ID","Nome","Categoria","EstoqueCalc","CustoAtual","ValorEstoqueCalc"] if c in top.columns]
            st.dataframe(top[cols].rename(columns={"EstoqueCalc":"EstoqueAtual","ValorEstoqueCalc":"ValorEstoque"}),
                         use_container_width=True, hide_index=True, height=420)
    else:
        st.info("Sem valor em estoque (custo/estoque ainda não cadastrados).")
    st.divider()

    st.markdown("**📋 Lista de produtos (filtrada)**")
    cols_show = [c for c in ["ID","Nome","Categoria","Fornecedor","CustoAtual","EstoqueCalc","EstoqueMin","ValorEstoqueCalc","Ativo","FatorCusto"] if c in dfv.columns]
    st.dataframe(dfv[cols_show].rename(columns={"EstoqueCalc":"EstoqueAtual","ValorEstoqueCalc":"ValorEstoque"}),
                 use_container_width=True, hide_index=True)

# =========================
# 🔎 Auditoria (ajuda a validar lucro/COGS)
# =========================
with st.expander("🔎 Auditoria de custos (período)", expanded=False):
    if not vendas.empty:
        audit = vendas.copy()
        audit = audit[audit["KeyID"] != ""]
        audit["QtdNum"] = audit["QtdNum"].astype(float)
        _custo_ref = prod_calc.set_index("KeyID")["CustoAtual"].astype(float) if not prod_calc.empty else pd.Series(dtype=float)
        audit["_CustoUnit"]   = audit["KeyID"].map(lambda k: float(_custo_ref.get(str(k), 0.0) or 0.0))
        audit["_CustoLinha"]  = audit["QtdNum"] * audit["_CustoUnit"]
        audit["_ReceitaLinha"]= audit["TotalNum"].astype(float)

        g = audit.groupby("KeyID", dropna=True).agg(
            Qtd=("QtdNum","sum"),
            CustoUnit=("_CustoUnit","first"),
            Custo=("_CustoLinha","sum"),
            Receita=("_ReceitaLinha","sum"),
        ).reset_index()
        if "Nome" in prod_calc.columns:
            g = g.merge(prod_calc[["KeyID","Nome"]], how="left", on="KeyID")
        g["Margem%"] = (g["Receita"] - g["Custo"]) / g["Receita"] * 100
        cols = [c for c in ["KeyID","Nome","Qtd","CustoUnit","Custo","Receita","Margem%"] if c in g.columns]
        st.dataframe(g[cols].sort_values("Receita", ascending=False), use_container_width=True)
        st.write("Totais → Receita:", _fmt_brl(audit["_ReceitaLinha"].sum()),
                 " | COGS:", _fmt_brl(audit["_CustoLinha"].sum()),
                 " | Lucro Bruto:", _fmt_brl(audit["_ReceitaLinha"].sum() - audit["_CustoLinha"].sum()))
    else:
        st.info("Sem vendas no período.")
