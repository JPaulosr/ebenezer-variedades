# -*- coding: utf-8 -*-
# pages/04_estoque.py — Estoque (MovimentosEstoque como fonte única) + busca + auto-refresh (UI moderna com cards)

from datetime import date, datetime
from pathlib import Path

import streamlit as st
import pandas as pd

# =========================
# UI BASE / TEMA
# =========================
st.set_page_config(page_title="Estoque — Movimentos & Ajustes", page_icon="📦", layout="wide")

st.markdown("""
<style>
:root{
  --bg: rgba(255,255,255,.03);
  --bg2: rgba(255,255,255,.06);
  --borda: rgba(255,255,255,.12);
  --muted: rgba(255,255,255,.65);
}
.block-container { padding-top: 1.2rem; }
.kpi{border:1px solid var(--borda); background:var(--bg); padding:1rem 1.1rem; border-radius:16px;}
.kpi h3{margin:.2rem 0 .6rem 0; font-size:1.05rem; color:var(--muted); font-weight:600}
.kpi .big{font-size:1.8rem; font-weight:800; line-height:1.1}
.kpi .sub{font-size:.9rem; color:var(--muted)}
.card{border:1px solid var(--borda); background:var(--bg); padding:1rem; border-radius:16px; margin:.4rem 0 1rem 0;}
.card h3{margin:0 0 .6rem 0}
.badge{display:inline-block; padding:.15rem .5rem; border-radius:999px; border:1px solid var(--borda); background:var(--bg2); font-size:.78rem; color:var(--muted)}
.small{color:var(--muted); font-size:.86rem}
.stDataFrame{border-radius:14px; overflow:hidden; border:1px solid var(--borda);}
hr{border:0; border-top:1px solid var(--borda); margin:1rem 0}
</style>
""", unsafe_allow_html=True)

# ---------- refresh automático ----------
if st.session_state.pop("_first_load_estoque", True):
    st.cache_data.clear()
st.session_state.setdefault("_first_load_estoque", False)


from utils.sheets import (
    sheet, carregar_aba, garantir_aba, append_rows,
    to_num, brl, safe_cost, first_col, fmt_num,
    norm_tipo_mov, calcular_estoque,
    tg_send, tg_media, gerar_id, parse_date,
    ABA_PROD, ABA_VEND, ABA_COMP, ABA_MOVS, ABA_CLIEN, ABA_FIADO, ABA_FPAGT,
)
# Aliases completos para compatibilidade com código existente
_to_num = to_num
_to_float = to_num        # mesma função, nome diferente que era usado em algumas páginas
_brl = brl
_fmt_brl = brl
_first_col = first_col
_fmt_num = fmt_num
_tg_send = tg_send
_tg_media = tg_media
_gerar_id = gerar_id
_parse_date = parse_date
_parse_date_any = parse_date
_norm_tipo_mov = norm_tipo_mov
_norm_tipo = norm_tipo_mov
conectar_sheets = sheet

def _canon_id(x):
    import re as _re
    return _re.sub(r"[^0-9]", "", str(x or ""))



# =========================
# Abas & Headers
# =========================
ABA_PRODUTOS="Produtos"
ABA_COMPRAS="Compras"             # somente para custo (fallback)
ABA_MOV="MovimentosEstoque"       # FONTE ÚNICA de quantidades
ABA_VENDAS="Vendas"

COMPRAS_HEADERS=["Data","Produto","Unidade","Fornecedor","Qtd","Custo Unitário","Total","IDProduto","Obs"]
MOV_HEADERS=["Data","IDProduto","Produto","Tipo","Qtd","Obs","ID","Documento/NF","Origem","SaldoApós"]

# =========================
# Carregar bases
# =========================
titles=_sheet_titles()
prod_df=_load_df(ABA_PRODUTOS)
compras_df=_load_df(ABA_COMPRAS) if ABA_COMPRAS in titles else pd.DataFrame(columns=COMPRAS_HEADERS)
mov_df=_load_df(ABA_MOV) if ABA_MOV in titles else pd.DataFrame(columns=MOV_HEADERS)

# =========================
# Normalizações
# =========================
COLP={
    "id":   next((c for c in ["ID","Id","id","Codigo","Código","SKU","IDProduto","ProdutoID"] if c in prod_df.columns),None),
    "nome": next((c for c in ["Nome","Produto","Descrição","Descricao"] if c in prod_df.columns),None),
}
if COLP["nome"] is None:
    st.error("Aba **Produtos** precisa ter coluna de nome.")
    st.stop()

base=prod_df.copy()
base["__key"]=base.apply(lambda r:_prod_key_from(r.get(COLP["id"],""), r.get(COLP["nome"],"")),axis=1)
base["Produto"]=base[COLP["nome"]]
base["IDProduto"]=base[COLP["id"]] if COLP["id"] else ""

# ---------- Custos ----------
# 1) Produtos.CustoAtual (prioridade)
custo_produto_map = {}
if "CustoAtual" in prod_df.columns:
    tmp = prod_df.copy()
    tmp["__key"] = tmp.apply(lambda r:_prod_key_from(r.get(COLP["id"],""), r.get(COLP["nome"],"")), axis=1)
    tmp["CustoAtual_num"] = tmp["CustoAtual"].apply(_to_num)
    custo_produto_map = dict(zip(tmp["__key"], tmp["CustoAtual_num"]))

# 2) Última compra (fallback)
custo_compra_map = {}
if not compras_df.empty:
    compras_df["__key"]=compras_df.apply(lambda r:_prod_key_from(r.get("IDProduto",""), r.get("Produto","")),axis=1)
    compras_df["Custo_num"]=compras_df["Custo Unitário"].apply(_to_num)
    last_cost=compras_df.groupby("__key",as_index=False).tail(1)
    custo_compra_map=dict(zip(last_cost["__key"], last_cost["Custo_num"]))

# 3) Escolha final de custo
def _custo_atual(key: str) -> float:
    v_prod = float(custo_produto_map.get(key, 0.0))
    if v_prod > 0:
        return v_prod
    return float(custo_compra_map.get(key, 0.0))

# ---------- Movimentos ----------
for c in MOV_HEADERS:
    if c not in mov_df.columns: mov_df[c]=""
if not mov_df.empty:
    mov_df["Tipo_norm"]=mov_df["Tipo"].apply(_norm_tipo)
    mov_df["Qtd_num"]=mov_df["Qtd"].apply(_to_num)
    mov_df["__key"]=mov_df.apply(lambda r:_prod_key_from(r.get("IDProduto",""), r.get("Produto","")),axis=1)
    def _sum_mov(tipo):
        m=mov_df[mov_df["Tipo_norm"]==tipo]
        return {} if m.empty else m.groupby("__key")["Qtd_num"].sum().to_dict()
    entradas_mov=_sum_mov("entrada"); saidas_mov=_sum_mov("saida"); ajustes_mov=_sum_mov("ajuste")
else:
    entradas_mov,saidas_mov,ajustes_mov={},{},{}

# =========================
# Consolidação
# =========================
df=base[["__key","Produto","IDProduto"]].copy()
def _get(mapper,key): return float(mapper.get(key,0.0))
df["Entradas"]=df["__key"].apply(lambda k:_get(entradas_mov,k))
df["Saidas"]=df["__key"].apply(lambda k:_get(saidas_mov,k))
df["Ajustes"]=df["__key"].apply(lambda k:_get(ajustes_mov,k))
df["EstoqueAtual"]=df["Entradas"]-df["Saidas"]+df["Ajustes"]
df["CustoAtual"]=df["__key"].apply(_custo_atual)
df["ValorTotal"]=(df["EstoqueAtual"].astype(float)*df["CustoAtual"].astype(float)).round(2)

# =========================
# HEADER
# =========================
left,right=st.columns([0.7,0.3])
with left:
    st.markdown("<h1>📦 Estoque — Movimentos & Ajustes</h1>",unsafe_allow_html=True)
    st.markdown(f"<div class='small'>Fonte de quantidade: <b>{ABA_MOV}</b> • Atualizado: <code>{datetime.now().strftime('%d/%m/%Y %H:%M:%S')}</code></div>",unsafe_allow_html=True)
with right:
    if Path("pages/03_Compras_Produtos_Entradas.py").exists():
        st.page_link("pages/03_Compras_Produtos_Entradas.py", label="🧾 Compras / Entradas", icon="🧾")
    if Path("pages/01_Produtos.py").exists():
        st.page_link("pages/01_Produtos.py", label="📦 Catálogo", icon="📦")

st.markdown("<div class='badge'>Quantidade = Entradas − Saídas + Ajustes • Custo: Produtos.CustoAtual ➜ Última Compra</div>", unsafe_allow_html=True)
st.markdown("<hr/>", unsafe_allow_html=True)

# ------ Diagnóstico (útil p/ permissões/URL) ------
with st.expander("🧩 Diagnóstico de Conexão com Google Sheets"):
    try:
        svc = st.secrets.get("GCP_SERVICE_ACCOUNT")
        client_email = (json.loads(svc)["client_email"] if isinstance(svc, str) else svc.get("client_email", ""))
        st.write("**Service Account:**", client_email)
        st.write("**PLANILHA_URL:**", st.secrets.get("PLANILHA_URL"))
        try:
            sh = _sheet()
            st.write("**Título da planilha:**", getattr(sh, "title", "(desconhecido)"))
            st.write("**Abas disponíveis:**", sorted(list(_sheet_titles())) or "(nenhuma)")
        except Exception as e:
            st.warning(f"Falha ao abrir planilha: {e}")
    except Exception as e:
        st.error(f"Não foi possível ler os segredos. Erro: {e}")

# =========================
# Busca / Filtros
# =========================
cBusca, cLow, cThr, cExp = st.columns([3,1.1,1.1,1])
with cBusca:
    termo = st.text_input("🔎 Buscar", placeholder="Nome ou ID do produto...")
with cLow:
    only_low = st.checkbox("Somente baixo estoque", value=False)
with cThr:
    low_thr = st.number_input("Limiar (≤)", value=0, step=1)
with cExp:
    exportar = st.button("⬇️ Exportar CSV")

mask = pd.Series([True]*len(df))
if termo.strip():
    t=_strip_accents_low(termo)
    by_nome = df["Produto"].astype(str).apply(_strip_accents_low).str.contains(t)
    by_id   = df["IDProduto"].astype(str).str.contains(termo.strip(), case=False, na=False)
    mask &= (by_nome | by_id)
if only_low:
    mask &= (df["EstoqueAtual"] <= float(low_thr))

df_view = df[mask].copy()

# =========================
# CARDS (KPIs)
# =========================
c1,c2,c3,c4 = st.columns(4)
with c1:
    st.markdown(f"<div class='kpi'><h3>Itens cadastrados</h3><div class='big'>{len(df):,}</div><div class='sub'>Total em Produtos</div></div>".replace(",", "."), unsafe_allow_html=True)
with c2:
    st.markdown(f"<div class='kpi'><h3>Com estoque &gt; 0</h3><div class='big'>{int((df_view['EstoqueAtual']>0).sum()):,}</div><div class='sub'>Filtrados</div></div>".replace(",", "."), unsafe_allow_html=True)
with c3:
    st.markdown(f"<div class='kpi'><h3>Qtd total</h3><div class='big'>{df_view['EstoqueAtual'].sum():.0f}</div><div class='sub'>Entradas - Saídas + Ajustes</div></div>", unsafe_allow_html=True)
with c4:
    vtotal=(df_view['EstoqueAtual']*df_view['CustoAtual']).sum()
    st.markdown(f"<div class='kpi'><h3>Valor total (R$)</h3><div class='big'>R$ {vtotal:,.2f}</div><div class='sub'>Estoque x custo</div></div>".replace(",", "X").replace(".", ",").replace("X","."), unsafe_allow_html=True)

# =========================
# TABELA
# =========================
cols_show=["IDProduto","Produto","Entradas","Saidas","Ajustes","EstoqueAtual","CustoAtual","ValorTotal"]
for c in cols_show:
    if c not in df_view.columns:
        df_view[c]=0 if c not in ("IDProduto","Produto") else ""

dfv=df_view[cols_show].copy()
dfv["Entradas"]=dfv["Entradas"].astype(float).round(2)
dfv["Saidas"]=dfv["Saidas"].astype(float).round(2)
dfv["Ajustes"]=dfv["Ajustes"].astype(float).round(2)
dfv["EstoqueAtual"]=dfv["EstoqueAtual"].astype(float).round(2)
dfv["CustoAtual"]=dfv["CustoAtual"].astype(float).round(2)
dfv["ValorTotal"]=(df_view["EstoqueAtual"].astype(float)*df_view["CustoAtual"].astype(float)).round(2)

st.markdown("<div class='card'><h3>📊 Tabela de Estoque</h3>", unsafe_allow_html=True)
st.dataframe(dfv.sort_values("Produto"), use_container_width=True, hide_index=True)
st.markdown("</div>", unsafe_allow_html=True)

if exportar:
    csv = dfv.sort_values("Produto").to_csv(index=False, sep=";").encode("utf-8-sig")
    st.download_button("Baixar CSV (utf-8)", data=csv, file_name="estoque.csv", mime="text/csv")

# =========================
# Últimos movimentos (debug)
# =========================
with st.expander("🧾 Últimos movimentos (debug)"):
    if mov_df.empty:
        st.caption("Sem movimentos ainda.")
    else:
        dbg_cols=[c for c in ["Data","Produto","IDProduto","Tipo","Qtd","Tipo_norm"] if c in mov_df.columns]
        st.dataframe(mov_df[dbg_cols].tail(30), use_container_width=True, hide_index=True)

st.markdown("<hr/>", unsafe_allow_html=True)

# =========================
# FORM: Saída
# =========================
st.markdown("<div class='card'><h3>➖ Registrar Saída / Baixa de Estoque</h3>", unsafe_allow_html=True)
with st.form("form_saida"):
    usar_lista_s = st.checkbox("Selecionar produto da lista", value=True, key="saida_lista")
    df_select = df_view if usar_lista_s and not df_view.empty else df
    if usar_lista_s:
        if df_select.empty:
            st.warning("Sem produtos para saída.")
            st.form_submit_button("Registrar saída", disabled=True)
            st.stop()
        def _fmt_saida(i):
            r=df_select.iloc[i]
            return f"{_nz(r['Produto'])} — Estq: {int(float(r['EstoqueAtual']))}"
        idx = st.selectbox("Produto", options=range(len(df_select)), format_func=_fmt_saida)
        row = df_select.iloc[idx]
        prod_nome_s=_nz(row["Produto"]); prod_id_s=_nz(row["IDProduto"])
    else:
        prod_nome_s = st.text_input("Produto (nome exato)", key="saida_nome")
        prod_id_s   = st.text_input("ID (opcional)", key="saida_id")
    csa,csb=st.columns(2)
    with csa: data_s = st.date_input("Data da saída", value=date.today(), key="saida_data")
    with csb: qtd_s  = st.text_input("Qtd", placeholder="Ex.: 2", key="saida_qtd")
    obs_s = st.text_input("Observações (opcional)", key="saida_obs")
    salvar_s = st.form_submit_button("Registrar saída", use_container_width=True)

if 'salvar_s' in locals() and salvar_s:
    if not prod_nome_s.strip() and not prod_id_s.strip():
        st.error("Selecione ou informe um produto.")
        st.stop()
    q=_to_num(qtd_s)
    if q<=0:
        st.error("Informe uma quantidade válida (> 0).")
        st.stop()
    ws_mov=_ensure_ws(ABA_MOV, MOV_HEADERS)
    _append_row(ws_mov, {
        "Data": data_s.strftime("%d/%m/%Y"),
        "IDProduto": _nz(prod_id_s),
        "Produto": prod_nome_s,
        "Tipo": "saida",
        "Qtd": (str(int(q)) if float(q).is_integer() else str(q)).replace(".", ","),
        "Obs": _nz(obs_s)
    })
    st.success("Saída registrada com sucesso! ✅")
    st.toast("Saída lançada", icon="➖")
    st.cache_data.clear()

st.markdown("</div>", unsafe_allow_html=True)

# =========================
# FORM: Ajuste
# =========================
st.markdown("<div class='card'><h3>🛠️ Registrar Ajuste de Estoque</h3>", unsafe_allow_html=True)
with st.form("form_ajuste"):
    usar_lista_a = st.checkbox("Selecionar produto da lista", value=True, key="ajuste_lista")
    df_select = df_view if usar_lista_a and not df_view.empty else df
    if usar_lista_a:
        if df_select.empty:
            st.warning("Sem produtos para ajuste.")
            st.form_submit_button("Registrar ajuste", disabled=True)
            st.stop()
        def _fmt_aj(i):
            r=df_select.iloc[i]
            return f"{_nz(r['Produto'])} — Estq: {int(float(r['EstoqueAtual']))}"
        idxa = st.selectbox("Produto", options=range(len(df_select)), format_func=_fmt_aj, key="ajuste_idx")
        rowa = df_select.iloc[idxa]
        prod_nome_a=_nz(rowa["Produto"]); prod_id_a=_nz(rowa["IDProduto"])
    else:
        prod_nome_a = st.text_input("Produto (nome exato)", key="ajuste_nome")
        prod_id_a   = st.text_input("ID (opcional)", key="ajuste_id")
    ca1,ca2=st.columns(2)
    with ca1: data_a = st.date_input("Data do ajuste", value=date.today(), key="ajuste_data")
    with ca2: qtd_a  = st.text_input("Qtd (use negativo para baixar, positivo para repor)", placeholder="Ex.: -1 ou 5", key="ajuste_qtd")
    obs_a = st.text_input("Motivo/Observações", key="ajuste_obs")
    salvar_a = st.form_submit_button("Registrar ajuste", use_container_width=True)

if 'salvar_a' in locals() and salvar_a:
    if not prod_nome_a.strip() and not prod_id_a.strip():
        st.error("Selecione ou informe um produto.")
        st.stop()
    qa=_to_num(qtd_a)
    if qa==0:
        st.error("Informe uma quantidade diferente de zero.")
        st.stop()
    ws_mov=_ensure_ws(ABA_MOV, MOV_HEADERS)
    _append_row(ws_mov, {
        "Data": data_a.strftime("%d/%m/%Y"),
        "IDProduto": _nz(prod_id_a),
        "Produto": prod_nome_a,
        "Tipo": "ajuste",
        "Qtd": (str(int(qa)) if float(qa).is_integer() else str(qa)).replace(".", ","),
        "Obs": _nz(obs_a)
    })
    st.success("Ajuste registrado com sucesso! ✅")
    st.toast("Ajuste lançado", icon="🛠️")
    st.cache_data.clear()

st.markdown("</div>", unsafe_allow_html=True)

# =========================
# Rodapé
# =========================
st.markdown("<div class='small'>Dica: ajuste o <b>Limiar (≤)</b> para destacar baixo estoque e use a busca por nome/ID.</div>", unsafe_allow_html=True)
