# -*- coding: utf-8 -*-
# pages/02_cadastrar_produto.py — Cadastrar/Editar Produtos com cálculo automático
import json, unicodedata, math
import streamlit as st
import pandas as pd
import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta

st.set_page_config(page_title="Cadastrar/Editar Produto", page_icon="➕", layout="wide")
st.title("➕ Cadastrar / Editar Produto")

# =============================================================================
# Credenciais / Sheets
# =============================================================================
def _normalize_private_key(key: str) -> str:
    if not isinstance(key, str):
        return key
    key = key.replace("\\n", "\n")
    key = "".join(ch for ch in key if unicodedata.category(ch)[0] != "C" or ch in ("\n", "\r", "\t"))
    return key

def _load_sa():
    svc = st.secrets.get("GCP_SERVICE_ACCOUNT")
    if svc is None:
        st.error("🛑 GCP_SERVICE_ACCOUNT ausente em st.secrets."); st.stop()
    if isinstance(svc, str):
        svc = json.loads(svc)
    svc = dict(svc)
    svc["private_key"] = _normalize_private_key(svc["private_key"])
    return svc

@st.cache_resource
def _client():
    scopes = ["https://www.googleapis.com/auth/spreadsheets",
              "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(_load_sa(), scopes=scopes)
    return gspread.authorize(creds)

@st.cache_resource
def _sheet():
    gc = _client()
    url_or_id = st.secrets.get("PLANILHA_URL")
    if not url_or_id:
        st.error("🛑 PLANILHA_URL ausente em st.secrets."); st.stop()
    return gc.open_by_url(url_or_id) if str(url_or_id).startswith("http") else gc.open_by_key(url_or_id)

@st.cache_data
def _load_df(aba: str) -> pd.DataFrame:
    ws = _sheet().worksheet(aba)
    df = get_as_dataframe(ws, evaluate_formulas=True, dtype=str, header=0).dropna(how="all")
    df.columns = [c.strip() for c in df.columns]
    return df

def _safe_load(aba):
    try:
        return _load_df(aba)
    except Exception:
        return pd.DataFrame()

def _to_float(x):
    if x is None or str(x).strip()=="":
        return ""
    s = str(x).strip().replace("R$", "").replace(".", "").replace(",", ".")
    try:
        return float(s)
    except:
        return ""

def _to_int(x):
    if x is None or str(x).strip()=="":
        return ""
    try:
        return int(float(str(x).strip().replace(",", ".")))
    except:
        return ""

def _gen_id():
    return "P-" + datetime.now().strftime("%Y%m%d%H%M%S")

def _msg_ok(msg):
    st.success(msg)
    try:
        st.cache_data.clear()
    except:
        pass

# =============================================================================
# Mapeamentos flexíveis da aba Produtos
# =============================================================================
def _pick_col(df, candidates):
    for c in candidates:
        if c in df.columns:
            return c
    return None

def _map_cols_produtos(df):
    return {
        "id":        _pick_col(df, ["ID","Id","id"]),
        "nome":      _pick_col(df, ["Nome","Produto","Descrição","Descricao"]),
        "categoria": _pick_col(df, ["Categoria","Grupo"]),
        "unidade":   _pick_col(df, ["Unidade","Unid"]),
        "forn":      _pick_col(df, ["Fornecedor","FornecedorNome","Fornecedor ID","FornecedorID"]),
        "custo":     _pick_col(df, ["CustoAtual","Custo","Custo Atual"]),
        "preco":     _pick_col(df, ["PreçoVenda","PrecoVenda","Preço Venda","Preco Venda","Preço","Valor"]),
        "markup":    _pick_col(df, ["Markup %","Markup%","Markup"]),
        "margem":    _pick_col(df, ["Margem %","Margem%","Margem"]),
        "estoque":   _pick_col(df, ["EstoqueAtual","Estoque","QtdEstoque","Quantidade"]),
        "est_min":   _pick_col(df, ["EstoqueMin","Estoque Min","Minimo","Mínimo"]),
        "lead":      _pick_col(df, ["LeadTimeDias","LeadTime","Lead Time"]),
        "ativo":     _pick_col(df, ["Ativo?","Ativo","Status"]),
        "codb":      _pick_col(df, ["Código de Barras","Codigo de Barras","EAN","EAN13","Barcode"]),
        "desc":      _pick_col(df, ["Descrição","Descricao","Observações","Observacoes"]),
        "atualizado": _pick_col(df, ["AtualizadoEm","Atualizado Em","Atualizado"])
    }

# Mapeamentos de outras abas (candidatos comuns)
def _map_cols_compras(df):
    return {
        "data": _pick_col(df, ["Data","DATA"]),
        "nome": _pick_col(df, ["Produto","Nome","Descrição","Descricao"]),
        "unid": _pick_col(df, ["Unidade","Unid"]),
        "forn": _pick_col(df, ["Fornecedor","FornecedorNome"]),
        "qtd":  _pick_col(df, ["Qtd","Quantidade","Qtde"]),
        "custo_unit": _pick_col(df, ["CustoUnit","Custo Unitário","Custo Unidade","PrecoUnitario","Preço Unitário"]),
        "total": _pick_col(df, ["Total","ValorTotal"]),
        "id": _pick_col(df, ["IDProduto","ID"])
    }

def _map_cols_mov(df):
    return {
        "data": _pick_col(df, ["Data","DATA"]),
        "id":   _pick_col(df, ["IDProduto","ID"]),
        "nome": _pick_col(df, ["Produto","Nome"]),
        "tipo": _pick_col(df, ["Tipo","Movimento","Mov"]),
        "qtd":  _pick_col(df, ["Qtd","Quantidade","Qtde"])
    }

def _map_cols_vendas(df):
    return {
        "data": _pick_col(df, ["Data","DATA"]),
        "id":   _pick_col(df, ["IDProduto","ID"]),
        "nome": _pick_col(df, ["Produto","Nome"]),
        "qtd":  _pick_col(df, ["Qtd","Quantidade","Qtde"])
    }

def _map_cols_forn(df):
    return {
        "forn": _pick_col(df, ["Fornecedor","Nome"]),
        "lead": _pick_col(df, ["LeadTimeDias","Lead Time","Lead"])
    }

# =============================================================================
# Carregar dados base
# =============================================================================
ABA = "Produtos"
try:
    df = _load_df(ABA)
except Exception as e:
    st.error("Erro ao abrir a aba Produtos.")
    with st.expander("Detalhes técnicos"):
        st.code(str(e))
    st.stop()

COL = _map_cols_produtos(df)

compras_df   = _safe_load("Compras")
movest_df    = _safe_load("MovimentosEstoque")
vendas_df    = _safe_load("Vendas")
forn_df      = _safe_load("Fornecedores")

CMP = _map_cols_compras(compras_df) if not compras_df.empty else {}
MOV = _map_cols_mov(movest_df) if not movest_df.empty else {}
VEN = _map_cols_vendas(vendas_df) if not vendas_df.empty else {}
FD  = _map_cols_forn(forn_df) if not forn_df.empty else {}

# =============================================================================
# Funções de cálculo automático
# =============================================================================
def _last_cost_and_unit(nome: str, fornecedor: str|None):
    if compras_df.empty or not CMP:
        return None, None
    base = compras_df.copy()
    # filtro por produto (obrigatório)
    if CMP.get("nome"):
        base = base[ base[CMP["nome"]].astype(str).str.strip().str.lower() == nome.strip().lower() ]
    # se tiver fornecedor, filtra também
    if fornecedor and CMP.get("forn"):
        base = base[ base[CMP["forn"]].astype(str).str.strip().str.lower() == fornecedor.strip().lower() ]
    if base.empty:
        return None, None
    # ordena por data desc se existir
    if CMP.get("data") and pd.api.types.is_datetime64_any_dtype(pd.to_datetime(base[CMP["data"]], errors="coerce")):
        base = base.assign(__d = pd.to_datetime(base[CMP["data"]], errors="coerce")).sort_values("__d", ascending=False)
    # pega primeira linha como último registro
    row = base.iloc[0]
    custo = _to_float(row.get(CMP.get("custo_unit",""), ""))
    unid  = str(row.get(CMP.get("unid",""), "")).strip() or None
    return custo if custo not in ("", None) else None, unid

def _stock_balance(prod_id: str|None, nome: str):
    # Prioridade: MovimentosEstoque; fallback: Vendas (saídas)
    saldo = 0
    has_any = False
    if not movest_df.empty and MOV:
        base = movest_df.copy()
        if prod_id and MOV.get("id"):
            base = base[ base[MOV["id"]].astype(str) == str(prod_id) ]
        elif MOV.get("nome"):
            base = base[ base[MOV["nome"]].astype(str).str.strip().str.lower() == nome.strip().lower() ]
        if not base.empty:
            has_any = True
            ent = base[ base[MOV["tipo"]].astype(str).str.lower().isin(["entrada","compra","ajuste+","entrada manual","in"]) ][MOV["qtd"]].apply(_to_float).sum()
            sai = base[ base[MOV["tipo"]].astype(str).str.lower().isin(["saida","venda","ajuste-","saída manual","out"]) ][MOV["qtd"]].apply(_to_float).sum()
            saldo = (ent or 0) - (sai or 0)

    if not has_any and (not vendas_df.empty) and VEN:
        base = vendas_df.copy()
        if prod_id and VEN.get("id"):
            base = base[ base[VEN["id"]].astype(str) == str(prod_id) ]
        elif VEN.get("nome"):
            base = base[ base[VEN["nome"]].astype(str).str.strip().str.lower() == nome.strip().lower() ]
        if not base.empty:
            # consideramos vendas como saídas e não temos entradas, então saldo tende a ficar negativo
            # para não confundir, retornamos 0 nesse caso (até ter movimentos de entrada)
            return 0
    return int(round(saldo, 0))

def _avg_daily_sales_30d(prod_id: str|None, nome: str):
    if vendas_df.empty or not VEN:
        return 0.0
    base = vendas_df.copy()
    if VEN.get("data") is None:
        return 0.0
    base["__d"] = pd.to_datetime(base[VEN["data"]], errors="coerce")
    maxd = base["__d"].max()
    if pd.isna(maxd):
        return 0.0
    start = maxd - timedelta(days=30)
    base = base[ base["__d"] >= start ]
    if prod_id and VEN.get("id"):
        base = base[ base[VEN["id"]].astype(str) == str(prod_id) ]
    elif VEN.get("nome"):
        base = base[ base[VEN["nome"]].astype(str).str.strip().str.lower() == nome.strip().lower() ]
    if base.empty:
        return 0.0
    qty = base[VEN["qtd"]].apply(_to_float).sum()
    days = max((maxd - start).days, 1)
    return float(qty) / float(days)

def _lead_time_fornecedor(fornecedor: str|None):
    if forn_df.empty or not FD or not fornecedor:
        return None
    base = forn_df.copy()
    base = base[ base[FD["forn"]].astype(str).str.strip().str.lower() == fornecedor.strip().lower() ]
    if base.empty:
        return None
    v = _to_int(base.iloc[0].get(FD["lead"], ""))
    return v if v != "" else None

def _calc_est_min(avg_daily: float, lead_time_days: int|None):
    lt = lead_time_days if lead_time_days not in (None, "", 0) else 7  # default 7 dias
    safety = 1.2  # 20% de folga
    estmin = math.ceil(avg_daily * lt * safety)
    return estmin if estmin > 0 else 5  # fallback 5

# =============================================================================
# UI — escolher ação
# =============================================================================
m1, m2 = st.columns([1.4, 2])
with m1:
    modo = st.radio("Ação", ["Cadastrar novo", "Editar existente"], horizontal=True)
with m2:
    st.caption("Campos como **CustoAtual**, **EstoqueAtual**, **EstoqueMin**, **Unidade** e **LeadTimeDias** são calculados automaticamente quando possível.")

st.divider()

# =============================================================================
# EDITAR EXISTENTE
# =============================================================================
if modo == "Editar existente":
    cc0, cc1, cc2, cc3 = st.columns([1.6, 1.1, 1.1, 1.2])
    with cc0:
        usar_lista = st.checkbox("Selecionar da lista (auto-sugestão)", value=True)
    with cc1:
        apenas_ativos = st.checkbox("Apenas ativos", value=True)
    with cc2:
        so_estoque = st.checkbox("Somente com estoque (>0)", value=False)
    with cc3:
        recalc_auto = st.checkbox("Atualizar campos calculados", value=True)

    base = df.copy()
    if COL["ativo"] and apenas_ativos:
        base = base[ base[COL["ativo"]].astype(str).str.strip().str.lower().isin(["1","true","sim","ativo","yes"]) ]
    if COL["estoque"] and so_estoque:
        def _gt0(x):
            try: return float(str(x).replace(",", ".").strip()) > 0
            except: return False
        base = base[ base[COL["estoque"]].apply(_gt0) ]

    if usar_lista:
        if base.empty:
            st.info("Nada encontrado para os filtros atuais."); st.stop()
        def _fmt_row(r):
            nome = str(r.get(COL["nome"], "(sem nome)"))
            forn = str(r.get(COL["forn"], "")).strip()
            preco = str(r.get(COL["preco"], "")).strip()
            return f"{nome}" + (f" — {forn}" if forn else "") + (f" — R$ {preco}" if preco else "")
        labels = base.apply(_fmt_row, axis=1).tolist()
        escolha = st.selectbox("Produto (digite para filtrar…)", ["(selecione)"] + labels, index=0)
        if escolha == "(selecione)":
            st.stop()
        pos = labels.index(escolha)
        sel = base.iloc[pos].to_dict()
    else:
        termo = st.text_input("🔎 Buscar", placeholder="Nome, fornecedor, categoria, código de barras…").strip()
        if termo:
            t = termo.lower()
            base = base[base.apply(lambda row: t in " ".join([str(x).lower() for x in row.values]), axis=1)]
        if base.empty:
            st.info("Nada encontrado para os filtros atuais."); st.stop()
        nomes_fmt = base.apply(
            lambda r: f'{str(r.get(COL["nome"],"(sem nome)"))} — {str(r.get(COL["forn"],"")).strip() or "s/ forn"} — R$ {str(r.get(COL["preco"],"")).strip()}',
            axis=1
        ).tolist()
        pos = st.selectbox("Selecione um produto", options=range(len(base)), format_func=lambda i: nomes_fmt[i])
        sel = base.iloc[pos].to_dict()

    # Form
    st.subheader("Editar")
    with st.form("editar_produto"):
        c1, c2, c3 = st.columns([1.6,1,1])
        with c1:
            nome = st.text_input("Nome", value=str(sel.get(COL["nome"],"")).strip())
        with c2:
            categoria = st.text_input("Categoria", value=str(sel.get(COL["categoria"],"")).strip())
        with c3:
            fornecedor = st.text_input("Fornecedor", value=str(sel.get(COL["forn"],"")).strip())

        c4, c5, c6 = st.columns([1,1,1])
        with c4:
            preco = st.text_input("Preço venda (R$)", value=str(sel.get(COL["preco"],"")).strip())
        with c5:
            # estoque aparece apenas para conferência/ajuste manual (pode deixar como está que recalcula se recalc_auto=True)
            estoque = st.text_input("Estoque atual (un)", value=str(sel.get(COL["estoque"],"")).strip())
        with c6:
            ativo_flag = str(sel.get(COL["ativo"],"")).strip().lower() in ["1","true","sim","ativo","yes"]
            ativo = st.checkbox("Ativo", value=ativo_flag)

        # informativo dos calculados
        with st.expander("Campos calculados automaticamente (informativo)", expanded=False):
            st.caption("Serão recalculados ao salvar se **Atualizar campos calculados** estiver marcado.")
            _custo_preview, _unid_preview = _last_cost_and_unit(nome, fornecedor)
            _lead_preview = _lead_time_fornecedor(fornecedor)
            _avg30 = _avg_daily_sales_30d(sel.get(COL["id"], ""), nome)
            _estmin_preview = _calc_est_min(_avg30, _lead_preview)
            st.write(f"**CustoAtual (prev.)**: {(_custo_preview if _custo_preview is not None else '—')}")
            st.write(f"**Unidade (prev.)**: {(_unid_preview or '—')}")
            st.write(f"**LeadTimeDias (prev.)**: {(_lead_preview if _lead_preview is not None else '—')}")
            st.write(f"**EstoqueMin (prev.)**: {_estmin_preview}")

        salvar = st.form_submit_button("💾 Atualizar produto")

    if salvar:
        if not nome.strip():
            st.error("Informe o **Nome**."); st.stop()
        pf = _to_float(preco)
        if pf == "":
            st.error("Preço inválido. Use números (ex: 19,90)."); st.stop()
        est_in = _to_int(estoque)
        if est_in == "":
            est_in = None

        updates = {}
        if COL["nome"]:      updates[COL["nome"]] = nome.strip()
        if COL["categoria"]: updates[COL["categoria"]] = categoria.strip()
        if COL["forn"]:      updates[COL["forn"]] = fornecedor.strip()
        if COL["preco"]:     updates[COL["preco"]] = f"{pf:.2f}".replace(".", ",")

        # Recalcular automáticos
        if recalc_auto:
            custo, unid = _last_cost_and_unit(nome, fornecedor)
            if custo is not None and COL["custo"]:   updates[COL["custo"]] = f"{custo:.2f}".replace(".", ",")
            if unid and COL["unidade"]:              updates[COL["unidade"]] = unid
            lead = _lead_time_fornecedor(fornecedor)
            if (lead is not None) and COL["lead"]:   updates[COL["lead"]] = str(lead)
            # estoque atual preferencialmente por movimentos; se não houver, mantém o digitado
            saldo = _stock_balance(sel.get(COL["id"], ""), nome)
            if COL["estoque"]:                       updates[COL["estoque"]] = str(saldo if saldo is not None else (est_in or 0))
            avg30 = _avg_daily_sales_30d(sel.get(COL["id"], ""), nome)
            estmin = _calc_est_min(avg30, lead)
            if COL["est_min"]:                       updates[COL["est_min"]] = str(estmin)
        else:
            if est_in is not None and COL["estoque"]:
                updates[COL["estoque"]] = str(est_in)

        if COL["ativo"]:     updates[COL["ativo"]] = "sim" if ativo else "não"
        if COL["atualizado"]: updates[COL["atualizado"]] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")

        id_col = COL["id"] or "ID"
        if id_col not in df.columns:
            st.error("Coluna de ID não encontrada na planilha."); st.stop()
        row_mask = (df[id_col] == sel.get(id_col, ""))
        if not row_mask.any():
            st.error("Não foi possível localizar a linha para atualizar."); st.stop()

        for k, v in updates.items():
            if k in df.columns:
                df.loc[row_mask, k] = v

        ws = _sheet().worksheet(ABA)
        df_old = _load_df(ABA)
        ids = df_old[id_col].tolist()
        i = ids.index(sel.get(id_col, "")) if sel.get(id_col, "") in ids else None
        if i is None:
            st.error("Falha ao localizar a linha na planilha."); st.stop()
        for col, val in updates.items():
            if col in df_old.columns:
                df_old.loc[i, col] = val
        ws.clear()
        set_with_dataframe(ws, df_old.fillna(""), include_index=False, include_column_header=True, resize=True)
        _msg_ok("Produto atualizado com sucesso! Campos calculados aplicados.")

# =============================================================================
# CADASTRAR NOVO
# =============================================================================
else:
    st.subheader("Cadastrar novo produto")
    with st.form("cadastrar_produto"):
        c1, c2, c3 = st.columns([1.6,1,1])
        with c1:
            nome = st.text_input("Nome")
        with c2:
            categoria = st.text_input("Categoria", placeholder="Ex.: limpeza, higiene…")
        with c3:
            fornecedor = st.text_input("Fornecedor")

        c4, c5 = st.columns([1,1])
        with c4:
            preco = st.text_input("Preço venda (R$)", placeholder="19,90")
        with c5:
            ativo = st.checkbox("Ativo", value=True)

        salvar = st.form_submit_button("➕ Cadastrar produto")

    if salvar:
        if not nome.strip():
            st.error("Informe o **Nome**."); st.stop()
        pf = _to_float(preco)
        if pf == "":
            st.error("Preço inválido. Use números (ex: 19,90)."); st.stop()

        novo_id = _gen_id()

        # Calculados
        custo, unid = _last_cost_and_unit(nome, fornecedor)
        lead = _lead_time_fornecedor(fornecedor)
        saldo = _stock_balance(None, nome)  # sem ID ainda
        avg30 = _avg_daily_sales_30d(None, nome)
        estmin = _calc_est_min(avg30, lead)

        # Monta linha nova só com colunas existentes
        new_row = {}
        if COL["id"]:        new_row[COL["id"]] = novo_id
        if COL["nome"]:      new_row[COL["nome"]] = nome.strip()
        if COL["categoria"]: new_row[COL["categoria"]] = categoria.strip()
        if COL["forn"]:      new_row[COL["forn"]] = fornecedor.strip()
        if COL["preco"]:     new_row[COL["preco"]] = f"{pf:.2f}".replace(".", ",")
        if (custo is not None) and COL["custo"]: new_row[COL["custo"]] = f"{custo:.2f}".replace(".", ",")
        if COL["estoque"]:   new_row[COL["estoque"]] = str(saldo if saldo is not None else 0)
        if COL["est_min"]:   new_row[COL["est_min"]] = str(estmin)
        if (lead is not None) and COL["lead"]: new_row[COL["lead"]] = str(lead)
        if unid and COL["unidade"]: new_row[COL["unidade"]] = unid
        if COL["ativo"]:     new_row[COL["ativo"]] = "sim" if ativo else "não"
        if COL["atualizado"]: new_row[COL["atualizado"]] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")

        ws = _sheet().worksheet(ABA)
        df_atual = _load_df(ABA)

        # Garante todas as colunas do header com vazio
        for col in df_atual.columns:
            if col not in new_row:
                new_row[col] = ""

        df_out = pd.concat([df_atual, pd.DataFrame([new_row])], ignore_index=True)
        set_with_dataframe(ws, df_out.fillna(""), include_index=False, include_column_header=True, resize=True)

        _msg_ok("Produto cadastrado com sucesso! Campos calculados aplicados.")
        st.toast("Cadastro concluído ✅", icon="✅")
        st.balloons()

st.divider()
st.page_link("pages/01_produtos.py", label="↩️ Ir para Catálogo de Produtos", icon="📦")
