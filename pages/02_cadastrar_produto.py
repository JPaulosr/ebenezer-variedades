# -*- coding: utf-8 -*-
# pages/02_cadastrar_produto.py — Cadastrar/Editar Produtos (compatível com: ID, Nome, Categoria, Unidade,
# Fornecedor, CustoAtual, PreçoVenda, Markup %, Margem %, EstoqueAtual, EstoqueMin, LeadTimeDias, Ativo?)
import json, unicodedata
import streamlit as st
import pandas as pd
import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from google.oauth2.service_account import Credentials
from datetime import datetime

st.set_page_config(page_title="Cadastrar/Editar Produto", page_icon="➕", layout="wide")
st.title("➕ Cadastrar / Editar Produto")

# =============================================================================
# Helpers de credencial/Sheets
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
def carregar_produtos(aba="Produtos") -> pd.DataFrame:
    ws = _sheet().worksheet(aba)
    df = get_as_dataframe(ws, evaluate_formulas=True, dtype=str, header=0).dropna(how="all")
    df.columns = [c.strip() for c in df.columns]
    return df

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
    # limpa caches de dados (ex.: catálogo em outra página) para refletir alterações
    try:
        st.cache_data.clear()
    except:
        pass

# =============================================================================
# Mapeamento flexível de colunas (adapta ao seu cabeçalho)
# =============================================================================
def _pick_col(df, candidates):
    for c in candidates:
        if c in df.columns:
            return c
    return None

def _map_cols(df):
    # retorna nomes REAIS das colunas encontradas
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

# =============================================================================
# Carregar dados
# =============================================================================
ABA = "Produtos"
try:
    df = carregar_produtos(ABA)
except Exception as e:
    st.error("Erro ao abrir a aba Produtos.")
    with st.expander("Detalhes técnicos"):
        st.code(str(e))
    st.stop()

COL = _map_cols(df)

# =============================================================================
# UI — escolher ação
# =============================================================================
m1, m2 = st.columns([1.4, 2])
with m1:
    modo = st.radio("Ação", ["Cadastrar novo", "Editar existente"], horizontal=True)
with m2:
    st.caption("Dica: use nomes consistentes de **Categoria** e **Fornecedor** para facilitar filtros no catálogo.")

st.divider()

# =============================================================================
# EDITAR EXISTENTE
# =============================================================================
if modo == "Editar existente":
    cc1, cc2, cc3 = st.columns([1.6, 1, 1])
    with cc1:
        usar_lista = st.checkbox("Selecionar da lista (auto-sugestão)", value=True)
    with cc2:
        apenas_ativos = st.checkbox("Apenas ativos", value=True)
    with cc3:
        so_estoque = st.checkbox("Somente com estoque (>0)", value=False)

    base = df.copy()

    # filtro de ativos
    if COL["ativo"] and apenas_ativos:
        base = base[ base[COL["ativo"]].astype(str).str.strip().str.lower()
                     .isin(["1","true","sim","ativo","yes"]) ]

    # filtro de estoque > 0
    if COL["estoque"] and so_estoque:
        def _gt0(x):
            try:
                return float(str(x).replace(",", ".").strip()) > 0
            except:
                return False
        base = base[ base[COL["estoque"]].apply(_gt0) ]

    # seleção
    if usar_lista:
        if base.empty:
            st.info("Nada encontrado para os filtros atuais."); st.stop()

        def _fmt_row(r):
            nome = str(r.get(COL["nome"], "(sem nome)"))
            forn = str(r.get(COL["forn"], "")).strip()
            preco = str(r.get(COL["preco"], "")).strip()
            t1 = f"{nome}"
            t2 = f" — {forn}" if forn else ""
            t3 = f" — R$ {preco}" if preco else ""
            return t1 + t2 + t3

        labels = base.apply(_fmt_row, axis=1).tolist()
        escolha = st.selectbox("Produto (digite para filtrar…)", ["(selecione)"] + labels, index=0)
        if escolha == "(selecione)":
            st.stop()
        pos = labels.index(escolha)
        sel = base.iloc[pos].to_dict()
    else:
        l, r = st.columns([2,1])
        with l:
            termo = st.text_input("🔎 Buscar produto", placeholder="Nome, fornecedor, categoria, código de barras…").strip()
        if termo:
            t = termo.lower()
            base = base[base.apply(lambda row: t in " ".join([str(x).lower() for x in row.values]), axis=1)]
        if base.empty:
            st.info("Nada encontrado para os filtros atuais."); st.stop()

        nomes_fmt = base.apply(
            lambda r: f'{str(r.get(COL["nome"],"(sem nome)"))} — {str(r.get(COL["forn"],"")).strip() or "s/ forn"}'
                      f' — R$ {str(r.get(COL["preco"],"")).strip()}',
            axis=1
        ).tolist()
        pos = st.selectbox("Selecione um produto para editar", options=range(len(base)), format_func=lambda i: nomes_fmt[i])
        sel = base.iloc[pos].to_dict()

    # formulário de edição
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
            estoque = st.text_input("Estoque atual (un)", value=str(sel.get(COL["estoque"],"")).strip())
        with c6:
            ativo_flag = str(sel.get(COL["ativo"],"")).strip().lower() in ["1","true","sim","ativo","yes"]
            ativo = st.checkbox("Ativo", value=ativo_flag)

        c7, c8 = st.columns([1,2])
        with c7:
            codb = st.text_input("Código de Barras", value=str(sel.get(COL["codb"],"")).strip())
        with c8:
            desc = st.text_area("Descrição/Observações", value=str(sel.get(COL["desc"],"")).strip(), height=100)

        salvar = st.form_submit_button("💾 Atualizar produto")

    if salvar:
        if not nome.strip():
            st.error("Informe o **Nome**."); st.stop()

        pf = _to_float(preco)
        if pf == "":
            st.error("Preço inválido. Use números (ex: 19,90)."); st.stop()

        est = _to_int(estoque)
        if est == "":
            st.error("Estoque inválido. Use número inteiro."); st.stop()

        # prepara updates somente para colunas existentes
        updates = {}
        if COL["nome"]:      updates[COL["nome"]] = nome.strip()
        if COL["categoria"]: updates[COL["categoria"]] = categoria.strip()
        if COL["forn"]:      updates[COL["forn"]] = fornecedor.strip()
        if COL["preco"]:     updates[COL["preco"]] = f"{pf:.2f}".replace(".", ",")
        if COL["estoque"]:   updates[COL["estoque"]] = str(est)
        if COL["ativo"]:     updates[COL["ativo"]] = "sim" if ativo else "não"
        if COL["codb"]:      updates[COL["codb"]] = codb.strip()
        if COL["desc"]:      updates[COL["desc"]] = desc.strip()
        if COL["atualizado"]: updates[COL["atualizado"]] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")

        # merge por ID
        id_col = COL["id"] or "ID"
        if id_col not in df.columns:
            st.error("Coluna de ID não encontrada na planilha."); st.stop()

        row_mask = (df[id_col] == sel.get(id_col, ""))
        if not row_mask.any():
            st.error("Não foi possível localizar a linha do produto para atualizar."); st.stop()

        for k, v in updates.items():
            if k in df.columns:
                df.loc[row_mask, k] = v

        # persistir
        ws = _sheet().worksheet(ABA)
        df_old = get_as_dataframe(ws, evaluate_formulas=False, dtype=str, header=0)
        df_old.columns = [c.strip() for c in df_old.columns]

        ids = df_old[id_col].tolist() if id_col in df_old.columns else []
        for _, row in df.iterrows():
            rid = row.get(id_col, "")
            if rid in ids:
                i = ids.index(rid)
                for col, val in updates.items():
                    if col in df_old.columns:
                        df_old.loc[i, col] = row.get(col, val)

        ws.clear()
        set_with_dataframe(ws, df_old.fillna(""), include_index=False, include_column_header=True, resize=True)

        _msg_ok("Produto atualizado com sucesso! Abra o catálogo para ver a mudança.")

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
            categoria = st.text_input("Categoria")
        with c3:
            fornecedor = st.text_input("Fornecedor")

        c4, c5, c6 = st.columns([1,1,1])
        with c4:
            preco = st.text_input("Preço venda (R$)", placeholder="19,90")
        with c5:
            estoque = st.text_input("Estoque atual (un)", placeholder="0")
        with c6:
            ativo = st.checkbox("Ativo", value=True)

        c7, c8 = st.columns([1,2])
        with c7:
            codb = st.text_input("Código de Barras (opcional)")
        with c8:
            desc = st.text_area("Descrição/Observações (opcional)", height=100)

        salvar = st.form_submit_button("➕ Cadastrar produto")

    if salvar:
        if not nome.strip():
            st.error("Informe o **Nome**."); st.stop()

        pf = _to_float(preco)
        if pf == "":
            st.error("Preço inválido. Use números (ex: 19,90)."); st.stop()

        est = _to_int(estoque)
        if est == "":
            st.error("Estoque inválido. Use número inteiro."); st.stop()

        novo_id = _gen_id()

        # Monta uma linha nova respeitando as colunas existentes
        new_row = {}
        # Preenche somente colunas que existem
        if COL["id"]:        new_row[COL["id"]] = novo_id
        if COL["nome"]:      new_row[COL["nome"]] = nome.strip()
        if COL["categoria"]: new_row[COL["categoria"]] = categoria.strip()
        if COL["forn"]:      new_row[COL["forn"]] = fornecedor.strip()
        if COL["preco"]:     new_row[COL["preco"]] = f"{pf:.2f}".replace(".", ",")
        if COL["estoque"]:   new_row[COL["estoque"]] = str(est)
        if COL["ativo"]:     new_row[COL["ativo"]] = "sim" if ativo else "não"
        if COL["codb"]:      new_row[COL["codb"]] = codb.strip()
        if COL["desc"]:      new_row[COL["desc"]] = desc.strip()
        if COL["atualizado"]: new_row[COL["atualizado"]] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")

        # Append preservando header
        ws = _sheet().worksheet(ABA)
        df_atual = get_as_dataframe(ws, evaluate_formulas=False, dtype=str, header=0)
        df_atual.columns = [c.strip() for c in df_atual.columns]

        # garante que todas as colunas da planilha existam na linha nova (com vazio)
        for col in df_atual.columns:
            if col not in new_row:
                new_row[col] = ""

        df_atual = pd.concat([df_atual, pd.DataFrame([new_row])], ignore_index=True)
        set_with_dataframe(ws, df_atual.fillna(""), include_index=False, include_column_header=True, resize=True)

        _msg_ok("Produto cadastrado com sucesso! Ele já aparece no catálogo.")
        st.toast("Cadastro concluído ✅", icon="✅")
        st.balloons()

st.divider()
# ajuste o caminho se seu arquivo do catálogo tiver outro nome
st.page_link("pages/01_produtos.py", label="↩️ Ir para Catálogo de Produtos", icon="📦")
