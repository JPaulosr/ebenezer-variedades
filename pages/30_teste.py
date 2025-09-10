# -*- coding: utf-8 -*-
# pages/05_fracionar.py — Fracionar GRANEL (L) em fracionados (1L, 500ml, etc.)
import json, unicodedata, hashlib
from datetime import datetime, date, timedelta

import streamlit as st
import pandas as pd
import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from google.oauth2.service_account import Credentials

st.set_page_config(page_title="Fracionar (Granel → Fracionados)", page_icon="🧪", layout="wide")
st.title("🧪 Fracionar — converter GRANEL (L) em fracionados")

# =============================================================================
# Auth / Sheets helpers
# =============================================================================
def _normalize_private_key(key: str) -> str:
    if not isinstance(key, str): return key
    key = key.replace("\\n", "\n")
    key = "".join(ch for ch in key if unicodedata.category(ch)[0] != "C" or ch in ("\n","\r","\t"))
    return key

def _load_sa() -> dict:
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
    try: return _load_df(aba)
    except Exception: return pd.DataFrame()

def _ensure_ws(name: str, headers: list[str]):
    sh = _sheet()
    try:
        ws = sh.worksheet(name)
    except Exception:
        ws = sh.add_worksheet(title=name, rows=2, cols=max(10, len(headers)))
        df_head = pd.DataFrame(columns=headers)
        set_with_dataframe(ws, df_head, include_index=False, include_column_header=True, resize=True)
        return ws
    # garante colunas sem perder dados
    cur = get_as_dataframe(ws, evaluate_formulas=True, dtype=str, header=0)
    cur.columns = [c.strip() for c in cur.columns]
    missing = [h for h in headers if h not in cur.columns]
    if missing:
        for h in missing: cur[h] = ""
        ws.clear()
        set_with_dataframe(ws, cur.fillna(""), include_index=False, include_column_header=True, resize=True)
    return ws

def _rewrite_append(ws, row: dict):
    cur = get_as_dataframe(ws, evaluate_formulas=True, dtype=str, header=0)
    if cur is None or cur.empty:
        cur = pd.DataFrame(columns=list(row.keys()))
    for c in cur.columns:
        row.setdefault(c, "")
    out = pd.concat([cur, pd.DataFrame([row])], ignore_index=True)
    ws.clear()
    set_with_dataframe(ws, out.fillna(""), include_index=False, include_column_header=True, resize=True)

# =============================================================================
# Normalização / números
# =============================================================================
def _to_float(x):
    if x is None or str(x).strip()=="":
        return ""
    s = str(x).strip().replace("R$","").replace(".","").replace(",",".")
    try: return float(s)
    except: return ""

def _to_int(x):
    if x is None or str(x).strip()=="":
        return ""
    try: return int(float(str(x).strip().replace(",", ".")))
    except: return ""

# =============================================================================
# Mapas de colunas
# =============================================================================
def _pick_col(df, candidates):
    for c in candidates:
        if c in df.columns: return c
    return None

def _map_cols_produtos(df):
    return {
        "id":        _pick_col(df, ["ID","Id","id"]),
        "nome":      _pick_col(df, ["Nome","Produto","Descrição","Descricao"]),
        "unidade":   _pick_col(df, ["Unidade","Unid"]),
        "forn":      _pick_col(df, ["Fornecedor","FornecedorNome"]),
        "custo":     _pick_col(df, ["CustoAtual","Custo","Custo Atual"]),
        "preco":     _pick_col(df, ["PreçoVenda","PrecoVenda","Preço","Valor"]),
        "estoque":   _pick_col(df, ["EstoqueAtual","Estoque","QtdEstoque","Quantidade"]),
        "ativo":     _pick_col(df, ["Ativo?","Ativo","Status"]),
    }

def _map_cols_mov(df):
    return {
        "data": _pick_col(df, ["Data","DATA"]),
        "id":   _pick_col(df, ["IDProduto","ID"]),
        "nome": _pick_col(df, ["Produto","Nome"]),
        "tipo": _pick_col(df, ["Tipo","Movimento","Mov"]),
        "qtd":  _pick_col(df, ["Qtd","Quantidade","Qtde"]),
        "obs":  _pick_col(df, ["Obs","Observação","Observacoes","Observações"]),
    }

def _map_cols_compras(df):
    return {
        "data": _pick_col(df, ["Data","DATA"]),
        "nome": _pick_col(df, ["Produto","Nome"]),
        "unid": _pick_col(df, ["Unidade","Unid"]),
        "forn": _pick_col(df, ["Fornecedor","FornecedorNome"]),
        "qtd":  _pick_col(df, ["Qtd","Quantidade","Qtde"]),
        "custo_unit": _pick_col(df, ["Custo Unitário","CustoUnit","Preço Unitário"]),
        "total": _pick_col(df, ["Total","ValorTotal"]),
        "id": _pick_col(df, ["IDProduto","ID"]),
        "refid": _pick_col(df, ["RefID"]),
        "obs": _pick_col(df, ["Obs","Observação","Observacoes","Observações"]),
    }

# =============================================================================
# Carrega bases
# =============================================================================
df_prod = _safe_load("Produtos")
df_mov  = _safe_load("MovimentosEstoque")
df_comp = _safe_load("Compras")

COLP = _map_cols_produtos(df_prod) if not df_prod.empty else {}
COLM = _map_cols_mov(df_mov) if not df_mov.empty else {}
COLC = _map_cols_compras(df_comp) if not df_comp.empty else {}

COMPRAS_HEADERS = ["Data","Produto","Unidade","Fornecedor","Qtd","Custo Unitário","Total","IDProduto","Obs","RefID"]
MOV_HEADERS     = ["Data","IDProduto","Produto","Tipo","Qtd","Obs"]

# =============================================================================
# Estoque (saldo) a partir de Movimentos
# =============================================================================
def _stock_balance(prod_id: str|None, nome: str):
    if df_mov.empty or not COLM: return 0
    base = df_mov.copy()
    if prod_id and COLM.get("id"):
        base = base[ base[COLM["id"]].astype(str) == str(prod_id) ]
    elif COLM.get("nome"):
        base = base[ base[COLM["nome"]].astype(str).str.strip().str.lower() == nome.strip().lower() ]
    if base.empty: return 0
    ent = base[ base[COLM["tipo"]].astype(str).str.lower().isin(["entrada","ajuste+","entrada manual","compra","in"]) ][COLM["qtd"]].apply(_to_float).sum()
    sai = base[ base[COLM["tipo"]].astype(str).str.lower().isin(["saida","venda","ajuste-","saída manual","out"]) ][COLM["qtd"]].apply(_to_float).sum()
    saldo = (ent or 0) - (sai or 0)
    try: return round(float(saldo), 3)
    except: return 0

# =============================================================================
# Anti duplicidade para "Compras internas"
# =============================================================================
def _norm_val_str(x) -> str:
    if x is None: return ""
    s = str(x).strip().replace("R$","").replace(".","").replace(",",".")
    try: return f"{float(s):.6f}"
    except: return str(x).strip().lower()

def _make_refid_compra(data_str, produto, fornecedor, qtd, custo_unit) -> str:
    base = "|".join([
        (data_str or "").strip(),
        (produto or "").strip().lower(),
        (fornecedor or "").strip().lower(),
        _norm_val_str(qtd),
        _norm_val_str(custo_unit),
    ])
    return hashlib.sha1(base.encode("utf-8")).hexdigest()[:16]

def _seen_refid_in_session(refid: str) -> bool:
    bag = st.session_state.setdefault("_seen_compra_refids", set())
    if refid in bag: return True
    bag.add(refid); return False

def _compra_exists(ws, refid: str) -> bool:
    try:
        cur = get_as_dataframe(ws, evaluate_formulas=True, dtype=str, header=0).dropna(how="all")
    except Exception:
        return False
    if cur.empty or "RefID" not in [c.strip() for c in cur.columns]: return False
    return refid in cur["RefID"].astype(str).tolist()

# =============================================================================
# UI — seleção do granel e linhas de saída
# =============================================================================
if df_prod.empty or not COLP:
    st.error("Aba 'Produtos' vazia ou não mapeada."); st.stop()

# candidatos a granel: unidade 'L' ou nome contém 'granel'
def _is_granel_row(r):
    un = str(r.get(COLP["unidade"], "")).strip().lower()
    nm = str(r.get(COLP["nome"], "")).strip().lower()
    return ("l" in un) or ("litro" in un) or ("granel" in nm)

df_granel = df_prod[df_prod.apply(_is_granel_row, axis=1)].copy()
if df_granel.empty:
    st.warning("Nenhum produto granel (L) encontrado. Cadastre um SKU de matéria-prima em litros.")
    st.stop()

# formatador
def _fmt_prod(r):
    nm = str(r.get(COLP["nome"], "(sem nome)"))
    un = str(r.get(COLP["unidade"], "") or "").strip()
    forn = str(r.get(COLP["forn"], "") or "").strip()
    return f"{nm} [{un}]" + (f" — {forn}" if forn else "")

labels_granel = df_granel.apply(_fmt_prod, axis=1).tolist()
idx = st.selectbox("Materia-prima (GRANEL em L)", options=range(len(df_granel)), format_func=lambda i: labels_granel[i])
row_g = df_granel.iloc[idx].to_dict()

prod_id_g   = row_g.get(COLP["id"], "")
nome_g      = str(row_g.get(COLP["nome"], ""))
unid_g      = str(row_g.get(COLP["unidade"], "") or "")
saldo_l     = _stock_balance(prod_id_g, nome_g)  # em litros

c1, c2, c3 = st.columns([1,1,1])
with c1: st.metric("Estoque atual do GRANEL (L)", f"{saldo_l}")
with c2: data_op = st.date_input("Data da operação", value=date.today())
with c3: gerar_compras = st.checkbox("Gerar 'Compras internas' p/ fracionados (ajusta CustoAtual)", value=True)

st.caption("Dica: se não quiser afetar CustoAtual dos fracionados, desmarque a opção de 'Compras internas' e deixe só os MovimentosEstoque.")

st.markdown("### Saídas (fracionados)")
# linhas dinâmicas
if "_frac_rows" not in st.session_state:
    st.session_state["_frac_rows"] = [
        {"label":"SKU 1L", "sku": "", "vol_l": "1", "qtd": "", "embalagem": ""},   # custo adicional da embalagem
        {"label":"SKU 500 ml", "sku": "", "vol_l": "0,5", "qtd": "", "embalagem": ""},
    ]

# catálogo para saída (todos exceto o granel selecionado)
df_out_options = df_prod.copy()
df_out_options = df_out_options[df_out_options[COLP["id"]] != prod_id_g]

def _fmt_out(r):
    nm = str(r.get(COLP["nome"], "(sem nome)"))
    un = str(r.get(COLP["unidade"], "") or "").strip()
    return f"{nm} [{un}]"

labels_out = df_out_options.apply(_fmt_out, axis=1).tolist()

# editor simples de linhas
rows = st.session_state["_frac_rows"]
for i, row in enumerate(rows):
    st.write(f"**Linha {i+1} — {row['label']}**")
    c1, c2, c3, c4 = st.columns([2,1,1,1])
    with c1:
        idx_out = st.selectbox("SKU de saída", options=["(selecione)"] + labels_out, key=f"sku_{i}")
        row["sku"] = idx_out
    with c2:
        row["vol_l"] = st.text_input("Volume por unidade (L)", value=row["vol_l"], key=f"vol_{i}")
    with c3:
        row["qtd"] = st.text_input("Quantidade a produzir", value=row["qtd"], key=f"qtd_{i}")
    with c4:
        row["embalagem"] = st.text_input("Custo embalagem por un. (R$)", value=row["embalagem"], key=f"emb_{i}")
    st.divider()

colb1, colb2, colb3 = st.columns([1,1,2])
with colb1:
    if st.button("➕ Adicionar linha"):
        rows.append({"label":f"SKU extra {len(rows)+1}", "sku":"", "vol_l":"", "qtd":"", "embalagem":""})
with colb2:
    if st.button("🗑️ Limpar linhas"):
        st.session_state["_frac_rows"] = []
        st.rerun()

# =============================================================================
# Simulação
# =============================================================================
def _find_row_by_label(label):
    if label == "(selecione)": return None
    if label not in labels_out: return None
    pos = labels_out.index(label)
    return df_out_options.iloc[pos].to_dict()

def _calc_litros_usados(rows):
    total = 0.0
    for r in rows:
        if not r.get("sku") or r["sku"]=="(selecione)": 
            continue
        vol = _to_float(r.get("vol_l", ""))
        qtd = _to_float(r.get("qtd", ""))
        if vol not in ("", None) and qtd not in ("", None):
            total += float(vol) * float(qtd)
    return round(total, 3)

litros_usados = _calc_litros_usados(rows)
st.metric("Litros que serão usados", f"{litros_usados}")

if litros_usados > saldo_l:
    st.error("Litros usados excedem o estoque atual do granel.")
elif litros_usados == 0:
    st.info("Preencha volume e quantidade das saídas para simular.")

# =============================================================================
# Confirmar
# =============================================================================
batch_id = "FRAC-" + datetime.now().strftime("%Y%m%d%H%M%S")

def _append_mov_saida_granel(ws_mov, data_str, qtd_l):
    _rewrite_append(ws_mov, {
        "Data": data_str,
        "IDProduto": prod_id_g,
        "Produto": nome_g,
        "Tipo": "ajuste-",
        "Qtd": str(qtd_l).replace(".", ","),
        "Obs": f"{batch_id} — saída para fracionamento"
    })

def _append_mov_entrada_saida(ws_mov, data_str, prod_dict, qtd_un):
    _rewrite_append(ws_mov, {
        "Data": data_str,
        "IDProduto": prod_dict.get(COLP["id"], ""),
        "Produto": prod_dict.get(COLP["nome"], ""),
        "Tipo": "ajuste+",
        "Qtd": str(int(qtd_un)) if float(qtd_un).is_integer() else str(qtd_un).replace(".", ","),
        "Obs": f"{batch_id} — entrada de fracionado"
    })

def _upsert_compra_interna(ws_cmp, data_str, prod_dict, qtd_un, custo_unit, obs_extra=""):
    refid = _make_refid_compra(
        data_str,
        prod_dict.get(COLP["nome"], ""),
        "Produção interna",
        str(int(qtd_un)) if float(qtd_un).is_integer() else str(qtd_un).replace(".", ","),
        f"{float(custo_unit):.2f}".replace(".", ","),
    )
    if _seen_refid_in_session(refid):
        return False, refid
    if _compra_exists(ws_cmp, refid):
        return False, refid

    total = round(float(qtd_un) * float(custo_unit), 2)
    _rewrite_append(ws_cmp, {
        "Data": data_str,
        "Produto": prod_dict.get(COLP["nome"], ""),
        "Unidade": prod_dict.get(COLP["unidade"], "") or "un",
        "Fornecedor": "Produção interna",
        "Qtd": str(int(qtd_un)) if float(qtd_un).is_integer() else str(qtd_un).replace(".", ","),
        "Custo Unitário": f"{float(custo_unit):.2f}".replace(".", ","),
        "Total": f"{total:.2f}".replace(".", ","),
        "IDProduto": prod_dict.get(COLP["id"], ""),
        "Obs": (obs_extra or "").strip(),
        "RefID": refid
    })
    return True, refid

st.markdown("### Confirmar operação")
ok_btn = st.button("✅ Gerar fracionamento agora")

if ok_btn:
    data_str = data_op.strftime("%d/%m/%Y")

    if litros_usados <= 0:
        st.error("Preencha pelo menos uma linha válida (SKU, volume e quantidade)."); st.stop()
    if litros_usados > saldo_l:
        st.error("Litros usados excedem o estoque atual do granel."); st.stop()

    # prepara planilhas
    ws_mov = _ensure_ws("MovimentosEstoque", MOV_HEADERS)
    ws_cmp = _ensure_ws("Compras", COMPRAS_HEADERS)

    # 1) saída do granel
    _append_mov_saida_granel(ws_mov, data_str, litros_usados)

    # 2) entradas dos fracionados (+ compras internas opcionais)
    criados = []
    for r in rows:
        if not r.get("sku") or r["sku"] == "(selecione)": 
            continue
        vol = _to_float(r.get("vol_l", ""))
        qtd = _to_float(r.get("qtd", ""))
        emb = _to_float(r.get("embalagem", "")) or 0.0
        if vol in ("", None) or qtd in ("", None) or qtd == 0:
            continue

        prod_out = _find_row_by_label(r["sku"])
        if not prod_out:
            continue

        # custo do fracionado = (custo por litro do GRANEL) * vol + embalagem
        # custo por litro: usamos CustoAtual do granel (se existir), senão 0
        custo_granel = _to_float(row_g.get(COLP["custo"], "")) or 0.0
        custo_unit_out = round(custo_granel * float(vol) + float(emb), 4)

        # entrada no MOV
        _append_mov_entrada_saida(ws_mov, data_str, prod_out, qtd)

        # compra interna (opcional)
        if gerar_compras:
            ok, rid = _upsert_compra_interna(
                ws_cmp, data_str, prod_out, qtd, custo_unit_out,
                obs_extra=f"{batch_id} — produção interna (vol={vol}L; emb={emb})"
            )
            # ok=False significa duplicado (já existia), mas o MOV já entrou; tudo certo
        criados.append({
            "Produto": prod_out.get(COLP["nome"], ""),
            "Qtd": qtd,
            "CustoUnit(calc)": custo_unit_out
        })

    st.success("Fracionamento lançado com sucesso! ✅")
    if criados:
        st.write("**Entradas geradas (fracionados):**")
        st.dataframe(pd.DataFrame(criados))
    st.info(f"Batch: {batch_id} • Saída do granel: {litros_usados} L • Data: {data_str}")

st.divider()
st.page_link("pages/01_produtos.py", label="📦 Ir para Catálogo de Produtos", icon="📦")
st.page_link("pages/03_compras_entradas.py", label="🧾 Ir para Compras/Entradas", icon="🧾")
