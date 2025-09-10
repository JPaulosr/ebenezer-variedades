# -*- coding: utf-8 -*-
# pages/02_cadastrar_produto.py — Cadastro/edição + estoque inicial/compra juntos (com Telegram igual ao 00_vendas.py)
import json, unicodedata, math
import streamlit as st
import pandas as pd
import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta, date
import requests  # Telegram

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
    try: return float(s)
    except: return ""

def _to_int(x):
    if x is None or str(x).strip()=="":
        return ""
    try: return int(float(str(x).strip().replace(",", ".")))
    except: return ""

def _gen_id():
    return "P-" + datetime.now().strftime("%Y%m%d%H%M%S")

def _msg_ok(msg):
    st.success(msg)
    try: st.cache_data.clear()
    except: pass

def _ensure_ws(name: str, headers: list[str]):
    sh = _sheet()
    try:
        ws = sh.worksheet(name)
        cur = get_as_dataframe(ws, evaluate_formulas=False, header=0)
        if cur.empty or any(h not in cur.columns for h in headers):
            cols = list(dict.fromkeys(headers + cur.columns.tolist()))
            df_head = pd.DataFrame(columns=cols)
            ws.clear()
            set_with_dataframe(ws, df_head, include_index=False, include_column_header=True, resize=True)
        return ws
    except Exception:
        ws = sh.add_worksheet(title=name, rows=2, cols=max(10, len(headers)))
        df_head = pd.DataFrame(columns=headers)
        set_with_dataframe(ws, df_head, include_index=False, include_column_header=True, resize=True)
        return ws

def _append_row(ws, row: dict):
    cur = get_as_dataframe(ws, evaluate_formulas=False, header=0)
    for col in cur.columns:
        row.setdefault(col, "")
    out = pd.concat([cur, pd.DataFrame([row])], ignore_index=True)
    ws.clear()
    set_with_dataframe(ws, out.fillna(""), include_index=False, include_column_header=True, resize=True)

def _fmt_money(v) -> str:
    try:
        return f"R$ {float(v):.2f}".replace(".", ",")
    except:
        return str(v)

# =============================================================================
# Telegram — MESMO PADRÃO DO 00_vendas.py
# =============================================================================
def _tg_enabled() -> bool:
    try:
        return str(st.secrets.get("TELEGRAM_ENABLED", "0")) == "1"
    except Exception:
        return False

def _tg_conf():
    token = st.secrets.get("TELEGRAM_TOKEN", "")
    # usa LOJINHA primeiro (como no 00_vendas.py); fallback para TELEGRAM_CHAT_ID
    chat_id = st.secrets.get("TELEGRAM_CHAT_ID_LOJINHA", "") or st.secrets.get("TELEGRAM_CHAT_ID", "")
    return token, chat_id

def _tg_send(msg: str):
    if not _tg_enabled(): return
    token, chat_id = _tg_conf()
    if not token or not chat_id: return
    try:
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {"chat_id": str(chat_id), "text": msg, "parse_mode": "HTML", "disable_web_page_preview": True}
        requests.post(url, json=payload, timeout=6)
    except Exception:
        pass

# =============================================================================
# Mapeamentos flexíveis
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

def _map_cols_compras(df):
    return {
        "data": _pick_col(df, ["Data","DATA"]),
        "nome": _pick_col(df, ["Produto","Nome","Descrição","Descricao"]),
        "unid": _pick_col(df, ["Unidade","Unid"]),
        "forn": _pick_col(df, ["Fornecedor","FornecedorNome"]),
        "qtd":  _pick_col(df, ["Qtd","Quantidade","Qtde"]),
        "custo_unit": _pick_col(df, ["Custo Unitário","CustoUnit","Custo Unit","PrecoUnitario","Preço Unitário"]),
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
    return {"forn": _pick_col(df, ["Fornecedor","Nome"]),
            "lead": _pick_col(df, ["LeadTimeDias","Lead Time","Lead"])}

# =============================================================================
# Carregar dados
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

# Cabeçalhos padrão (se criar do zero)
COMPRAS_HEADERS = ["Data","Produto","Unidade","Fornecedor","Qtd","Custo Unitário","Total","IDProduto","Obs"]
MOV_HEADERS     = ["Data","IDProduto","Produto","Tipo","Qtd","Obs"]

# =============================================================================
# Funções de cálculo
# =============================================================================
def _last_cost_and_unit(nome: str, fornecedor: str|None):
    if compras_df.empty or not CMP:
        return None, None
    base = compras_df.copy()
    if CMP.get("nome"):
        base = base[ base[CMP["nome"]].astype(str).str.strip().str.lower() == nome.strip().lower() ]
    if fornecedor and CMP.get("forn"):
        base = base[ base[CMP["forn"]].astype(str).str.strip().str.lower() == fornecedor.strip().lower() ]
    if base.empty:
        return None, None
    row = base.assign(__d=pd.to_datetime(base.get(CMP.get("data",""), None), errors="coerce")).sort_values("__d", ascending=False).iloc[0]
    custo = _to_float(row.get(CMP.get("custo_unit",""), ""))
    unid  = str(row.get(CMP.get("unid",""), "")).strip() or None
    return (custo if custo not in ("", None) else None), unid

def _stock_balance(prod_id: str|None, nome: str):
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
            return 0
    return int(round(saldo, 0))

def _avg_daily_sales_30d(prod_id: str|None, nome: str):
    if vendas_df.empty or not VEN or VEN.get("data") is None:
        return 0.0
    base = vendas_df.copy()
    base["__d"] = pd.to_datetime(base[VEN["data"]], errors="coerce")
    maxd = base["__d"].max()
    if pd.isna(maxd): return 0.0
    start = maxd - timedelta(days=30)
    base = base[ base["__d"] >= start ]
    if prod_id and VEN.get("id"):
        base = base[ base[VEN["id"]].astype(str) == str(prod_id) ]
    elif VEN.get("nome"):
        base = base[ base[VEN["nome"]].astype(str).str.strip().str.lower() == nome.strip().lower() ]
    if base.empty: return 0.0
    qty = base[VEN["qtd"]].apply(_to_float).sum()
    days = max((maxd - start).days, 1)
    return float(qty) / float(days)

def _lead_time_fornecedor(fornecedor: str|None):
    if forn_df.empty or not FD or not fornecedor:
        return None
    base = forn_df.copy()
    base = base[ base[FD["forn"]].astype(str).str.strip().str.lower() == fornecedor.strip().lower() ]
    if base.empty: return None
    v = _to_int(base.iloc[0].get(FD["lead"], ""))
    return v if v != "" else None

def _calc_est_min(avg_daily: float, lead_time_days: int|None):
    lt = lead_time_days if lead_time_days not in (None, "", 0) else 7
    safety = 1.2
    estmin = math.ceil(avg_daily * lt * safety)
    return estmin if estmin > 0 else 5

# =============================================================================
# UI — ação
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
    with cc0: usar_lista = st.checkbox("Selecionar da lista (auto-sugestão)", value=True)
    with cc1: apenas_ativos = st.checkbox("Apenas ativos", value=True)
    with cc2: so_estoque = st.checkbox("Somente com estoque (>0)", value=False)
    with cc3: recalc_auto = st.checkbox("Atualizar campos calculados", value=True)

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
            st.info("Nada encontrado."); st.stop()
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
            st.info("Nada encontrado."); st.stop()
        nomes_fmt = base.apply(
            lambda r: f'{str(r.get(COL["nome"],"(sem nome)"))} — {str(r.get(COL["forn"],"")).strip() or "s/ forn"} — R$ {str(r.get(COL["preco"],"")).strip()}',
            axis=1
        ).tolist()
        pos = st.selectbox("Selecione", options=range(len(base)), format_func=lambda i: nomes_fmt[i])
        sel = base.iloc[pos].to_dict()

    st.subheader("Editar")
    with st.form("editar_produto"):
        c1, c2, c3 = st.columns([1.6,1,1])
        with c1: nome = st.text_input("Nome", value=str(sel.get(COL["nome"],"")).strip())
        with c2: categoria = st.text_input("Categoria", value=str(sel.get(COL["categoria"],"")).strip())
        with c3: fornecedor = st.text_input("Fornecedor", value=str(sel.get(COL["forn"],"")).strip())
        c4, c5, c6 = st.columns([1,1,1])
        with c4: preco = st.text_input("Preço venda (R$)", value=str(sel.get(COL["preco"],"")).strip())
        with c5: estoque = st.text_input("Estoque atual (un)", value=str(sel.get(COL["estoque"],"")).strip())
        with c6:
            ativo_flag = str(sel.get(COL["ativo"],"")).strip().lower() in ["1","true","sim","ativo","yes"]
            ativo = st.checkbox("Ativo", value=ativo_flag)

        # --- compra/entrada opcional na edição ---
        st.markdown("#### 🧾 Lançar nova compra/entrada (opcional)")
        c7, c8, c9, c10 = st.columns([1,1,1,1])
        with c7: data_compra_e = st.date_input("Data da compra", value=date.today(), key="dc_e")
        with c8: qtd_compra_e  = st.text_input("Qtd comprada", placeholder="Ex.: 10", key="qc_e")
        with c9: custo_unit_e  = st.text_input("Custo unitário (R$)", placeholder="Ex.: 12,50", key="cu_e")
        with c10: unid_compra_e = st.text_input("Unidade", value=str(sel.get(COL["unidade"],"")), key="un_e")
        forn_compra_e = st.text_input("Fornecedor (compra)", value=fornecedor, key="fo_e")
        obs_compra_e  = st.text_input("Observações (opcional)", key="ob_e")

        salvar = st.form_submit_button("💾 Atualizar produto")

    if salvar:
        if not nome.strip():
            st.error("Informe o **Nome**."); st.stop()
        pf = _to_float(preco)
        if pf == "": st.error("Preço inválido. Use números (ex: 19,90)."); st.stop()
        est_in = _to_int(estoque); est_in = None if est_in == "" else est_in

        updates = {}
        if COL["nome"]:      updates[COL["nome"]] = nome.strip()
        if COL["categoria"]: updates[COL["categoria"]] = categoria.strip()
        if COL["forn"]:      updates[COL["forn"]] = fornecedor.strip()
        if COL["preco"]:     updates[COL["preco"]] = f"{pf:.2f}".replace(".", ",")

        # compra/entrada opcional
        qtd_f = _to_float(qtd_compra_e)
        cst_f = _to_float(custo_unit_e)
        fazer_compra_e = (qtd_f not in ("", None, 0)) and (cst_f not in ("", None, 0))
        if fazer_compra_e:
            ws_compras = _ensure_ws("Compras", COMPRAS_HEADERS)
            total = round(float(qtd_f) * float(cst_f), 2)
            prod_id = sel.get(COL["id"], sel.get("ID", ""))
            _append_row(ws_compras, {
                "Data": data_compra_e.strftime("%d/%m/%Y"),
                "Produto": nome.strip(),
                "Unidade": (unid_compra_e or "").strip(),
                "Fornecedor": (forn_compra_e or "").strip(),
                "Qtd": str(int(qtd_f)) if float(qtd_f).is_integer() else str(qtd_f).replace(".", ","),
                "Custo Unitário": f"{float(cst_f):.2f}".replace(".", ","),
                "Total": f"{total:.2f}".replace(".", ","),
                "IDProduto": prod_id,
                "Obs": obs_compra_e or ""
            })
            ws_mov = _ensure_ws("MovimentosEstoque", MOV_HEADERS)
            _append_row(ws_mov, {
                "Data": data_compra_e.strftime("%d/%m/%Y"),
                "IDProduto": prod_id,
                "Produto": nome.strip(),
                "Tipo": "entrada",
                "Qtd": str(int(qtd_f)) if float(qtd_f).is_integer() else str(qtd_f).replace(".", ","),
                "Obs": f"Compra — {obs_compra_e or ''}".strip()
            })
            if COL["custo"]:   updates[COL["custo"]] = f"{float(cst_f):.2f}".replace(".", ",")
            if COL["estoque"]: updates[COL["estoque"]] = str(int((_stock_balance(prod_id, nome) or 0)))

            # >>> Telegram (compra/entrada na edição) — igual 00_vendas.py
            msg = (
                "🧾 <b>Compra/Entrada registrada</b>\n"
                f"<b>Produto:</b> {nome}\n"
                f"<b>Qtd:</b> {int(qtd_f) if float(qtd_f).is_integer() else qtd_f}\n"
                f"<b>Custo unit.:</b> {_fmt_money(cst_f)}\n"
                f"<b>Total:</b> {_fmt_money(total)}\n"
                f"<b>Fornecedor:</b> {(forn_compra_e or '').strip() or '—'}\n"
                f"<b>Data:</b> {data_compra_e.strftime('%d/%m/%Y')}"
            )
            _tg_send(msg)

        # Recalc automáticos (estoque min, lead etc.)
        if recalc_auto or fazer_compra_e:
            custo_prev, unid_prev = _last_cost_and_unit(nome, fornecedor)
            lead_prev = _lead_time_fornecedor(fornecedor)
            avg30 = _avg_daily_sales_30d(sel.get(COL["id"], ""), nome)
            estmin = _calc_est_min(avg30, lead_prev)
            if (custo_prev is not None) and COL["custo"]:   updates.setdefault(COL["custo"], f"{custo_prev:.2f}".replace(".", ","))
            if unid_prev and COL["unidade"]:                updates.setdefault(COL["unidade"], unid_prev)
            if (lead_prev is not None) and COL["lead"]:     updates.setdefault(COL["lead"], str(lead_prev))
            saldo = _stock_balance(sel.get(COL["id"], ""), nome)
            if COL["estoque"]:                               updates.setdefault(COL["estoque"], str(saldo if saldo is not None else (est_in or 0)))
            if COL["est_min"]:                               updates[COL["est_min"]] = str(estmin)
        else:
            if est_in is not None and COL["estoque"]:
                updates[COL["estoque"]] = str(est_in)

        if COL["ativo"]:       updates[COL["ativo"]] = "sim" if ativo else "não"
        if COL["atualizado"]:  updates[COL["atualizado"]] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")

        id_col = COL["id"] or "ID"
        if id_col not in df.columns:
            st.error("Coluna de ID não encontrada."); st.stop()
        row_mask = (df[id_col] == sel.get(id_col, ""))
        if not row_mask.any():
            st.error("Linha não localizada."); st.stop()

        ws = _sheet().worksheet(ABA)
        df_old = _load_df(ABA)
        ids = df_old[id_col].tolist()
        i = ids.index(sel.get(id_col, "")) if sel.get(id_col, "") in ids else None
        if i is None:
            st.error("Falha ao localizar a linha."); st.stop()
        for col, val in updates.items():
            if col in df_old.columns:
                df_old.loc[i, col] = val
        ws.clear()
        set_with_dataframe(ws, df_old.fillna(""), include_index=False, include_column_header=True, resize=True)
        _msg_ok("Produto atualizado com sucesso! 👍")

# =============================================================================
# CADASTRAR NOVO
# =============================================================================
else:
    st.subheader("Cadastrar novo produto")
    with st.form("cadastrar_produto"):
        c1, c2, c3 = st.columns([1.6,1,1])
        with c1: nome = st.text_input("Nome")
        with c2: categoria = st.text_input("Categoria", placeholder="Ex.: limpeza, higiene…")
        with c3: fornecedor = st.text_input("Fornecedor")
        c4, c5 = st.columns([1,1])
        with c4: preco = st.text_input("Preço venda (R$)", placeholder="19,90")
        with c5: ativo = st.checkbox("Ativo", value=True)

        st.markdown("#### 📦 Estoque inicial (opcional, recomendado)")
        c6, c7, c8, c9 = st.columns([1,1,1,1])
        with c6: qtd_compra = st.text_input("Qtd comprada", placeholder="Ex.: 10")
        with c7: custo_unit = st.text_input("Custo unitário (R$)", placeholder="Ex.: 12,50")
        with c8: unid_compra = st.text_input("Unidade", placeholder="Ex.: un, cx, pct")
        with c9: data_compra = st.date_input("Data da compra", value=date.today())
        forn_compra = st.text_input("Fornecedor (compra)", value=fornecedor)
        obs_compra  = st.text_input("Observações (opcional)")

        salvar = st.form_submit_button("➕ Cadastrar produto")

    if salvar:
        if not nome.strip():
            st.error("Informe o **Nome**."); st.stop()
        pf = _to_float(preco)
        if pf == "":
            st.error("Preço inválido. Use números (ex: 19,90)."); st.stop()

        novo_id = _gen_id()
        # calculados básicos
        custo_hist, unid_hist = _last_cost_and_unit(nome, fornecedor)
        lead = _lead_time_fornecedor(fornecedor)
        saldo = _stock_balance(None, nome)
        avg30 = _avg_daily_sales_30d(None, nome)
        estmin = _calc_est_min(avg30, lead)

        new_row = {}
        if COL["id"]:        new_row[COL["id"]] = novo_id
        if COL["nome"]:      new_row[COL["nome"]] = nome.strip()
        if COL["categoria"]: new_row[COL["categoria"]] = categoria.strip()
        if COL["forn"]:      new_row[COL["forn"]] = fornecedor.strip()
        if COL["preco"]:     new_row[COL["preco"]] = f"{pf:.2f}".replace(".", ",")
        if (custo_hist is not None) and COL["custo"]: new_row[COL["custo"]] = f"{custo_hist:.2f}".replace(".", ",")
        if COL["estoque"]:   new_row[COL["estoque"]] = str(saldo if saldo is not None else 0)
        if COL["est_min"]:   new_row[COL["est_min"]] = str(estmin)
        if (lead is not None) and COL["lead"]: new_row[COL["lead"]] = str(lead)
        if unid_hist and COL["unidade"]: new_row[COL["unidade"]] = unid_hist
        if COL["ativo"]:     new_row[COL["ativo"]] = "sim" if ativo else "não"
        if COL["atualizado"]: new_row[COL["atualizado"]] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")

        # se tiver estoque inicial informado, grava Compra + Movimento e ajusta custo/estoque
        qtd_f = _to_float(qtd_compra)
        cst_f = _to_float(custo_unit)
        fazer_compra = (qtd_f not in ("", None, 0)) and (cst_f not in ("", None, 0))
        if fazer_compra:
            ws_compras = _ensure_ws("Compras", COMPRAS_HEADERS)
            total = round(float(qtd_f) * float(cst_f), 2)
            _append_row(ws_compras, {
                "Data": data_compra.strftime("%d/%m/%Y"),
                "Produto": nome.strip(),
                "Unidade": (unid_compra or "").strip(),
                "Fornecedor": (forn_compra or fornecedor or "").strip(),
                "Qtd": str(int(qtd_f)) if float(qtd_f).is_integer() else str(qtd_f).replace(".", ","),
                "Custo Unitário": f"{float(cst_f):.2f}".replace(".", ","),
                "Total": f"{total:.2f}".replace(".", ","),
                "IDProduto": new_row.get(COL["id"], novo_id),
                "Obs": obs_compra or ""
            })
            ws_mov = _ensure_ws("MovimentosEstoque", MOV_HEADERS)
            _append_row(ws_mov, {
                "Data": data_compra.strftime("%d/%m/%Y"),
                "IDProduto": new_row.get(COL["id"], novo_id),
                "Produto": nome.strip(),
                "Tipo": "entrada",
                "Qtd": str(int(qtd_f)) if float(qtd_f).is_integer() else str(qtd_f).replace(".", ","),
                "Obs": f"Compra inicial — {obs_compra or ''}".strip()
            })
            if COL["custo"]:   new_row[COL["custo"]] = f"{float(cst_f):.2f}".replace(".", ",")
            if COL["estoque"]: new_row[COL["estoque"]] = str(int((_stock_balance(None, nome) or 0)))

        # grava na aba Produtos
        ws = _sheet().worksheet(ABA)
        df_atual = _load_df(ABA)
        for col in df_atual.columns:
            new_row.setdefault(col, "")
        df_out = pd.concat([df_atual, pd.DataFrame([new_row])], ignore_index=True)
        set_with_dataframe(ws, df_out.fillna(""), include_index=False, include_column_header=True, resize=True)

        # >>> Telegram (cadastro de novo produto) — igual 00_vendas.py
        msg = (
            "➕ <b>Novo produto cadastrado</b>\n"
            f"<b>Nome:</b> {nome.strip()}\n"
            f"<b>Categoria:</b> {categoria.strip() or '—'}\n"
            f"<b>Fornecedor:</b> {fornecedor.strip() or '—'}\n"
            f"<b>Preço venda:</b> {_fmt_money(pf)}\n"
            f"<b>ID:</b> {new_row.get(COL['id'], novo_id)}"
        )
        if fazer_compra:
            msg += (
                "\n<b>Estoque inicial:</b>\n"
                f"• Qtd: {int(qtd_f) if float(qtd_f).is_integer() else qtd_f}\n"
                f"• Custo unit.: {_fmt_money(cst_f)}\n"
                f"• Total: {_fmt_money(float(qtd_f)*float(cst_f))}\n"
                f"• Data: {data_compra.strftime('%d/%m/%Y')}"
            )
        _tg_send(msg)

        _msg_ok("Produto cadastrado com sucesso! ✅")
        st.toast("Cadastro concluído", icon="✅")
        st.balloons()

st.divider()
st.page_link("pages/01_produtos.py", label="↩️ Ir para Catálogo de Produtos", icon="📦")
st.page_link("pages/03_compras_entradas.py", label="🧾 Ir para Compras/Entradas", icon="🧾")
