# pages/04_vendas_rapidas.py — Vendas rápidas (carrinho + histórico/estorno/duplicar)
# COM FIADO + TELEGRAM + ESTOQUE (Compras/Vendas/Ajustes) + MOVIMENTOS + RESUMO DO DIA
# -*- coding: utf-8 -*-
import json, unicodedata
from datetime import datetime, date, timedelta
import streamlit as st
import pandas as pd
import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from google.oauth2.service_account import Credentials
import requests  # Telegram

st.set_page_config(page_title="Vendas rápidas", page_icon="🧾", layout="wide")
st.title("🧾 Vendas rápidas (carrinho)")

# ================= Helpers =================
def _normalize_private_key(key: str) -> str:
    if not isinstance(key, str): return key
    key = key.replace("\\n", "\n")
    key = "".join(ch for ch in key if unicodedata.category(ch)[0] != "C" or ch in ("\n", "\r", "\t"))
    return key

def _load_sa():
    svc = st.secrets.get("GCP_SERVICE_ACCOUNT")
    if svc is None: st.error("🛑 GCP_SERVICE_ACCOUNT ausente."); st.stop()
    if isinstance(svc, str): svc = json.loads(svc)
    svc = {**svc, "private_key": _normalize_private_key(svc["private_key"])}
    return svc

@st.cache_resource
def conectar_sheets():
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(_load_sa(), scopes=scopes)
    gc = gspread.authorize(creds)
    url_or_id = st.secrets.get("PLANILHA_URL", "")
    if not url_or_id: st.error("🛑 PLANILHA_URL ausente."); st.stop()
    return gc.open_by_url(url_or_id) if url_or_id.startswith("http") else gc.open_by_key(url_or_id)

@st.cache_data(ttl=10)
def carregar_aba(nome: str) -> pd.DataFrame:
    ws = conectar_sheets().worksheet(nome)
    df = get_as_dataframe(ws, evaluate_formulas=True, dtype=str, header=0).dropna(how="all")
    df.columns = [c.strip() for c in df.columns]
    return df

def _first_col(df: pd.DataFrame, candidates) -> str | None:
    if df is None or df.empty: return None
    for c in candidates:
        if c in df.columns: return c
    low = {c.lower(): c for c in df.columns}
    for c in candidates:
        if c.lower() in low: return low[c.lower()]
    return None

def _to_num(x):
    if x is None: return 0.0
    if isinstance(x, (int, float)): return float(x)
    s = str(x).strip()
    if s == "" or s.lower() in ("nan", "none"): return 0.0
    s = s.replace(".", "").replace(",", ".") if s.count(",")==1 and s.count(".")>1 else s.replace(",", ".")
    try: return float(s)
    except: return 0.0

def _fmt_brl_num(v):
    return f"R$ {float(v):,.2f}".replace(",", "X").replace(".", ",").replace("X",".")

def _gerar_id(prefixo="F"):
    return f"{prefixo}-{datetime.now().strftime('%Y%m%d%H%M%S%f')[:-3]}"

def _garantir_aba(sh, nome, cols):
    try:
        ws = sh.worksheet(nome)
    except Exception:
        ws = sh.add_worksheet(title=nome, rows=3000, cols=max(10,len(cols)))
        ws.update("A1", [cols])
        return ws
    headers = ws.row_values(1) or []
    headers = [h.strip() for h in headers]
    falt = [c for c in cols if c not in headers]
    if falt:
        ws.update("A1", [headers + falt])
    return ws

def _append_rows(ws, rows: list[dict]):
    headers = ws.row_values(1)
    hdr = [h.strip() for h in headers]
    to_append = []
    for d in rows:
        to_append.append([d.get(h, "") for h in hdr])
    if to_append:
        ws.append_rows(to_append, value_input_option="USER_ENTERED")

# -------- Telegram --------
def _tg_enabled() -> bool:
    try:
        return str(st.secrets.get("TELEGRAM_ENABLED", "0")) == "1"
    except Exception:
        return False

def _tg_conf():
    token = st.secrets.get("TELEGRAM_TOKEN", "")
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

# ---------------- Clientes ----------------
ABA_CLIENTES = "Clientes"
COLS_CLIENTES = ["Cliente","Telefone","Obs"]

def _ensure_cliente(cli_nome: str):
    """Garante que o cliente exista na aba Clientes; se não existir, cadastra."""
    cli_nome = (cli_nome or "").strip()
    if not cli_nome:
        return
    sh = conectar_sheets()
    ws_cli = _garantir_aba(sh, ABA_CLIENTES, COLS_CLIENTES)
    try:
        dfc = carregar_aba(ABA_CLIENTES)
    except Exception:
        dfc = pd.DataFrame(columns=COLS_CLIENTES)
    ja_tem = False
    if not dfc.empty:
        col_cli = "Cliente" if "Cliente" in dfc.columns else dfc.columns[0]
        ja_tem = any(str(x).strip().lower() == cli_nome.lower() for x in dfc[col_cli].dropna())
    if not ja_tem:
        _append_rows(ws_cli, [{"Cliente": cli_nome, "Telefone": "", "Obs": ""}])

def _carregar_clientes() -> list[str]:
    try:
        dfc = carregar_aba(ABA_CLIENTES)
        if dfc.empty: return []
        col_cli = "Cliente" if "Cliente" in dfc.columns else dfc.columns[0]
        return sorted(list(dict.fromkeys([str(x).strip() for x in dfc[col_cli].dropna()])))
    except Exception:
        return []

# ---------------- Catálogo / Estoque / Custo ----------------
ABA_PROD, ABA_VEND = "Produtos", "Vendas"
ABA_COMPRAS = "Compras"
ABA_AJUSTES = "Ajustes"
ABA_MOVS   = "MovimentosEstoque"
ABA_FIADO  = "Fiado"

COLS_FIADO = ["ID","Data","Cliente","Valor","Vencimento","Status","Obs","DataPagamento","FormaPagamento","ValorPago"]

# Calcula custo e estoque
def _build_maps_e_estoque():
    # Produtos
    try:
        dfp = carregar_aba(ABA_PROD)
    except Exception as e:
        st.error("Erro ao abrir a aba Produtos."); st.stop()
    col_id   = _first_col(dfp, ["ID","Codigo","Código","SKU"])
    col_nome = _first_col(dfp, ["Nome","Produto","Descrição"])
    col_preco= _first_col(dfp, ["PreçoVenda","PrecoVenda","Preço","Preco"])
    col_unid = _first_col(dfp, ["Unidade","Und"])
    col_custo= _first_col(dfp, ["Custo","PreçoCusto","PrecoCusto","CustoUnit","Custo Unidade"])
    if not col_id or not col_nome:
        st.error("A aba Produtos precisa ter colunas de ID e Nome."); st.stop()

    # Mapa básico
    dfp["_label"] = dfp.apply(lambda r: f"{str(r[col_id])} — {str(r[col_nome])}", axis=1)
    cat_map = dfp.set_index("_label")[[col_id, col_nome, col_preco, col_unid]].to_dict("index")
    labels = ["(selecione)"] + sorted(cat_map.keys())
    id_to_name = {}
    id_to_cost = {}
    for _, r in dfp.iterrows():
        pid = str(r[col_id]).strip()
        if pid:
            id_to_name[pid] = str(r.get(col_nome,"") or "").strip()
            if col_custo: id_to_cost[pid] = _to_num(r.get(col_custo))

    # Tenta obter custo (fallback) da última compra
    try:
        dcc = carregar_aba(ABA_COMPRAS)
    except Exception:
        dcc = pd.DataFrame()
    if not dcc.empty:
        col_cc_pid = _first_col(dcc, ["IDProduto","ProdutoID","ID"])
        col_cc_qtd = _first_col(dcc, ["Qtd","Quantidade"])
        col_cc_cus = _first_col(dcc, ["Custo Unitário","CustoUnit","CustoUnitário","Custo Unit","CustoUnitario"])
        col_cc_dat = _first_col(dcc, ["Data"])
        if col_cc_pid and col_cc_cus:
            # último custo por produto
            dcc["_dt"] = pd.to_datetime(dcc[col_cc_dat], format="%d/%m/%Y", errors="coerce") if col_cc_dat else pd.NaT
            dcc = dcc.sort_values("_dt")
            last_cost = dcc.groupby(col_cc_pid)[col_cc_cus].last()
            for pid, cus in last_cost.items():
                pid = str(pid)
                if pid and (pid not in id_to_cost or id_to_cost[pid]==0):
                    id_to_cost[pid] = _to_num(cus)

    # ------- ESTOQUE: Entradas(Compras) - Saídas(Vendas, líquido) + Ajustes -------
    entradas = {}
    if not dcc.empty and col_cc_pid and col_cc_qtd:
        for _, r in dcc.iterrows():
            pid = str(r.get(col_cc_pid,"")).strip()
            entradas[pid] = entradas.get(pid, 0.0) + _to_num(r.get(col_cc_qtd))

    try:
        dv = carregar_aba(ABA_VEND)
    except Exception:
        dv = pd.DataFrame()
    saidas = {}
    if not dv.empty:
        col_v_pid = _first_col(dv, ["IDProduto","ProdutoID","ID"])
        col_v_qtd = _first_col(dv, ["Qtd","Quantidade"])
        if col_v_pid and col_v_qtd:
            # soma Qtd (estorno vem negativo, então já liquida)
            for _, r in dv.iterrows():
                pid = str(r.get(col_v_pid,"")).strip()
                saidas[pid] = saidas.get(pid, 0.0) + _to_num(r.get(col_v_qtd))

    try:
        daj = carregar_aba(ABA_AJUSTES)
    except Exception:
        daj = pd.DataFrame()
    ajustes = {}
    if not daj.empty:
        col_aj_pid = _first_col(daj, ["ID","IDProduto","ProdutoID"])
        col_aj_qtd = _first_col(daj, ["Qtd","Quantidade","Qtde"])
        if col_aj_pid and col_aj_qtd:
            for _, r in daj.iterrows():
                pid = str(r.get(col_aj_pid,"")).strip()
                ajustes[pid] = ajustes.get(pid, 0.0) + _to_num(r.get(col_aj_qtd))

    id_to_stock = {}
    for pid in set(list(entradas.keys()) + list(saidas.keys()) + list(ajustes.keys()) + list(id_to_name.keys())):
        e = entradas.get(pid, 0.0)
        s = saidas.get(pid, 0.0)
        a = ajustes.get(pid, 0.0)
        id_to_stock[pid] = e - s + a

    return dfp, cat_map, labels, id_to_name, id_to_cost, id_to_stock, col_id, _first_col(dfp, ["Nome","Produto","Descrição"]), _first_col(dfp, ["PreçoVenda","PrecoVenda","Preço","Preco"]), _first_col(dfp, ["Unidade","Und"])

# ====== carrega mapas/estoque uma vez ======
dfp, cat_map, labels, id_to_name, id_to_cost, id_to_stock, col_id, col_nome, col_preco, col_unid = _build_maps_e_estoque()

# ================= Estado inicial =================
if "cart" not in st.session_state: st.session_state["cart"] = []
if "forma" not in st.session_state: st.session_state["forma"] = "Dinheiro"
if "obs" not in st.session_state:   st.session_state["obs"] = ""
if "data_venda" not in st.session_state: st.session_state["data_venda"] = date.today()
if "desc" not in st.session_state:  st.session_state["desc"] = 0.0
if "cliente" not in st.session_state: st.session_state["cliente"] = ""
if "venc_fiado" not in st.session_state: st.session_state["venc_fiado"] = date.today() + timedelta(days=30)

# ================= Carrinho =================
st.subheader("Nova venda / cupom")

# Data
cdate, = st.columns(1)
with cdate:
    st.session_state["data_venda"] = st.date_input("Data da venda", value=st.session_state["data_venda"])

with st.form("add_item"):
    sel = st.selectbox("Produto", labels, index=0)
    c1, c2, c3 = st.columns([1, 1, 1])
    with c1:
        qtd = st.number_input("Qtd", min_value=1, step=1, value=1)
    with c2:
        preco_sug = 0.0
        if sel != "(selecione)" and col_preco:
            preco_sug = _to_num(cat_map[sel].get(col_preco))
        preco = st.number_input("Preço unitário (R$)", min_value=0.0, value=float(preco_sug), step=0.1, format="%.2f")
    with c3:
        unid_show = cat_map[sel].get(col_unid) if sel != "(selecione)" and col_unid else "un"
        st.text_input("Unidade", value=str(unid_show), disabled=True)
    add = st.form_submit_button("➕ Adicionar ao carrinho", use_container_width=True)

if add:
    if sel == "(selecione)":
        st.warning("Selecione um produto.")
    else:
        info = cat_map[sel]
        st.session_state["cart"].append({
            "id": str(info[col_id]),
            "nome": str(info[col_nome]),
            "unid": str(info.get(col_unid, "un")),
            "qtd": int(qtd),
            "preco": float(preco)
        })
        st.success("Item adicionado.")

# Tabela do carrinho
st.subheader("Carrinho")
if not st.session_state["cart"]:
    st.info("Nenhum item no carrinho.")
else:
    for idx, it in enumerate(st.session_state["cart"]):
        c1, c2, c3, c4, c5, c6 = st.columns([1.3, 3.2, 1, 1.4, 1.6, 0.8])
        c1.write(f"{it['nome']}")
        with c3:
            st.session_state["cart"][idx]["qtd"] = st.number_input("Qtd", key=f"q_{idx}", min_value=1, step=1, value=int(it["qtd"]))
        with c4:
            st.session_state["cart"][idx]["preco"] = st.number_input("Preço (R$)", key=f"p_{idx}", min_value=0.0, step=0.1, value=float(it["preco"]), format="%.2f")
        subtotal = st.session_state['cart'][idx]['qtd']*st.session_state['cart'][idx]['preco']
        c2.caption(f"Estoque atual: {int(id_to_stock.get(it['id'], 0))}")
        c5.write(f"Subtotal: <b>{_fmt_brl_num(subtotal)}</b>", unsafe_allow_html=True)
        if c6.button("🗑️", key=f"rm_{idx}"):
            st.session_state["cart"].pop(idx)
            st.experimental_rerun()

    st.markdown("---")
    total_itens = sum(i["qtd"] for i in st.session_state["cart"])
    total_bruto = sum(i["qtd"]*i["preco"] for i in st.session_state["cart"])

    cL, cR = st.columns([2, 1.2])
    with cL:
        formas = ["Dinheiro","Pix","Cartão Débito","Cartão Crédito","Fiado","Outros"]
        idx_forma = formas.index(st.session_state["forma"]) if st.session_state["forma"] in formas else 0
        st.session_state["forma"] = st.selectbox("Forma de pagamento", formas, index=idx_forma)

        if st.session_state["forma"] == "Fiado":
            clientes_existentes = _carregar_clientes()
            usar_lista = st.checkbox("Selecionar cliente cadastrado", value=True)

            if usar_lista and clientes_existentes:
                sel_cli = st.selectbox("Cliente (lista)", ["(selecione)"] + clientes_existentes, index=0)
                novo_cli = st.text_input("Ou cadastrar novo cliente", value="")
                st.session_state["cliente"] = (novo_cli.strip() or (sel_cli if sel_cli != "(selecione)" else "")).strip()
            else:
                st.session_state["cliente"] = st.text_input("Cliente (obrigatório para Fiado)", value=st.session_state["cliente"])

            st.session_state["venc_fiado"] = st.date_input("Vencimento do fiado", value=st.session_state["venc_fiado"])
        else:
            st.session_state["cliente"] = st.text_input("Cliente (opcional)", value=st.session_state["cliente"])

        st.session_state["obs"]   = st.text_input("Observações (opcional)", value=st.session_state["obs"])
    with cR:
        st.session_state["desc"]  = st.number_input("Desconto (R$)", min_value=0.0, value=float(st.session_state["desc"]), step=0.5, format="%.2f")
        total_liq = max(0.0, total_bruto - float(st.session_state["desc"]))
        st.metric("Total itens", total_itens)
        st.metric("Total bruto", _fmt_brl_num(total_bruto))
        st.metric("Total líquido", _fmt_brl_num(total_liq))

    colA, colB = st.columns([1, 1])
    if colA.button("🧾 Registrar venda", type="primary", use_container_width=True):
        if not st.session_state["cart"]:
            st.warning("Carrinho vazio.")
        else:
            # garante cadastro do cliente (mesmo sem fiado)
            cli_nome = st.session_state.get("cliente","").strip()
            if cli_nome:
                _ensure_cliente(cli_nome)

            if st.session_state["forma"] == "Fiado" and not cli_nome:
                st.error("Informe o Cliente para registrar fiado."); st.stop()

            sh = conectar_sheets()

            # garante aba Vendas
            try:
                ws_v = sh.worksheet(ABA_VEND)
            except Exception:
                ws_v = sh.add_worksheet(title=ABA_VEND, rows=2000, cols=16)
                ws_v.update("A1:K1", [["Data","VendaID","IDProduto","Qtd","PrecoUnit","TotalLinha","FormaPagto","Obs","Desconto","TotalCupom","CupomStatus"]])

            dfv = get_as_dataframe(ws_v, evaluate_formulas=False, dtype=str, header=0).dropna(how="all")
            if dfv.empty:
                dfv = pd.DataFrame(columns=["Data","VendaID","IDProduto","Qtd","PrecoUnit","TotalLinha","FormaPagto","Obs","Desconto","TotalCupom","CupomStatus"])
            dfv.columns = [c.strip() for c in dfv.columns]
            for c in ["Desconto","TotalCupom","CupomStatus","Cliente","FiadoID"]:
                if c not in dfv.columns: dfv[c] = None

            # Movimentos de estoque
            try:
                ws_m = sh.worksheet(ABA_MOVS)
            except Exception:
                ws_m = _garantir_aba(sh, ABA_MOVS, ["Data","IDProduto","Produto","Tipo","Qtd","Obs","ID","Documento/NF","Origem","SaldoApós"])

            venda_id = "V-" + datetime.now().strftime("%Y%m%d%H%M%S")
            data_str = st.session_state["data_venda"].strftime("%d/%m/%Y")
            desconto = float(st.session_state["desc"])
            total_cupom = max(0.0, total_bruto - desconto)

            # ---- FIADO (opcional)
            fiado_id = ""
            fiado_msg = ""
            if st.session_state["forma"] == "Fiado":
                ws_f = _garantir_aba(sh, ABA_FIADO, COLS_FIADO)
                fiado_id = _gerar_id("F")
                venc_str = st.session_state["venc_fiado"].strftime("%d/%m/%Y") if isinstance(st.session_state["venc_fiado"], date) else ""
                linha_fiado = {
                    "ID": fiado_id,
                    "Data": data_str,
                    "Cliente": cli_nome,
                    "Valor": float(total_cupom),
                    "Vencimento": venc_str,
                    "Status": "Em aberto",
                    "Obs": st.session_state.get("obs",""),
                    "DataPagamento": "",
                    "FormaPagamento": "",
                    "ValorPago": ""
                }
                _append_rows(ws_f, [linha_fiado])
                fiado_msg = f"\n💳 <b>Fiado</b> criado para <b>{cli_nome}</b> — venc: {venc_str}"

            # ===== monta as linhas da venda (cria `novas`) =====
            novas = []
            stock_before_after = {}   # {pid: (before, after)}
            lucro_total_venda = 0.0

            for it in st.session_state["cart"]:
                pid = str(it["id"])
                nome_prod = id_to_name.get(pid, pid)
                qtd = int(it["qtd"])
                preco_unit = float(it["preco"])
                subtotal = qtd * preco_unit
                custo_unit = id_to_cost.get(pid, 0.0)
                lucro_total_venda += qtd * (preco_unit - custo_unit)

                # estoque antes/depois (calculado das abas Compras/Vendas/Ajustes)
                before = id_to_stock.get(pid, 0.0)
                after  = before - qtd
                stock_before_after[pid] = (before, after)
                # atualiza um mapa local para caso tenha o mesmo produto repetido
                id_to_stock[pid] = after

                novas.append({
                    "Data": data_str,
                    "VendaID": venda_id,
                    "IDProduto": pid,
                    "Qtd": str(qtd),
                    "PrecoUnit": f"{preco_unit:.2f}".replace(".", ","),
                    "TotalLinha": f"{subtotal:.2f}".replace(".", ","),
                    "FormaPagto": st.session_state["forma"],
                    "Obs": st.session_state["obs"],
                    "Desconto": f"{desconto:.2f}".replace(".", ","),
                    "TotalCupom": f"{total_cupom:.2f}".replace(".", ","),
                    "CupomStatus": "OK",
                    "Cliente": cli_nome,
                    "FiadoID": fiado_id
                })

            # grava Vendas
            df_novo = pd.concat([dfv, pd.DataFrame(novas)], ignore_index=True)
            ws_v.clear()
            set_with_dataframe(ws_v, df_novo)

            # registra movimentos de estoque
            movs = []
            for it in st.session_state["cart"]:
                pid = str(it["id"])
                nome_prod = id_to_name.get(pid, pid)
                qtd = int(it["qtd"])
                bef, aft = stock_before_after.get(pid, (None, None))
                movs.append({
                    "Data": data_str,
                    "IDProduto": pid,
                    "Produto": nome_prod,
                    "Tipo": "B saída",
                    "Qtd": str(qtd),
                    "Obs": st.session_state.get("obs",""),
                    "ID": venda_id,
                    "Documento/NF": "",
                    "Origem": "Vendas rápidas",
                    "SaldoApós": str(int(aft)) if aft is not None else ""
                })
            _append_rows(ws_m, movs)

            # limpa carrinho e força refresh
            st.session_state["cart"] = []
            st.cache_data.clear()
            st.session_state["_force_refresh"] = True

            # ===== TELEGRAM (sem IDs públicos) =====
            def _render_item_line(pid, bef_aft, qtd, preco):
                nome  = id_to_name.get(pid, "Produto")
                subtotal = qtd * preco
                estoque_txt = ""
                if pid in bef_aft:
                    b,a = bef_aft[pid]
                    estoque_txt = f" — <i>estoque:</i> {int(b)} → <b>{int(a)}</b>"
                return f"• <b>{nome}</b> — x{qtd} @ {_fmt_brl_num(preco)} = <b>{_fmt_brl_num(subtotal)}</b>{estoque_txt}"

            itens_txt = "\n".join(_render_item_line(str(it['id']), stock_before_after, int(it['qtd']), float(it['preco'])) for it in st.session_state.get("_last_cart", st.session_state["cart"]) or novas)
            # OBS: acima usamos backup se a sessão recarregar; mas normalmente `st.session_state["cart"]` ainda existe aqui.

            cliente_linha = f"\n👤 Cliente: <b>{cli_nome}</b>" if cli_nome else ""
            lucro_bloco = f"\n💰 Lucro (estimado): <b>{_fmt_brl_num(lucro_total_venda)}</b>" if id_to_cost else ""

            # Resumo do dia
            try:
                vend_all = carregar_aba(ABA_VEND)
            except Exception:
                vend_all = pd.DataFrame()
            resumo_dia_txt = ""
            if not vend_all.empty:
                col_data_v  = _first_col(vend_all, ["Data"])
                col_venda_v = _first_col(vend_all, ["VendaID"])
                col_qtd_v   = _first_col(vend_all, ["Qtd","Quantidade"])
                col_preco_v = _first_col(vend_all, ["PrecoUnit","Preço","PreçoUnitário","Preco"])
                col_total_v = _first_col(vend_all, ["TotalCupom"])
                if col_data_v and col_venda_v:
                    hoje_str = date.today().strftime("%d/%m/%Y")
                    dia = vend_all[vend_all[col_data_v]==hoje_str].copy()
                    if not dia.empty:
                        dia["_qtd"]   = dia[col_qtd_v].map(_to_num) if col_qtd_v else 0.0
                        dia["_preco"] = dia[col_preco_v].map(_to_num) if col_preco_v else 0.0
                        dia["_total"] = dia[col_total_v].map(_to_num) if col_total_v else (dia["_qtd"]*dia["_preco"])
                        cupons = dia.groupby(col_venda_v, dropna=False)["_total"].max().sum()
                        total_desc = 0.0
                        if "Desconto" in dia.columns:
                            total_desc = dia.groupby(col_venda_v)["Desconto"].max().map(_to_num).sum()
                        total_bruto_dia = (dia["_qtd"]*dia["_preco"]).sum()
                        total_liq_dia = cupons if cupons>0 else max(0.0, total_bruto_dia - total_desc)

                        lucro_dia = 0.0
                        if id_to_cost:
                            pid_col = _first_col(dia, ["IDProduto","ProdutoID","ID"])
                            for _, rr in dia.iterrows():
                                pid = str(rr.get(pid_col,""))
                                qtdx = _to_num(rr.get("Qtd", 0))
                                precx = _to_num(rr.get(col_preco_v, 0))
                                cx = id_to_cost.get(pid, 0.0)
                                lucro_dia += qtdx * (precx - cx)

                        # top 3 por quantidade
                        top_txt = ""
                        pid_col = _first_col(dia, ["IDProduto","ProdutoID","ID"])
                        if pid_col:
                            topg = dia.groupby(pid_col)["_qtd"].sum().sort_values(ascending=False).head(3)
                            lines = []
                            for pid, q in topg.items():
                                nm = id_to_name.get(str(pid), str(pid))
                                lines.append(f"• {nm} — x{int(q)}")
                            if lines:
                                top_txt = "\n🏅 Top 3 (qtd):\n" + "\n".join(lines)

                        resumo_lucro = f"\n💰 Lucro (estimado): <b>{_fmt_brl_num(lucro_dia)}</b>" if id_to_cost else ""
                        resumo_dia_txt = (
                            "\n\n📊 <b>Resumo do dia (até agora)</b>\n"
                            f"Bruto: {_fmt_brl_num(total_bruto_dia)}\n"
                            f"Descontos: {_fmt_brl_num(total_desc)}\n"
                            f"Líquido: <b>{_fmt_brl_num(total_liq_dia)}</b>"
                            f"{resumo_lucro}"
                            f"{top_txt}"
                        )

            msg = (
                f"🧾 <b>Venda registrada</b>\n"
                f"{data_str}\n"
                f"Forma: <b>{st.session_state['forma']}</b>"
                f"{cliente_linha}\n"
                f"{'-'*24}\n"
                f"{itens_txt}\n"
                f"{'-'*24}\n"
                f"{'Desconto: ' + _fmt_brl_num(desconto) + '\\n' if desconto>0 else ''}"
                f"Total: <b>{_fmt_brl_num(total_cupom)}</b>"
                f"{lucro_bloco}"
                f"{fiado_msg}"
                f"{resumo_dia_txt}"
            )
            _tg_send(msg)

            st.success("Venda registrada!")

    if colB.button("🧹 Limpar carrinho", use_container_width=True):
        st.session_state["cart"] = []
        st.info("Carrinho limpo.")

st.divider()

# ================= Histórico de cupons =================
st.subheader("Histórico (últimos 10 cupons)")
try:
    vend = carregar_aba(ABA_VEND)
except Exception:
    vend = pd.DataFrame()

if vend.empty:
    st.info("Ainda não há vendas registradas.")
else:
    col_data  = _first_col(vend, ["Data"])
    col_idp   = _first_col(vend, ["IDProduto","ProdutoID","ID"])
    col_qtd   = _first_col(vend, ["Qtd","Quantidade","Qtde","Qde"])
    col_preco = _first_col(vend, ["PrecoUnit","PreçoUnitário","Preço","Preco"])
    col_venda = _first_col(vend, ["VendaID","Pedido","Cupom"])
    col_forma = _first_col(vend, ["FormaPagto","FormaPagamento","Pagamento","Forma"])

    vend["_Bruto"] = vend.apply(
        lambda r: _to_num(r.get("TotalLinha")) if "TotalLinha" in vend.columns
        else (_to_num(r.get(col_qtd))*_to_num(r.get(col_preco)) if col_qtd and col_preco else 0.0), axis=1
    )
    vend["_Desc"]  = vend["Desconto"].map(_to_num) if "Desconto" in vend.columns else 0.0
    vend["_TotalC"]= vend["TotalCupom"].map(_to_num) if "TotalCupom" in vend.columns else (vend["_Bruto"])

    grp = vend.groupby(col_venda, dropna=False).agg({
        col_data: "first",
        col_forma: "first",
        "_Bruto": "sum",
        "_Desc": "max",
        "_TotalC": "max",
        "Obs": "first"
    }).reset_index().rename(columns={col_venda:"VendaID", col_data:"Data", col_forma:"Forma"})

    try:
        grp["_ord"] = pd.to_datetime(grp["Data"], format="%d/%m/%Y", errors="coerce")
    except Exception:
        grp["_ord"] = pd.NaT
    grp = grp.sort_values(["_ord","VendaID"], ascending=[False, False]).head(10).reset_index(drop=True)

    for i, row in grp.iterrows():
        b1, b2, b3, b4, _ = st.columns([2.4, 1.2, 1.2, 1.5, 2.2])
        # UI amigável
        b1.write(f"**{row['Data']}**")
        b2.write(row["Forma"] if pd.notna(row["Forma"]) else "—")
        bruto = row["_Bruto"]; desc = row["_Desc"]; total = row["_TotalC"] if row["_TotalC"]>0 else (bruto - desc)
        b4.markdown(f"<div style='padding:4px 8px;border-radius:8px;background:#111;border:1px solid #333;display:inline-block'>{_fmt_brl_num(total)}</div>", unsafe_allow_html=True)
        cancelado = str(row.get("Obs","")).upper().startswith("ESTORNO DE") or str(row["VendaID"]).startswith("CN-")

        c1, c2, c3 = st.columns([0.9, 0.9, 4])

        def _carrega_carrinho(venda_id):
            linhas = vend[vend[col_venda]==venda_id].copy()
            cart = []
            for _, r in linhas.iterrows():
                cart.append({
                    "id": str(r.get("IDProduto") or r.get("ProdutoID") or r.get("ID")),
                    "nome": id_to_name.get(str(r.get("IDProduto") or r.get("ProdutoID") or r.get("ID")), ""),
                    "unid": "un",
                    "qtd": int(_to_num(r[col_qtd])) if col_qtd else 1,
                    "preco": float(_to_num(r[col_preco])) if col_preco else 0.0
                })
            st.session_state["prefill_cart"] = {
                "cart": cart,
                "forma": row["Forma"] if pd.notna(row["Forma"]) else "Dinheiro",
                "obs": "",
                "data": date.today(),
                "desc": float(row["_Desc"]) if pd.notna(row["_Desc"]) else 0.0
            }
            st.experimental_rerun()

        def _cancelar_cupom(venda_id):
            if str(venda_id).startswith("CN-"):
                st.warning("Esse cupom já é um estorno."); return
            if any(str(x).startswith(f"CN-{venda_id}") for x in vend[col_venda].unique()):
                st.warning("Estorno já registrado para esse cupom."); return

            linhas = vend[vend[col_venda]==venda_id].copy()
            if linhas.empty:
                st.warning("Cupom não encontrado."); return

            sh = conectar_sheets()
            ws = sh.worksheet(ABA_VEND)
            dfv2 = get_as_dataframe(ws, evaluate_formulas=False, dtype=str, header=0).dropna(how="all")
            dfv2.columns = [c.strip() for c in dfv2.columns]
            for c in ["Desconto","TotalCupom","CupomStatus","Cliente","FiadoID"]:
                if c not in dfv2.columns: dfv2[c] = None

            cn_id = f"CN-{venda_id}"
            data_str = date.today().strftime("%d/%m/%Y")
            novas = []
            total_estorno = 0.0
            for _, r in linhas.iterrows():
                pid = str(r.get("IDProduto") or r.get("ProdutoID") or r.get("ID"))
                qtd = -abs(_to_num(r[col_qtd])) if col_qtd else -1
                preco = _to_num(r[col_preco]) if col_preco else 0.0
                total_linha = qtd * preco
                total_estorno += total_linha
                novas.append({
                    "Data": data_str,
                    "VendaID": cn_id,
                    "IDProduto": pid,
                    "Qtd": str(int(qtd)),
                    "PrecoUnit": f"{preco:.2f}".replace(".", ","),
                    "TotalLinha": f"{total_linha:.2f}".replace(".", ","),
                    "FormaPagto": f"Estorno - {str(r.get('FormaPagto') or row['Forma'] or 'Dinheiro')}",
                    "Obs": f"ESTORNO DE {venda_id}",
                    "Desconto": "0,00",
                    "TotalCupom": "0,00",
                    "CupomStatus": "ESTORNO",
                    "Cliente": str(r.get("Cliente") or ""),
                    "FiadoID": ""
                })

            df_novo2 = pd.concat([dfv2, pd.DataFrame(novas)], ignore_index=True)
            ws.clear()
            set_with_dataframe(ws, df_novo2)
            st.cache_data.clear()
            st.session_state["_force_refresh"] = True

            # Movimentos: estorno = entrada
            try:
                ws_m = sh.worksheet(ABA_MOVS)
            except Exception:
                ws_m = _garantir_aba(sh, ABA_MOVS, ["Data","IDProduto","Produto","Tipo","Qtd","Obs","ID","Documento/NF","Origem","SaldoApós"])
            movs = []
            for _, r in linhas.iterrows():
                pid = str(r.get("IDProduto") or r.get("ProdutoID") or r.get("ID"))
                nome_prod = id_to_name.get(pid, pid)
                qtd = int(abs(_to_num(r[col_qtd]))) if col_qtd else 1
                # saldo após (aproximado): estoque atual + qtd
                bef = id_to_stock.get(pid, 0.0)
                aft = bef + qtd
                id_to_stock[pid] = aft
                movs.append({
                    "Data": data_str,
                    "IDProduto": pid,
                    "Produto": nome_prod,
                    "Tipo": "B entrada",
                    "Qtd": str(qtd),
                    "Obs": f"ESTORNO DE {venda_id}",
                    "ID": cn_id,
                    "Documento/NF": "",
                    "Origem": "Vendas rápidas",
                    "SaldoApós": str(int(aft))
                })
            _append_rows(ws_m, movs)

            # Telegram enxuto (sem IDs)
            itens_txt_estorno = "\n".join(f"• <b>{id_to_name.get(str(r.get('IDProduto') or r.get('ProdutoID') or r.get('ID')), 'Produto')}</b> — x{int(abs(_to_num(r[col_qtd])) if col_qtd else 1)}" for _, r in linhas.iterrows())
            cliente_est = ""
            if "Cliente" in linhas.columns and not linhas["Cliente"].dropna().empty:
                cliente_est = str(linhas["Cliente"].dropna().iloc[0])
            cliente_linha = f"\n👤 Cliente: <b>{cliente_est}</b>" if cliente_est else ""
            msg = (
                f"⛔ <b>Estorno lançado</b>\n"
                f"{data_str}\n"
                f"Valor estorno (linhas): <b>{_fmt_brl_num(abs(total_estorno))}</b>"
                f"{cliente_linha}\n"
                f"{'-'*24}\n"
                f"{itens_txt_estorno}"
            )
            _tg_send(msg)

            st.success("Estorno lançado.")

        c1.button("🔁 Duplicar", key=f"dup_{i}", on_click=_carrega_carrinho, args=(row["VendaID"],))
        c2.button("⛔ Cancelar", key=f"cn_{i}", disabled=cancelado, on_click=_cancelar_cupom, args=(row["VendaID"],))
        c3.caption(row.get("Obs","") if isinstance(row.get("Obs",""), str) else "")
        st.markdown("---")
