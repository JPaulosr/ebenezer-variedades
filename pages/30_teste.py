# pages/04_vendas_rapidas.py — Vendas rápidas (carrinho + histórico/estorno/duplicar)
# -*- coding: utf-8 -*-
import json, unicodedata
from datetime import datetime, date, timedelta
import streamlit as st
import pandas as pd
import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from google.oauth2.service_account import Credentials
import requests  # <<< Telegram

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

# -------------- Telegram helpers --------------
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
    """Envia mensagem para o Telegram se habilitado. Silencioso em erro."""
    if not _tg_enabled():
        return
    token, chat_id = _tg_conf()
    if not token or not chat_id:
        return
    try:
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {
            "chat_id": str(chat_id),
            "text": msg,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        requests.post(url, json=payload, timeout=6)
    except Exception:
        pass
# ---------------------------------------------

# ================= Abas principais =================
ABA_PROD, ABA_VEND = "Produtos", "Vendas"
ABA_FIADO = "Fiado"
ABA_CLIENTES = "Clientes"

COLS_FIADO = ["ID","Data","Cliente","Valor","Vencimento","Status","Obs","DataPagamento","FormaPagamento","ValorPago"]
COLS_CLIENTES = ["Cliente","Telefone","Obs"]

# ---- Clientes (cadastro) ----
def _garantir_aba_clientes(sh):
    return _garantir_aba(sh, ABA_CLIENTES, COLS_CLIENTES)

def _carregar_clientes() -> list[str]:
    try:
        dfc = carregar_aba(ABA_CLIENTES)
        col_cli = "Cliente" if "Cliente" in dfc.columns else (dfc.columns[0] if not dfc.empty else None)
        if not col_cli: return []
        lst = [str(x).strip() for x in dfc[col_cli].dropna().tolist() if str(x).strip()]
        return sorted(list(dict.fromkeys(lst)))
    except Exception:
        return []

# ================= Catálogo =================
try:
    dfp = carregar_aba(ABA_PROD)
except Exception as e:
    st.error("Erro ao abrir a aba Produtos.")
    with st.expander("Detalhes técnicos"): st.code(str(e))
    st.stop()

col_id   = _first_col(dfp, ["ID","Codigo","Código","SKU"])
col_nome = _first_col(dfp, ["Nome","Produto","Descrição"])
col_preco= _first_col(dfp, ["PreçoVenda","PrecoVenda","Preço","Preco"])
col_unid = _first_col(dfp, ["Unidade","Und"])
if not col_id or not col_nome:
    st.error("A aba Produtos precisa ter colunas de ID e Nome."); st.stop()

dfp["_label"] = dfp.apply(lambda r: f"{str(r[col_id])} — {str(r[col_nome])}", axis=1)
cat_map = dfp.set_index("_label")[[col_id, col_nome, col_preco, col_unid]].to_dict("index")
labels = ["(selecione)"] + sorted(cat_map.keys())

# ================= Prefill (duplicar cupom) =================
if "prefill_cart" in st.session_state:
    st.session_state["cart"]  = st.session_state["prefill_cart"].get("cart", [])
    st.session_state["forma"] = st.session_state["prefill_cart"].get("forma", "Dinheiro")
    st.session_state["obs"]   = st.session_state["prefill_cart"].get("obs", "")
    st.session_state["data_venda"] = st.session_state["prefill_cart"].get("data", date.today())
    st.session_state["desc"]  = float(st.session_state["prefill_cart"].get("desc", 0.0))
    st.session_state.pop("prefill_cart")

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
        c1, c2, c3, c4, c5, c6 = st.columns([1.8, 3, 1, 1.4, 1.6, 0.8])
        c1.write(it["id"])
        c2.write(it["nome"])
        with c3:
            st.session_state["cart"][idx]["qtd"] = st.number_input("Qtd", key=f"q_{idx}", min_value=1, step=1, value=int(it["qtd"]))
        with c4:
            st.session_state["cart"][idx]["preco"] = st.number_input("Preço (R$)", key=f"p_{idx}", min_value=0.0, step=0.1, value=float(it["preco"]), format="%.2f")
        c5.write(f"Subtotal: R$ {(st.session_state['cart'][idx]['qtd']*st.session_state['cart'][idx]['preco']):,.2f}".replace(",", "X").replace(".", ",").replace("X","."))
        if c6.button("🗑️", key=f"rm_{idx}"):
            st.session_state["cart"].pop(idx)
            st.experimental_rerun()

    st.markdown("---")
    total_itens = sum(i["qtd"] for i in st.session_state["cart"])
    total_bruto = sum(i["qtd"]*i["preco"] for i in st.session_state["cart"])

    cL, cR = st.columns([2, 1.2])
    with cL:
        # >>> Inclui "Fiado"
        formas = ["Dinheiro","Pix","Cartão Débito","Cartão Crédito","Fiado","Outros"]
        idx_forma = formas.index(st.session_state["forma"]) if st.session_state["forma"] in formas else 0
        st.session_state["forma"] = st.selectbox("Forma de pagamento", formas, index=idx_forma)

        if st.session_state["forma"] == "Fiado":
            # Clientes cadastrados
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
        st.metric("Total bruto", f"R$ {total_bruto:,.2f}".replace(",", "X").replace(".", ",").replace("X","."))
        st.metric("Total líquido", f"R$ {total_liq:,.2f}".replace(",", "X").replace(".", ",").replace("X","."))

    colA, colB = st.columns([1, 1])
    if colA.button("🧾 Registrar venda", type="primary", use_container_width=True):
        if not st.session_state["cart"]:
            st.warning("Carrinho vazio.")
        else:
            # valida cliente quando Fiado
            if st.session_state["forma"] == "Fiado" and not st.session_state["cliente"].strip():
                st.error("Informe o Cliente para registrar fiado."); st.stop()

            # abre/cria Vendas
            sh = conectar_sheets()
            try:
                ws = sh.worksheet(ABA_VEND)
            except Exception:
                ws = sh.add_worksheet(title=ABA_VEND, rows=2000, cols=14)
                ws.update("A1:K1", [["Data","VendaID","IDProduto","Qtd","PrecoUnit","TotalLinha","FormaPagto","Obs","Desconto","TotalCupom","CupomStatus"]])

            dfv = get_as_dataframe(ws, evaluate_formulas=False, dtype=str, header=0).dropna(how="all")
            if dfv.empty:
                dfv = pd.DataFrame(columns=["Data","VendaID","IDProduto","Qtd","PrecoUnit","TotalLinha","FormaPagto","Obs","Desconto","TotalCupom","CupomStatus"])
            dfv.columns = [c.strip() for c in dfv.columns]

            # garante colunas novas se a aba for antiga
            for c in ["Desconto","TotalCupom","CupomStatus","Cliente","FiadoID"]:
                if c not in dfv.columns: dfv[c] = None

            venda_id = "V-" + datetime.now().strftime("%Y%m%d%H%M%S")
            data_str = st.session_state["data_venda"].strftime("%d/%m/%Y")
            desconto = float(st.session_state["desc"])
            total_cupom = max(0.0, total_bruto - desconto)

            # ---- Se for FIADO, garante cadastro do cliente e cria o fiado
            fiado_id = ""
            fiado_msg = ""
            if st.session_state["forma"] == "Fiado":
                cli_nome = st.session_state.get("cliente","").strip()

                # cadastra cliente se não existir
                ws_cli = _garantir_aba(sh, ABA_CLIENTES, COLS_CLIENTES)
                try:
                    dfc = carregar_aba(ABA_CLIENTES)
                except Exception:
                    dfc = pd.DataFrame(columns=COLS_CLIENTES)
                ja_tem = False
                if not dfc.empty:
                    if "Cliente" in dfc.columns:
                        ja_tem = any(str(x).strip().lower() == cli_nome.lower() for x in dfc["Cliente"].dropna())
                    else:
                        ja_tem = any(str(x).strip().lower() == cli_nome.lower() for x in dfc[dfc.columns[0]].dropna())
                if cli_nome and not ja_tem:
                    _append_rows(ws_cli, [{"Cliente": cli_nome, "Telefone": "", "Obs": ""}])

                # cria fiado
                ws_fiado = _garantir_aba(sh, ABA_FIADO, COLS_FIADO)
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
                _append_rows(ws_fiado, [linha_fiado])
                fiado_msg = f"\n💳 <b>Fiado</b> criado: <code>{fiado_id}</code>\n👤 Cliente: <b>{cli_nome}</b>\n📅 Vencimento: {venc_str}"

            # monta as linhas da venda (já com Cliente e FiadoID quando aplicável)
            novas = []
            for it in st.session_state["cart"]:
                novas.append({
                    "Data": data_str,
                    "VendaID": venda_id,
                    "IDProduto": it["id"],
                    "Qtd": str(int(it["qtd"])),
                    "PrecoUnit": f"{float(it['preco']):.2f}".replace(".", ","),
                    "TotalLinha": f"{it['qtd']*it['preco']:.2f}".replace(".", ","),
                    "FormaPagto": st.session_state["forma"],
                    "Obs": st.session_state["obs"],
                    "Desconto": f"{desconto:.2f}".replace(".", ","),
                    "TotalCupom": f"{total_cupom:.2f}".replace(".", ","),
                    "CupomStatus": "OK",
                    "Cliente": st.session_state.get("cliente",""),
                    "FiadoID": fiado_id
                })

            # grava Vendas
            df_novo = pd.concat([dfv, pd.DataFrame(novas)], ignore_index=True)
            ws.clear()
            set_with_dataframe(ws, df_novo)

            # limpa carrinho e força refresh nas outras páginas
            st.session_state["cart"] = []
            st.cache_data.clear()
            st.session_state["_force_refresh"] = True

            # -------- Telegram: venda -----------
            itens_txt = "\n".join([f"• {x['id']} x{x['qtd']} @ {_fmt_brl_num(x['preco'])}" for x in novas])
            cliente_txt = st.session_state.get("cliente","").strip()
            cliente_linha = f"\n👤 Cliente: <b>{cliente_txt}</b>" if cliente_txt else ""
            msg = (
                f"🧾 <b>Venda registrada</b>\n"
                f"ID: <code>{venda_id}</code>\n"
                f"Data: {data_str}\n"
                f"Forma: {st.session_state['forma']}\n"
                f"{'Desconto: ' + _fmt_brl_num(desconto) + '\\n' if desconto>0 else ''}"
                f"Total: <b>{_fmt_brl_num(total_cupom)}</b>"
                f"{cliente_linha}\n"
                f"{'-'*18}\n"
                f"{itens_txt}"
                f"{fiado_msg}"
            )
            _tg_send(msg)
            # -----------------------------------

            if fiado_id:
                st.success(f"Venda registrada ({venda_id}) e Fiado criado/vinculado (ID {fiado_id}, total {_fmt_brl_num(total_cupom)}).")
            else:
                st.success(f"Venda registrada ({venda_id})!")

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
    # Detecta colunas
    col_data  = _first_col(vend, ["Data"])
    col_idp   = _first_col(vend, ["IDProduto","ProdutoID","ID"])
    col_qtd   = _first_col(vend, ["Qtd","Quantidade","Qtde","Qde"])
    col_preco = _first_col(vend, ["PrecoUnit","PreçoUnitário","Preço","Preco"])
    col_venda = _first_col(vend, ["VendaID","Pedido","Cupom"])
    col_forma = _first_col(vend, ["FormaPagto","FormaPagamento","Pagamento","Forma"])
    has_desc  = "Desconto"   in vend.columns
    has_total = "TotalCupom" in vend.columns
    has_stat  = "CupomStatus" in vend.columns

    vend["_Bruto"] = vend.apply(lambda r: _to_num(r.get("TotalLinha")) if "TotalLinha" in vend.columns else (_to_num(r.get(col_qtd))*_to_num(r.get(col_preco)) if col_qtd and col_preco else 0.0), axis=1)
    vend["_Desc"]  = vend["Desconto"].map(_to_num) if has_desc else 0.0
    vend["_TotalC"]= vend["TotalCupom"].map(_to_num) if has_total else (vend["_Bruto"])

    # agrega por VendaID
    grp = vend.groupby(col_venda, dropna=False).agg({
        col_data: "first",
        col_forma: "first",
        "_Bruto": "sum",
        "_Desc": "max",
        "_TotalC": "max",
        "Obs": "first"
    }).reset_index().rename(columns={col_venda:"VendaID", col_data:"Data", col_forma:"Forma"})
    # Ordena por data/venda (recente primeiro)
    try:
        grp["_ord"] = pd.to_datetime(grp["Data"], format="%d/%m/%Y", errors="coerce")
    except Exception:
        grp["_ord"] = pd.NaT
    grp = grp.sort_values(["_ord","VendaID"], ascending=[False, False]).head(10).reset_index(drop=True)

    # Tabela + ações
    for i, row in grp.iterrows():
        b1, b2, b3, b4, b5 = st.columns([1.5, 1.3, 1.2, 1.3, 2.2])
        b1.write(f"**{row['VendaID']}**")
        b2.write(row["Data"])
        b3.write(row["Forma"] if pd.notna(row["Forma"]) else "—")
        bruto = row["_Bruto"]; desc = row["_Desc"]; total = row["_TotalC"] if row["_TotalC"]>0 else (bruto - desc)
        b4.write(f"{_fmt_brl_num(total)}")
        cancelado = str(row.get("Obs","")).upper().startswith("ESTORNO DE") or str(row["VendaID"]).startswith("CN-")

        c1, c2, c3 = st.columns([0.9, 0.9, 4])
        def _carrega_carrinho(venda_id):
            linhas = vend[vend[col_venda]==venda_id].copy()
            cart = []
            for _, r in linhas.iterrows():
                cart.append({
                    "id": str(r.get("IDProduto") or r.get("ProdutoID") or r.get("ID")),
                    "nome": "",  # opcional
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
            # evita duplo estorno
            if any(str(x).startswith(f"CN-{venda_id}") for x in vend[col_venda].unique()):
                st.warning("Estorno já registrado para esse cupom."); return

            linhas = vend[vend[col_venda]==venda_id].copy()
            if linhas.empty:
                st.warning("Cupom não encontrado."); return

            sh = conectar_sheets()
            ws = sh.worksheet(ABA_VEND)
            dfv = get_as_dataframe(ws, evaluate_formulas=False, dtype=str, header=0).dropna(how="all")
            dfv.columns = [c.strip() for c in dfv.columns]
            for c in ["Desconto","TotalCupom","CupomStatus","Cliente","FiadoID"]:
                if c not in dfv.columns: dfv[c] = None

            cn_id = f"CN-{venda_id}"
            data_str = date.today().strftime("%d/%m/%Y")
            novas = []
            total_estorno = 0.0
            for _, r in linhas.iterrows():
                qtd = -abs(_to_num(r[col_qtd])) if col_qtd else -1
                preco = _to_num(r[col_preco]) if col_preco else 0.0
                total_linha = qtd*preco
                total_estorno += total_linha
                novas.append({
                    "Data": data_str,
                    "VendaID": cn_id,
                    "IDProduto": str(r.get("IDProduto") or r.get("ProdutoID") or r.get("ID")),
                    "Qtd": str(int(qtd)),
                    "PrecoUnit": f"{preco:.2f}".replace(".", ","),
                    "TotalLinha": f"{total_linha:.2f}".replace(".", ","),
                    "FormaPagto": f"Estorno - {str(r.get('FormaPagto') or row['Forma'] or 'Dinheiro')}",
                    "Obs": f"ESTORNO DE {venda_id}",
                    "Desconto": "0,00",
                    "TotalCupom": "0,00",
                    "CupomStatus": "ESTORNO",
                    "Cliente": str(r.get("Cliente") or ""),
                    "FiadoID": ""  # estorno não herda fiado
                })
            df_novo = pd.concat([dfv, pd.DataFrame(novas)], ignore_index=True)
            ws.clear()
            set_with_dataframe(ws, df_novo)
            st.cache_data.clear()
            st.session_state["_force_refresh"] = True

            # -------- Telegram: estorno ----------
            cliente_est = str(linhas.get("Cliente", [""])[0]) if "Cliente" in linhas.columns and not linhas.empty else ""
            cliente_linha = f"\n👤 Cliente: <b>{cliente_est}</b>" if cliente_est else ""
            msg = (
                f"⛔ <b>Estorno lançado</b>\n"
                f"ID: <code>{cn_id}</code>\n"
                f"De: <code>{venda_id}</code>\n"
                f"Data: {data_str}\n"
                f"Valor estorno (linhas): <b>{_fmt_brl_num(abs(total_estorno))}</b>"
                f"{cliente_linha}"
            )
            _tg_send(msg)
            # -------------------------------------

            st.success(f"Estorno lançado ({cn_id}).")

        c1.button("🔁 Duplicar", key=f"dup_{i}", on_click=_carrega_carrinho, args=(row["VendaID"],))
        c2.button("⛔ Cancelar", key=f"cn_{i}", disabled=cancelado, on_click=_cancelar_cupom, args=(row["VendaID"],))
        c3.caption(row.get("Obs","") if isinstance(row.get("Obs",""), str) else "")
        st.markdown("---")
