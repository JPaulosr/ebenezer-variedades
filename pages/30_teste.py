# pages/00_vendas.py — Vendas (carrinho + histórico/estorno/duplicar) com Telegram (recibo + resumo do dia)
# -*- coding: utf-8 -*-
import json, unicodedata
from datetime import datetime, date
import requests

import streamlit as st
import pandas as pd
import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from google.oauth2.service_account import Credentials

st.set_page_config(page_title="Vendas", page_icon="🧾", layout="wide")
st.title("🧾 Vendas (carrinho)")

# =========================================================
# Helper para rerun compatível (Streamlit >=1.27 e versões antigas)
# =========================================================
def _rerun():
    try:
        st.rerun()  # versões novas
    except Exception:
        try:
            st.experimental_rerun()  # fallback para versões antigas
        except Exception:
            pass

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

# ================= Telegram =================
def _tg_send_loja(texto, kb=None, parse_mode="Markdown"):
    """
    Envia mensagem para o canal da lojinha e retorna (ok, resp_text).
    Mostra erro detalhado em caso de falha (ex.: chat_id errado, bot sem permissão, markdown inválido).
    """
    token = st.secrets.get("TELEGRAM_TOKEN", "")
    chat_id = st.secrets.get("TELEGRAM_CHAT_ID_LOJINHA", "")
    if not token or not chat_id:
        st.error("🛑 TELEGRAM_TOKEN ou TELEGRAM_CHAT_ID_LOJINHA ausente nos secrets.")
        return False, "missing_secrets"

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,  # -100... (privado) ou @canal (público)
        "text": texto,
        "parse_mode": parse_mode,  # use None se der erro de markdown
        "disable_web_page_preview": True,
    }
    if kb:
        payload["reply_markup"] = json.dumps({"inline_keyboard": kb})

    try:
        r = requests.post(url, data=payload, timeout=12)
        if not r.ok:
            st.error(f"❌ Telegram falhou: {r.status_code} — {r.text}")
            return False, r.text
        return True, r.text
    except Exception as e:
        st.error(f"❌ Erro ao chamar Telegram: {e}")
        return False, str(e)

def _fmt_recibo_venda(venda_id: str, data_str: str, forma: str, itens: list[dict], desconto: float, total_liq: float) -> str:
    # itens: [{"nome": "...", "qtd": 2, "preco": 15.0}]
    linhas = []
    for it in itens:
        nome = it.get("nome") or "Item"
        linhas.append(f"• *{nome}* x{int(it['qtd'])} — R$ {float(it['preco']):.2f}".replace(".", ","))
    if not linhas:
        linhas = ["—"]
    corpo = "\n".join(linhas)
    return (
        f"*🧾 Venda registrada*\n"
        f"*Cupom:* `{venda_id}`\n"
        f"*Data:* {data_str}\n"
        f"*Forma:* {forma}\n"
        f"———\n"
        f"{corpo}\n"
        f"———\n"
        f"*Desconto:* R$ {float(desconto):.2f}\n"
        f"*Total líquido:* R$ {float(total_liq):.2f}"
    ).replace(".", ",")

def _resumo_do_dia_texto(data_str: str, vend_df: pd.DataFrame) -> str | None:
    """Checklist/resumo do dia (parcial) com base na aba Vendas."""
    if vend_df is None or vend_df.empty:
        return f"*Checklist do dia* ({data_str})\n— Sem cupons OK por enquanto."

    col_data  = _first_col(vend_df, ["Data"])
    col_qtd   = _first_col(vend_df, ["Qtd","Quantidade","Qtde","Qde"])
    col_preco = _first_col(vend_df, ["PrecoUnit","PreçoUnitário","Preço","Preco"])
    col_venda = _first_col(vend_df, ["VendaID","Pedido","Cupom"])
    if not (col_data and col_venda):
        return f"*Checklist do dia* ({data_str})\n— Estrutura da planilha incompleta."

    has_total = "TotalCupom" in vend_df.columns
    has_desc  = "Desconto"   in vend_df.columns
    has_stat  = "CupomStatus" in vend_df.columns

    df = vend_df.copy()
    # Bruto por linha
    if "TotalLinha" in df.columns:
        df["_Bruto"] = df["TotalLinha"].map(_to_num)
    else:
        df["_Bruto"] = df.apply(
            lambda r: _to_num(r.get(col_qtd))*_to_num(r.get(col_preco)) if col_qtd and col_preco else 0.0,
            axis=1
        )
    df["_Desc"]   = df["Desconto"].map(_to_num) if has_desc else 0.0
    df["_TotalC"] = df["TotalCupom"].map(_to_num) if has_total else df["_Bruto"]

    # somente o dia selecionado
    df = df[df[col_data] == data_str].copy()

    # exclui estornos
    if has_stat:
        df = df[~df["CupomStatus"].astype(str).str.upper().eq("ESTORNO")]
    df = df[~df[col_venda].astype(str).str.startswith("CN-")]
    df = df[~df.get("Obs", "").astype(str).str.upper().str.startswith("ESTORNO DE")]

    if df.empty:
        return f"*Checklist do dia* ({data_str})\n— Sem cupons OK por enquanto."

    # agrega por cupom
    grp = df.groupby(col_venda, dropna=False).agg({
        "_Bruto": "sum",
        "_Desc": "max",
        "_TotalC": "max"
    }).reset_index()

    cupons_ok = int(len(grp))
    bruto_tot = float(grp["_Bruto"].sum())
    desc_tot  = float(grp["_Desc"].sum())
    liq_tot   = float(grp["_TotalC"].sum())
    itens_dia = int(df[col_qtd].map(_to_num).sum()) if col_qtd else 0

    return (
        f"*Checklist do dia* ({data_str})\n"
        f"• Cupons OK: *{cupons_ok}*\n"
        f"• Itens vendidos: *{itens_dia}*\n"
        f"• Bruto: *R$ {bruto_tot:.2f}*\n"
        f"• Desconto: *R$ {desc_tot:.2f}*\n"
        f"• Líquido: *R$ {liq_tot:.2f}*"
    ).replace(".", ",")

# ================= Abas/colunas =================
ABA_PROD = "Produtos"
ABA_VEND = "Vendas"

# ================= Catálogo =================
try:
    dfp = carregar_aba(ABA_PROD)
except Exception as e:
    st.error("Erro ao abrir a aba Produtos.")
    with st.expander("Detalhes técnicos"): st.code(str(e))
    st.stop()

col_id   = _first_col(dfp, ["ID","Codigo","Código","SKU","IDProduto"])
col_nome = _first_col(dfp, ["Nome","Produto","Descrição","Descricao"])
col_preco= _first_col(dfp, ["PreçoVenda","PrecoVenda","Preço","Preco","PrecoUnit"])
col_unid = _first_col(dfp, ["Unidade","Und"])
if not col_id or not col_nome:
    st.error("A aba Produtos precisa ter colunas de ID e Nome."); st.stop()

dfp["_label"] = dfp.apply(lambda r: f"{str(r[col_id])} — {str(r[col_nome])}", axis=1)
cat_map = dfp.set_index("_label")[[col_id, col_nome, col_preco, col_unid]].to_dict("index")
labels = ["(selecione)"] + sorted(cat_map.keys())

# ================= Estado inicial =================
if "cart" not in st.session_state: st.session_state["cart"] = []
if "forma" not in st.session_state: st.session_state["forma"] = "Dinheiro"
if "obs" not in st.session_state:   st.session_state["obs"] = ""
if "data_venda" not in st.session_state: st.session_state["data_venda"] = date.today()
if "desc" not in st.session_state:  st.session_state["desc"] = 0.0

# ================= Prefill (duplicar cupom) =================
if "prefill_cart" in st.session_state:
    st.session_state["cart"]  = st.session_state["prefill_cart"].get("cart", [])
    st.session_state["forma"] = st.session_state["prefill_cart"].get("forma", "Dinheiro")
    st.session_state["obs"]   = st.session_state["prefill_cart"].get("obs", "")
    st.session_state["data_venda"] = st.session_state["prefill_cart"].get("data", date.today())
    st.session_state["desc"]  = float(st.session_state["prefill_cart"].get("desc", 0.0))
    st.session_state.pop("prefill_cart")

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

        # ---- Qtd (clamp >= 1) ----
        with c3:
            q_val = int(_to_num(it.get("qtd", 1)))
            if q_val < 1:
                q_val = 1
            st.session_state["cart"][idx]["qtd"] = st.number_input(
                "Qtd", key=f"q_{idx}", min_value=1, step=1, value=q_val
            )

        # ---- Preço (clamp >= 0) ----
        with c4:
            p_val = float(_to_num(it.get("preco", 0.0)))
            if p_val < 0:
                p_val = 0.0
            st.session_state["cart"][idx]["preco"] = st.number_input(
                "Preço (R$)", key=f"p_{idx}", min_value=0.0, step=0.1, value=p_val, format="%.2f"
            )

        c5.write(_fmt_brl_num(st.session_state['cart'][idx]['qtd']*st.session_state['cart'][idx]['preco']))
        if c6.button("🗑️", key=f"rm_{idx}"):
            st.session_state["cart"].pop(idx)
            _rerun()

    st.markdown("---")
    total_itens = sum(i["qtd"] for i in st.session_state["cart"])
    total_bruto = sum(i["qtd"]*i["preco"] for i in st.session_state["cart"])

    cL, cR = st.columns([2, 1.2])
    with cL:
        formas = ["Dinheiro","Pix","Cartão Débito","Cartão Crédito","Outros"]
        idx_forma = formas.index(st.session_state["forma"]) if st.session_state["forma"] in formas else 0
        st.session_state["forma"] = st.selectbox("Forma de pagamento", formas, index=idx_forma)
        st.session_state["obs"]   = st.text_input("Observações (opcional)", value=st.session_state["obs"])
    with cR:
        st.session_state["desc"]  = st.number_input("Desconto (R$)", min_value=0.0, value=float(st.session_state["desc"]), step=0.5, format="%.2f")
        total_liq = max(0.0, total_bruto - float(st.session_state["desc"]))
        st.metric("Total itens", total_itens)
        st.metric("Total bruto", _fmt_brl_num(total_bruto))
        st.metric("Total líquido", _fmt_brl_num(total_liq))

    # Botão opcional de teste de envio ao Telegram
    if st.button("📨 Testar Telegram (lojinha)", use_container_width=True):
        ok_test, resp_test = _tg_send_loja("Teste de envio ✅")
        if ok_test:
            st.success("Teste enviado. Verifique o canal.")
        else:
            st.warning("Não foi possível enviar o teste. Veja o erro acima.")

    colA, colB = st.columns([1, 1])
    if colA.button("🧾 Registrar venda", type="primary", use_container_width=True):
        if not st.session_state["cart"]:
            st.warning("Carrinho vazio.")
        else:
            # abre/cria Vendas
            sh = conectar_sheets()
            try:
                ws = sh.worksheet(ABA_VEND)
            except Exception:
                ws = sh.add_worksheet(title=ABA_VEND, rows=2000, cols=12)
                ws.update("A1:K1", [["Data","VendaID","IDProduto","Qtd","PrecoUnit","TotalLinha","FormaPagto","Obs","Desconto","TotalCupom","CupomStatus"]])

            dfv = get_as_dataframe(ws, evaluate_formulas=False, dtype=str, header=0).dropna(how="all")
            if dfv.empty:
                dfv = pd.DataFrame(columns=["Data","VendaID","IDProduto","Qtd","PrecoUnit","TotalLinha","FormaPagto","Obs","Desconto","TotalCupom","CupomStatus"])
            dfv.columns = [c.strip() for c in dfv.columns]

            # garante colunas novas se a aba for antiga
            for c in ["Desconto","TotalCupom","CupomStatus"]:
                if c not in dfv.columns: dfv[c] = None

            venda_id = "V-" + datetime.now().strftime("%Y%m%d%H%M%S")
            data_str = st.session_state["data_venda"].strftime("%d/%m/%Y")
            desconto = float(st.session_state["desc"])
            total_cupom = max(0.0, total_bruto - desconto)

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
                    "CupomStatus": "OK"
                })

            df_novo = pd.concat([dfv, pd.DataFrame(novas)], ignore_index=True)
            ws.clear()
            set_with_dataframe(ws, df_novo)

            # ======= TELEGRAM: RECIBO DA VENDA =======
            itens_tg = []
            try:
                cat_by_id = {str(r[col_id]): str(r[col_nome]) for _, r in dfp.iterrows()}
            except Exception:
                cat_by_id = {}
            for it in st.session_state["cart"]:
                nome = it.get("nome") or cat_by_id.get(str(it["id"])) or f"ID {it['id']}"
                itens_tg.append({"nome": nome, "qtd": int(it["qtd"]), "preco": float(it["preco"])})

            msg_recibo = _fmt_recibo_venda(
                venda_id=venda_id,
                data_str=data_str,
                forma=st.session_state["forma"],
                itens=itens_tg,
                desconto=desconto,
                total_liq=total_cupom
            )

            kb = []
            plan_url = st.secrets.get("PLANILHA_URL", "")
            app_url  = st.secrets.get("APP_URL", "")
            row = []
            if plan_url: row.append({"text": "📄 Planilha", "url": plan_url})
            if app_url:  row.append({"text": "📲 App", "url": app_url})
            if row: kb.append(row)

            ok1, resp1 = _tg_send_loja(msg_recibo, kb)

            # ======= TELEGRAM: RESUMO DO DIA (PARCIAL) =======
            try:
                vend_full = carregar_aba(ABA_VEND)  # recarrega para refletir a venda gravada
                msg_resumo = _resumo_do_dia_texto(data_str, vend_full)
                ok2, resp2 = _tg_send_loja(msg_resumo)
            except Exception as e:
                st.warning(f"[Resumo do dia] falhou ao montar/enviar: {e}")

            # limpa carrinho e força refresh nas outras páginas
            st.session_state["cart"] = []
            st.cache_data.clear()
            st.session_state["_force_refresh"] = True
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

    vend["_Bruto"] = vend.apply(
        lambda r: _to_num(r.get("TotalLinha")) if "TotalLinha" in vend.columns else (
            _to_num(r.get(col_qtd))*_to_num(r.get(col_preco)) if col_qtd and col_preco else 0.0
        ),
        axis=1
    )
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
        b4.write(_fmt_brl_num(total))
        cancelado = str(row.get("Obs","")).upper().startswith("ESTORNO DE") or str(row["VendaID"]).startswith("CN-")

        c1, c2, c3 = st.columns([0.9, 0.9, 4])

        # ---------- Duplicar ----------
        def _carrega_carrinho(venda_id):
            linhas = vend[vend[col_venda]==venda_id].copy()
            cart = []
            for _, r in linhas.iterrows():
                q_raw = int(_to_num(r[col_qtd])) if col_qtd else 1
                q = abs(q_raw) or 1  # evita 0 e negativo
                p = float(_to_num(r[col_preco])) if col_preco else 0.0
                if p < 0: p = 0.0
                if q == 0: continue
                cart.append({
                    "id": str(r.get("IDProduto") or r.get("ProdutoID") or r.get("ID")),
                    "nome": "",  # opcional
                    "unid": "un",
                    "qtd": q,
                    "preco": p
                })
            st.session_state["prefill_cart"] = {
                "cart": cart,
                "forma": row["Forma"] if pd.notna(row["Forma"]) else "Dinheiro",
                "obs": "",
                "data": date.today(),
                "desc": float(row["_Desc"]) if pd.notna(row["_Desc"]) else 0.0
            }
            _rerun()

        # ---------- Estornar ----------
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
            for c in ["Desconto","TotalCupom","CupomStatus"]:
                if c not in dfv.columns: dfv[c] = None

            cn_id = f"CN-{venda_id}"
            data_str = date.today().strftime("%d/%m/%Y")
            novas = []
            for _, r in linhas.iterrows():
                qtd = -abs(_to_num(r[col_qtd])) if col_qtd else -1
                preco = _to_num(r[col_preco]) if col_preco else 0.0
                novas.append({
                    "Data": data_str,
                    "VendaID": cn_id,
                    "IDProduto": str(r.get("IDProduto") or r.get("ProdutoID") or r.get("ID")),
                    "Qtd": str(int(qtd)),
                    "PrecoUnit": f"{preco:.2f}".replace(".", ","),
                    "TotalLinha": f"{qtd*preco:.2f}".replace(".", ","),
                    "FormaPagto": f"Estorno - {str(r.get('FormaPagto') or row['Forma'] or 'Dinheiro')}",
                    "Obs": f"ESTORNO DE {venda_id}",
                    "Desconto": "0,00",
                    "TotalCupom": "0,00",
                    "CupomStatus": "ESTORNO"
                })
            df_novo = pd.concat([dfv, pd.DataFrame(novas)], ignore_index=True)
            ws.clear()
            set_with_dataframe(ws, df_novo)
            st.cache_data.clear()
            st.session_state["_force_refresh"] = True
            st.success(f"Estorno lançado ({cn_id}).")

        c1.button("🔁 Duplicar", key=f"dup_{i}", on_click=_carrega_carrinho, args=(row["VendaID"],))
        c2.button("⛔ Cancelar", key=f"cn_{i}", disabled=cancelado, on_click=_cancelar_cupom, args=(row["VendaID"],))
        c3.caption(row.get("Obs","") if isinstance(row.get("Obs",""), str) else "")
        st.markdown("---")
