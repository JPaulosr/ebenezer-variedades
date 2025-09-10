# pages/00_vendas.py — Vendas (carrinho + histórico/estorno/duplicar) com _rerun, clamps e FIADO
# -*- coding: utf-8 -*-
import json, unicodedata
from datetime import datetime, date, timedelta
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

def _ensure_ws(sh, title: str, headers: list[str]):
    """Garante que a worksheet exista e tenha o cabeçalho fornecido."""
    try:
        ws = sh.worksheet(title)
    except Exception:
        ws = sh.add_worksheet(title=title, rows=2000, cols=max(10, len(headers)))
        ws.update(f"A1:{chr(64+len(headers))}1", [headers])
        return ws
    # ajusta cabeçalho se faltar coluna
    try:
        df_exist = get_as_dataframe(ws, evaluate_formulas=False, dtype=str, header=0)
    except Exception:
        df_exist = pd.DataFrame()
    df_exist = df_exist if df_exist is not None else pd.DataFrame()
    if df_exist.empty:
        set_with_dataframe(ws, pd.DataFrame(columns=headers))
    else:
        cols = [c.strip() for c in df_exist.columns]
        changed = False
        for h in headers:
            if h not in cols:
                cols.append(h); changed = True
        if changed:
            df_exist.columns = [c.strip() for c in df_exist.columns]
            for h in headers:
                if h not in df_exist.columns:
                    df_exist[h] = None
            ws.clear()
            set_with_dataframe(ws, df_exist)
    return ws

# ================= Abas/colunas =================
ABA_PROD  = "Produtos"
ABA_VEND  = "Vendas"
ABA_FIADO = "Fiados"  # nova aba de controle de fiado

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
# campos FIADO
if "cliente" not in st.session_state: st.session_state["cliente"] = ""
if "venc_fiado" not in st.session_state: st.session_state["venc_fiado"] = date.today() + timedelta(days=30)
if "entrada_fiado" not in st.session_state: st.session_state["entrada_fiado"] = 0.0

# ================= Prefill (duplicar cupom) =================
if "prefill_cart" in st.session_state:
    st.session_state["cart"]  = st.session_state["prefill_cart"].get("cart", [])
    st.session_state["forma"] = st.session_state["prefill_cart"].get("forma", "Dinheiro")
    st.session_state["obs"]   = st.session_state["prefill_cart"].get("obs", "")
    st.session_state["data_venda"] = st.session_state["prefill_cart"].get("data", date.today())
    st.session_state["desc"]  = float(st.session_state["prefill_cart"].get("desc", 0.0))
    # se duplicou um fiado, mantém cliente mas zera entrada (normalmente reabre venda à vista)
    st.session_state["cliente"] = st.session_state["prefill_cart"].get("cliente", "")
    st.session_state["venc_fiado"] = date.today() + timedelta(days=30)
    st.session_state["entrada_fiado"] = 0.0
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

    cL, cR = st.columns([2.2, 1.2])
    with cL:
        formas = ["Dinheiro","Pix","Cartão Débito","Cartão Crédito","Fiado","Outros"]
        idx_forma = formas.index(st.session_state["forma"]) if st.session_state["forma"] in formas else 0
        st.session_state["forma"] = st.selectbox("Forma de pagamento", formas, index=idx_forma)
        st.session_state["obs"]   = st.text_input("Observações (opcional)", value=st.session_state["obs"])

        # Campos extras para FIADO
        if st.session_state["forma"] == "Fiado":
            st.markdown("**Dados do Fiado**")
            cfa, cfb, cfc = st.columns([1.6, 1, 1])
            with cfa:
                st.session_state["cliente"] = st.text_input("Cliente (obrigatório)", value=st.session_state["cliente"])
            with cfb:
                st.session_state["venc_fiado"] = st.date_input("Vencimento", value=st.session_state["venc_fiado"])
            with cfc:
                st.session_state["entrada_fiado"] = st.number_input("Entrada (R$)", min_value=0.0,
                                                                    value=float(st.session_state["entrada_fiado"]),
                                                                    step=1.0, format="%.2f")
    with cR:
        st.session_state["desc"]  = st.number_input("Desconto (R$)", min_value=0.0, value=float(st.session_state["desc"]), step=0.5, format="%.2f")
        total_liq = max(0.0, total_bruto - float(st.session_state["desc"]))
        st.metric("Total itens", total_itens)
        st.metric("Total bruto", _fmt_brl_num(total_bruto))
        st.metric("Total líquido", _fmt_brl_num(total_liq))

    colA, colB = st.columns([1, 1])

    # ========= registrar venda =========
    if colA.button("🧾 Registrar venda", type="primary", use_container_width=True):
        if not st.session_state["cart"]:
            st.warning("Carrinho vazio.")
        else:
            # valida FIADO
            if st.session_state["forma"] == "Fiado" and not str(st.session_state["cliente"]).strip():
                st.error("Informe o **Cliente** para vendas no Fiado."); st.stop()

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

            # ==== Controle de FIADO ====
            if st.session_state["forma"] == "Fiado":
                ws_f = _ensure_ws(sh, ABA_FIADO, ["VendaID","Data","Cliente","Total","Entrada","Recebido","Saldo","Vencimento","Status","Obs"])
                df_f = get_as_dataframe(ws_f, evaluate_formulas=False, dtype=str, header=0).dropna(how="all")
                if df_f.empty:
                    df_f = pd.DataFrame(columns=["VendaID","Data","Cliente","Total","Entrada","Recebido","Saldo","Vencimento","Status","Obs"])
                df_f.columns = [c.strip() for c in df_f.columns]
                for c in ["Entrada","Recebido","Saldo","Total"]:
                    if c not in df_f.columns: df_f[c] = "0,00"

                total_num = float(total_cupom)
                entrada   = max(0.0, float(st.session_state.get("entrada_fiado", 0.0)))
                recebido  = entrada
                saldo     = max(0.0, total_num - recebido)
                status    = "ABERTO" if saldo > 0.0001 else "LIQUIDADO"

                # upsert por VendaID
                mask = df_f["VendaID"].astype(str).str.strip().eq(venda_id)
                row_new = {
                    "VendaID": venda_id,
                    "Data": data_str,
                    "Cliente": str(st.session_state["cliente"]).strip(),
                    "Total": f"{total_num:.2f}".replace(".", ","),
                    "Entrada": f"{entrada:.2f}".replace(".", ","),
                    "Recebido": f"{recebido:.2f}".replace(".", ","),
                    "Saldo": f"{saldo:.2f}".replace(".", ","),
                    "Vencimento": st.session_state["venc_fiado"].strftime("%d/%m/%Y"),
                    "Status": status,
                    "Obs": st.session_state["obs"]
                }
                if mask.any():
                    for k, v in row_new.items():
                        df_f.loc[mask, k] = v
                else:
                    df_f = pd.concat([df_f, pd.DataFrame([row_new])], ignore_index=True)

                ws_f.clear()
                set_with_dataframe(ws_f, df_f)

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

# Carrega Fiados (para status/saldo e receber)
try:
    fiados = carregar_aba(ABA_FIADO)
    fiados.columns = [c.strip() for c in fiados.columns]
except Exception:
    fiados = pd.DataFrame(columns=["VendaID","Data","Cliente","Total","Entrada","Recebido","Saldo","Vencimento","Status","Obs"])

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

    # Junte info de Fiados
    if not fiados.empty:
        fiados["_SaldoNum"] = fiados["Saldo"].map(_to_num)
        fiados_map_saldo  = dict(zip(fiados["VendaID"].astype(str), fiados["_SaldoNum"]))
        fiados_map_status = dict(zip(fiados["VendaID"].astype(str), fiados["Status"].astype(str)))
        fiados_map_cli    = dict(zip(fiados["VendaID"].astype(str), fiados["Cliente"].astype(str)))
        fiados_map_venc   = dict(zip(fiados["VendaID"].astype(str), fiados["Vencimento"].astype(str)))
    else:
        fiados_map_saldo = {}; fiados_map_status = {}; fiados_map_cli={}; fiados_map_venc={}

    # Ordena por data/venda (recente primeiro)
    try:
        grp["_ord"] = pd.to_datetime(grp["Data"], format="%d/%m/%Y", errors="coerce")
    except Exception:
        grp["_ord"] = pd.NaT
    grp = grp.sort_values(["_ord","VendaID"], ascending=[False, False]).head(10).reset_index(drop=True)

    # Helpers de recebimento
    def _receber_fiado(venda_id: str, valor: float):
        if valor <= 0:
            st.warning("Informe um valor maior que zero para receber."); return
        sh = conectar_sheets()
        ws_f = _ensure_ws(sh, ABA_FIADO, ["VendaID","Data","Cliente","Total","Entrada","Recebido","Saldo","Vencimento","Status","Obs"])
        df_f = get_as_dataframe(ws_f, evaluate_formulas=False, dtype=str, header=0).dropna(how="all")
        if df_f.empty:
            st.error("Aba Fiados vazia."); return
        df_f.columns = [c.strip() for c in df_f.columns]
        mask = df_f["VendaID"].astype(str).str.strip().eq(venda_id)
        if not mask.any():
            st.error("Venda não encontrada na aba Fiados."); return

        total    = _to_num(df_f.loc[mask, "Total"].iloc[0])
        recebido = _to_num(df_f.loc[mask, "Recebido"].iloc[0])
        saldo    = max(0.0, total - recebido)
        val = min(float(valor), saldo)
        novo_receb = recebido + val
        novo_saldo = max(0.0, total - novo_receb)
        novo_status= "LIQUIDADO" if novo_saldo <= 0.0001 else "ABERTO"

        df_f.loc[mask, "Recebido"] = f"{novo_receb:.2f}".replace(".", ",")
        df_f.loc[mask, "Saldo"]    = f"{novo_saldo:.2f}".replace(".", ",")
        df_f.loc[mask, "Status"]   = novo_status

        ws_f.clear()
        set_with_dataframe(ws_f, df_f)
        st.cache_data.clear()
        st.success(f"Recebido { _fmt_brl_num(val) } de {venda_id}. Saldo: { _fmt_brl_num(novo_saldo) }")
        _rerun()

    # Tabela + ações
    for i, row in grp.iterrows():
        venda_id = str(row["VendaID"])
        saldo_f  = fiados_map_saldo.get(venda_id, 0.0)
        status_f = fiados_map_status.get(venda_id, "")
        cliente_f= fiados_map_cli.get(venda_id, "")
        venc_f   = fiados_map_venc.get(venda_id, "")

        b1, b2, b3, b4, b5 = st.columns([1.6, 1.2, 1.6, 1.6, 2.8])
        b1.write(f"**{venda_id}**")
        b2.write(row["Data"])
        forma_txt = row["Forma"] if pd.notna(row["Forma"]) else "—"
        if status_f:
            forma_txt += f" · {status_f}" + (f" · {cliente_f}" if cliente_f else "")
        b3.write(forma_txt)
        bruto = row["_Bruto"]; desc = row["_Desc"]; total = row["_TotalC"] if row["_TotalC"]>0 else (bruto - desc)
        b4.write(_fmt_brl_num(total))
        if status_f:
            b5.write(f"Saldo: **{_fmt_brl_num(saldo_f)}**" + (f" · Venc.: {venc_f}" if venc_f else ""))

        cancelado = str(row.get("Obs","")).upper().startswith("ESTORNO DE") or venda_id.startswith("CN-")

        # Ações
        c1, c2, c3, c4, c5 = st.columns([0.9, 0.9, 1.2, 1.0, 3])
        # ---------- Duplicar ----------
        def _carrega_carrinho(venda_id_local):
            linhas = vend[vend[col_venda]==venda_id_local].copy()
            cart = []
            for _, r in linhas.iterrows():
                q_raw = int(_to_num(r[col_qtd])) if col_qtd else 1
                q = abs(q_raw) or 1
                p = float(_to_num(r[col_preco])) if col_preco else 0.0
                if p < 0: p = 0.0
                if q == 0: continue
                cart.append({
                    "id": str(r.get("IDProduto") or r.get("ProdutoID") or r.get("ID")),
                    "nome": "",
                    "unid": "un",
                    "qtd": q,
                    "preco": p
                })
            st.session_state["prefill_cart"] = {
                "cart": cart,
                "forma": row["Forma"] if pd.notna(row["Forma"]) else "Dinheiro",
                "obs": "",
                "data": date.today(),
                "desc": float(row["_Desc"]) if pd.notna(row["_Desc"]) else 0.0,
                "cliente": cliente_f
            }
            _rerun()

        # ---------- Estornar ----------
        def _cancelar_cupom(venda_id_local):
            if str(venda_id_local).startswith("CN-"):
                st.warning("Esse cupom já é um estorno."); return
            if any(str(x).startswith(f"CN-{venda_id_local}") for x in vend[col_venda].unique()):
                st.warning("Estorno já registrado para esse cupom."); return

            linhas = vend[vend[col_venda]==venda_id_local].copy()
            if linhas.empty:
                st.warning("Cupom não encontrado."); return

            sh = conectar_sheets()
            ws = sh.worksheet(ABA_VEND)
            dfv = get_as_dataframe(ws, evaluate_formulas=False, dtype=str, header=0).dropna(how="all")
            dfv.columns = [c.strip() for c in dfv.columns]
            for c in ["Desconto","TotalCupom","CupomStatus"]:
                if c not in dfv.columns: dfv[c] = None

            cn_id = f"CN-{venda_id_local}"
            data_str2 = date.today().strftime("%d/%m/%Y")
            novas = []
            for _, r in linhas.iterrows():
                qtd = -abs(_to_num(r[col_qtd])) if col_qtd else -1
                preco = _to_num(r[col_preco]) if col_preco else 0.0
                novas.append({
                    "Data": data_str2,
                    "VendaID": cn_id,
                    "IDProduto": str(r.get("IDProduto") or r.get("ProdutoID") or r.get("ID")),
                    "Qtd": str(int(qtd)),
                    "PrecoUnit": f"{preco:.2f}".replace(".", ","),
                    "TotalLinha": f"{qtd*preco:.2f}".replace(".", ","),
                    "FormaPagto": f"Estorno - {str(r.get('FormaPagto') or row['Forma'] or 'Dinheiro')}",
                    "Obs": f"ESTORNO DE {venda_id_local}",
                    "Desconto": "0,00",
                    "TotalCupom": "0,00",
                    "CupomStatus": "ESTORNO"
                })
            df_novo = pd.concat([dfv, pd.DataFrame(novas)], ignore_index=True)
            ws.clear()
            set_with_dataframe(ws, df_novo)

            # se for FIADO, cancela na aba Fiados
            try:
                sh = conectar_sheets()
                ws_f = _ensure_ws(sh, ABA_FIADO, ["VendaID","Data","Cliente","Total","Entrada","Recebido","Saldo","Vencimento","Status","Obs"])
                df_f = get_as_dataframe(ws_f, evaluate_formulas=False, dtype=str, header=0).dropna(how="all")
                df_f.columns = [c.strip() for c in df_f.columns]
                m2 = df_f["VendaID"].astype(str).str.strip().eq(venda_id_local)
                if m2.any():
                    df_f.loc[m2, "Saldo"]  = "0,00"
                    df_f.loc[m2, "Status"] = "CANCELADO"
                    ws_f.clear()
                    set_with_dataframe(ws_f, df_f)
            except Exception:
                pass

            st.cache_data.clear()
            st.session_state["_force_refresh"] = True
            st.success(f"Estorno lançado ({cn_id}).")

        c1.button("🔁 Duplicar", key=f"dup_{i}", on_click=_carrega_carrinho, args=(venda_id,))
        c2.button("⛔ Cancelar", key=f"cn_{i}", disabled=cancelado, on_click=_cancelar_cupom, args=(venda_id,))

        # ---------- Receber Fiado ----------
        if status_f and status_f.upper() in ("ABERTO","EM ABERTO") and saldo_f > 0:
            default_val = float(saldo_f)
            val = c3.number_input("Receber (R$)", key=f"rec_{i}", min_value=0.0, value=default_val, step=1.0, format="%.2f")
            if c4.button("💰 Receber", key=f"btnrec_{i}", use_container_width=True):
                _receber_fiado(venda_id, float(val))
        else:
            c3.write("")  # placeholders para alinhar
            c4.write("")
        c5.caption(row.get("Obs","") if isinstance(row.get("Obs",""), str) else "")
        st.markdown("---")
