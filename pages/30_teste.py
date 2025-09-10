# pages/00_vendas.py — Vendas (carrinho + histórico/estorno/duplicar) com switch Telegram
# -*- coding: utf-8 -*-
import json, unicodedata
from datetime import datetime, date
import streamlit as st
import pandas as pd
import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from google.oauth2.service_account import Credentials
import requests  # Telegram

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
            st.experimental_rerun()  # fallback
        except Exception:
            pass

def _toast(msg: str, icon: str | None = None):
    try:
        st.toast(msg, icon=icon)
    except Exception:
        st.info(msg)

# ================= Helpers =================
def _normalize_private_key(key: str) -> str:
    if not isinstance(key, str): return key
    key = key.replace("\\n", "\n")
    key = "".join(ch for ch in key if unicodedata.category(ch)[0] != "C" or ch in ("\n", "\r", "\t"))
    return key

def _get_secret(*names, default=""):
    # pega o primeiro secret não vazio dentre os nomes informados
    for n in names:
        try:
            v = st.secrets.get(n, "")
            if isinstance(v, (dict, list)):  # nunca retornamos estruturas aqui
                continue
            v = (v or "").strip()
            if v:
                return v
        except Exception:
            continue
    return default

def _load_sa():
    svc = st.secrets.get("GCP_SERVICE_ACCOUNT")
    if svc is None:
        st.error("🛑 GCP_SERVICE_ACCOUNT ausente.")
        st.stop()
    if isinstance(svc, str):
        svc = json.loads(svc)
    svc = {**svc, "private_key": _normalize_private_key(svc["private_key"])}
    return svc

@st.cache_resource
def conectar_sheets():
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(_load_sa(), scopes=scopes)
    gc = gspread.authorize(creds)
    url_or_id = st.secrets.get("PLANILHA_URL", "")
    if not url_or_id:
        st.error("🛑 PLANILHA_URL ausente.")
        st.stop()
    return gc.open_by_url(url_or_id) if str(url_or_id).startswith("http") else gc.open_by_key(url_or_id)

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

def _fmt_brl2(v: float) -> str:
    try: v = float(v)
    except Exception: v = 0.0
    return f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X",".")

# ================= Abas/colunas =================
ABA_PROD = "Produtos"
ABA_VEND = "Vendas"

# ==================== TELEGRAM (lojinha) ====================
def _tg_token() -> str | None:
    return _get_secret("TELEGRAM_TOKEN", "TELEGRAM_BOT_TOKEN") or None

def _tg_chat() -> str | None:
    return _get_secret("TELEGRAM_CHAT_ID_LOJINHA", "TELEGRAM_CHAT_ID", "TELEGRAM_CHAT_ID_JP") or None

def _tg_enabled_default() -> bool:
    # valor padrão vindo dos secrets (se ausente, considera "1")
    return (_get_secret("TELEGRAM_ENABLED", default="1") != "0")

@st.cache_data(ttl=30)
def _config_get(key: str, default: str | None = None) -> str | None:
    """
    Lê a aba 'Config' (colunas: Chave, Valor) e retorna o valor da chave.
    Se não existir, retorna default. Cache leve de 30s.
    """
    try:
        ws = conectar_sheets().worksheet("Config")
    except Exception:
        return default
    df = get_as_dataframe(ws, evaluate_formulas=False, dtype=str, header=0).dropna(how="all")
    if df.empty:
        return default
    df.columns = [c.strip() for c in df.columns]
    if not {"Chave","Valor"}.issubset(df.columns):
        return default
    row = df[df["Chave"].astype(str).str.strip() == key]
    if row.empty:
        return default
    return str(row["Valor"].iloc[0]).strip()

def _tg_enabled() -> bool:
    """
    Ordem de precedência:
    1) st.session_state["TG_SEND_OVERRIDE"]  (switch da sessão)
    2) aba Config -> TELEGRAM_ENABLED        (persistido na planilha)
    3) secrets -> TELEGRAM_ENABLED           (padrão)
    """
    if "TG_SEND_OVERRIDE" in st.session_state:
        return bool(st.session_state["TG_SEND_OVERRIDE"])
    cfg = _config_get("TELEGRAM_ENABLED", None)
    if cfg is not None:
        return cfg != "0"
    return _tg_enabled_default()

def _tg_ready() -> bool:
    return bool(_tg_enabled() and (_tg_token() or "").strip() and (_tg_chat() or "").strip())

def tg_send_loja(text: str) -> tuple[bool, str]:
    """Envia texto para o canal da lojinha. Retorna (ok, erro_str)."""
    if not _tg_enabled():
        return (False, "Envio desativado na UI/Config (TELEGRAM_ENABLED=0).")
    token, chat_id = _tg_token(), _tg_chat()
    if not token or not chat_id:
        return (False, "🛑 TELEGRAM_TOKEN/TELEGRAM_BOT_TOKEN ou TELEGRAM_CHAT_ID(_LOJINHA/_JP) ausente nos secrets.")
    try:
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
        r = requests.post(url, json=payload, timeout=30)
        js = {}
        try: js = r.json()
        except Exception: pass
        if r.ok and js.get("ok"):
            return (True, "")
        return (False, f"Telegram erro: HTTP {r.status_code} • {js}")
    except Exception as e:
        return (False, f"Exceção Telegram: {e}")

def make_card_caption_venda(venda_id: str, data_str: str, forma: str,
                            itens: list[dict], desconto: float, total_bruto: float, total_liq: float) -> str:
    linhas = [
        "🧾 <b>Venda registrada</b>",
        f"🗓️ Data: <b>{data_str}</b>",
        f"🆔 Cupom: <code>{venda_id}</code>",
        f"💳 Pagamento: <b>{forma}</b>",
        "",
        "📦 <b>Itens</b>",
    ]
    if itens:
        for it in itens:
            nome = (it.get("nome") or it.get("id") or "-")
            qtd  = int(it.get("qtd", 0) or 0)
            pu   = float(it.get("preco", 0) or 0.0)
            linhas.append(f"• {nome} — {qtd} × {_fmt_brl2(pu)} = <b>{_fmt_brl2(qtd*pu)}</b>")
    else:
        linhas.append("—")
    linhas += [
        "",
        "📊 <b>Totais</b>",
        f"Bruto: <b>{_fmt_brl2(total_bruto)}</b>",
    ]
    if float(desconto or 0) > 0:
        linhas.append(f"Desconto: <b>{_fmt_brl2(desconto)}</b>")
    linhas.append(f"Líquido: <b>{_fmt_brl2(total_liq)}</b>")
    return "\n".join(linhas)

def make_resumo_do_dia_vendas(df_vend: pd.DataFrame, data_str: str) -> str:
    if df_vend is None or df_vend.empty:
        return f"📊 <b>Resumo do Dia (Lojinha)</b>\n🗓️ {data_str}\n—\nSem vendas."
    d = df_vend.copy()
    d["Data"] = d["Data"].astype(str).str.strip()
    d = d[d["Data"] == data_str].copy()
    if d.empty:
        return f"📊 <b>Resumo do Dia (Lojinha)</b>\n🗓️ {data_str}\n—\nSem vendas."
    d["_Qtd"]   = pd.to_numeric(d.get("Qtd", 0), errors="coerce").fillna(0).astype(int)
    d["_Preco"] = pd.to_numeric(d.get("PrecoUnit", "0").astype(str).str.replace(",", "."), errors="coerce").fillna(0.0)
    d["_Linha"] = d["_Qtd"] * d["_Preco"]
    agg = d.groupby("VendaID", dropna=False).agg({
        "_Linha":"sum",
        "Desconto": lambda x: str(x.dropna().iloc[0]) if "Desconto" in d.columns else None,
        "TotalCupom": lambda x: str(x.dropna().iloc[0]) if "TotalCupom" in d.columns else None,
        "FormaPagto": lambda x: str(x.dropna().iloc[0]) if "FormaPagto" in d.columns else None
    }).reset_index()

    def _num_pt(s):
        try:
            ss = str(s).strip()
            if not ss: return 0.0
            ss = ss.replace(".", "").replace(",", ".")
            return float(ss)
        except: return 0.0

    agg["_Desc"]   = agg["Desconto"].map(_num_pt) if "Desconto" in agg.columns else 0.0
    agg["_TotalC"] = agg["TotalCupom"].map(_num_pt) if "TotalCupom" in agg.columns else 0.0

    bruto_dia   = float(agg["_Linha"].sum())
    desc_dia    = float(pd.to_numeric(agg["_Desc"], errors="coerce").fillna(0).sum())
    liquido_dia = float(pd.to_numeric(agg["_TotalC"].replace(0, pd.NA), errors="coerce").fillna((agg["_Linha"]-agg["_Desc"])).sum())

    if "FormaPagto" in agg.columns:
        fser = agg["FormaPagto"].fillna("").astype(str).str.strip().replace("", pd.NA).dropna()
        formas_str = " • ".join(f"{k}: {v}" for k, v in fser.value_counts().to_dict().items()) if not fser.empty else "—"
    else:
        formas_str = "—"

    linhas = [
        "📊 <b>Resumo do Dia (Lojinha)</b>",
        f"🗓️ {data_str}",
        "—",
        f"🧾 Cupons: <b>{len(agg)}</b>",
        f"📦 Itens (linhas): <b>{int(d['_Qtd'].sum())}</b>",
        f"💵 Bruto: <b>{_fmt_brl2(bruto_dia)}</b>",
        f"🏷️ Descontos: <b>{_fmt_brl2(desc_dia)}</b>",
        f"🪙 Líquido: <b>{_fmt_brl2(liquido_dia)}</b>",
        f"💳 Formas: {formas_str}",
    ]
    return "\n".join(linhas)

# ======= Switch de Envio ao Telegram (sessão + opção de persistir) =======
with st.container():
    colA, colB, colC = st.columns([2,1,1])
    with colA:
        on_ui = st.toggle("📲 Enviar mensagens ao Telegram (sessão)", value=_tg_enabled(),
                          help="Muda apenas nesta sessão. Use 'Salvar como padrão' para persistir.")
    with colB:
        save_default = st.checkbox("Salvar como padrão", value=False,
                                   help="Salva o estado atual na aba 'Config' da planilha (chave TELEGRAM_ENABLED).")
    with colC:
        if st.button("Aplicar", use_container_width=True):
            st.session_state["TG_SEND_OVERRIDE"] = bool(on_ui)
            _toast("Preferência de Telegram (sessão) atualizada.")
            if save_default:
                # grava na aba Config
                sh = conectar_sheets()
                try:
                    ws = sh.worksheet("Config")
                except Exception:
                    ws = sh.add_worksheet(title="Config", rows=50, cols=2)
                    ws.update("A1:B1", [["Chave","Valor"]])
                df_cfg = get_as_dataframe(ws, evaluate_formulas=False, dtype=str, header=0).dropna(how="all")
                if df_cfg.empty:
                    df_cfg = pd.DataFrame(columns=["Chave","Valor"])
                df_cfg.columns = [c.strip() for c in df_cfg.columns]
                df_cfg = df_cfg[["Chave","Valor"]] if {"Chave","Valor"}.issubset(df_cfg.columns) else pd.DataFrame(columns=["Chave","Valor"])

                key = "TELEGRAM_ENABLED"
                val = "1" if on_ui else "0"
                exists = df_cfg["Chave"].astype(str).str.strip().eq(key)
                if exists.any():
                    df_cfg.loc[exists, "Valor"] = val
                else:
                    df_cfg = pd.concat([df_cfg, pd.DataFrame([{"Chave": key, "Valor": val}])], ignore_index=True)

                ws.clear()
                set_with_dataframe(ws, df_cfg)
                st.cache_data.clear()
                _toast("Padrão salvo em Config (TELEGRAM_ENABLED).")
            _rerun()

# Badge de status (discreto)
st.markdown("✅ **Telegram ON** — mensagens serão enviadas." if _tg_ready()
            else "📵 **Telegram OFF** — mensagens não serão enviadas.", unsafe_allow_html=True)

# ===== Debug Telegram =====
with st.expander("🔧 Debug Telegram (lojinha) — remova depois"):
    def _mask(s, keep=6):
        if not s: return "—"
        s = str(s); return s[:keep] + "…" if len(s) > keep else s
    tok, cid = _tg_token(), _tg_chat()
    st.write("TOKEN presente?", bool(tok), _mask(tok or ""))
    st.write("CHAT_ID (lojinha/jp) presente?", bool(cid), _mask(cid or ""))
    try:
        st.write("TELEGRAM_ENABLED (secrets):", _get_secret("TELEGRAM_ENABLED", default="1"))
    except Exception:
        pass
    if st.button("📨 Testar Telegram (lojinha)"):
        ok, err = tg_send_loja("✅ Teste lojinha (Streamlit).")
        st.success("Mensagem enviada!") if ok else st.error(err or "Falhou")

st.divider()

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

    colA, colB = st.columns([1, 1])

    # ========= registrar venda =========
    if colA.button("🧾 Registrar venda", type="primary", use_container_width=True):
        if not st.session_state["cart"]:
            st.warning("Carrinho vazio.")
        else:
            # Mantém uma cópia do carrinho para o card do Telegram
            cart_backup = list(st.session_state["cart"])
            st.session_state["_last_cart"] = cart_backup

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

            # limpa cache e força refresh
            st.cache_data.clear()
            st.session_state["_force_refresh"] = True

            # ========= Envio Telegram (card + resumo do dia) =========
            msg_mostrada = False
            try:
                if _tg_ready():
                    # Card da venda
                    card_txt = make_card_caption_venda(
                        venda_id=venda_id,
                        data_str=data_str,
                        forma=st.session_state["forma"],
                        itens=[{"id": it["id"], "nome": it.get("nome") or "", "qtd": int(it["qtd"]), "preco": float(it["preco"])} for it in cart_backup],
                        desconto=desconto,
                        total_bruto=total_bruto,
                        total_liq=total_cupom
                    )
                    ok1, err1 = tg_send_loja(card_txt)

                    # Resumo do dia (recarrega Vendas da planilha)
                    try:
                        vend_all = carregar_aba(ABA_VEND)
                    except Exception:
                        vend_all = pd.DataFrame()
                    resumo_txt = make_resumo_do_dia_vendas(vend_all, data_str)
                    ok2, err2 = tg_send_loja(resumo_txt)

                    if ok1 and ok2:
                        st.success(f"Venda registrada ({venda_id})! 📲 Card + resumo enviados.")
                    elif ok1 or ok2:
                        st.warning(f"Venda registrada ({venda_id}). Só um dos envios do Telegram funcionou.")
                        if not ok1 and err1: st.caption(f"Card: {err1}")
                        if not ok2 and err2: st.caption(f"Resumo: {err2}")
                    else:
                        st.warning(f"Venda registrada ({venda_id}), mas o Telegram falhou.")
                        if err1: st.caption(f"Card: {err1}")
                        if err2: st.caption(f"Resumo: {err2}")
                    msg_mostrada = True
                else:
                    st.info(f"Venda registrada ({venda_id}). Telegram não configurado ou desativado.")
                    msg_mostrada = True
            except Exception as e:
                st.warning(f"Venda registrada ({venda_id}), mas não consegui enviar ao Telegram: {e}")
                msg_mostrada = True

            # por último, limpa carrinho e avisa (sem duplicar sucesso)
            st.session_state["cart"] = []
            if not msg_mostrada:
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
    has_desc  = "Desconto"    in vend.columns
    has_total = "TotalCupom"  in vend.columns

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
                "desc": float(row["_Desc"]) if pd.notna(row["_Desc"]) else 0.0
            }
            _rerun()

        # ---------- Estornar ----------
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
