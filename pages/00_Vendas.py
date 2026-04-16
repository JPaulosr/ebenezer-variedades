# pages/00_Vendas.py — Vendas rápidas (redesenhada)
# -*- coding: utf-8 -*-
from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Tuple

import pandas as pd
import requests
import streamlit as st
from utils.sheets import (
    sheet, carregar_aba, garantir_aba, append_rows,
    to_num, brl, safe_cost, first_col, fmt_num,
    norm_tipo_mov, calcular_estoque, tg_send, tg_media, gerar_id,
    ABA_PROD, ABA_VEND, ABA_COMP, ABA_MOVS, ABA_CLIEN, ABA_FIADO, ABA_FPAGT,
)
_to_num = to_num; _brl = brl; _first_col = first_col; conectar_sheets = sheet
_tg_send = tg_send; _tg_media = tg_media; _gerar_id = gerar_id


# ──────────────────────────────────────────────
#  CONFIG & TEMA
# ──────────────────────────────────────────────
import pathlib
_cfg = pathlib.Path(".streamlit"); _cfg.mkdir(exist_ok=True)
(_cfg / "config.toml").write_text('[theme]\nbase = "dark"\n')

st.set_page_config(page_title="Vendas", page_icon="🧾", layout="wide",
                   initial_sidebar_state="collapsed")

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Nunito:wght@400;600;700;800;900&family=DM+Sans:wght@300;400;500&display=swap');
html, body, [class*="css"] { font-family: 'DM Sans', sans-serif; }

.page-header {
    background: linear-gradient(135deg, #1a1a2e 0%, #16213e 60%, #0f3460 100%);
    border-radius: 20px; padding: 24px 32px; margin-bottom: 24px;
    display: flex; align-items: center; justify-content: space-between;
    box-shadow: 0 8px 32px rgba(15,52,96,0.25);
}
.page-header h1 { font-family:'Nunito',sans-serif; font-weight:900; font-size:1.7rem; color:#fff; margin:0; }
.page-header .sub { font-size:0.82rem; color:rgba(255,255,255,0.5); margin-top:4px; }
.header-badge {
    background:rgba(255,255,255,0.1); border:1px solid rgba(255,255,255,0.2);
    border-radius:50px; padding:8px 18px; color:#fff; font-size:0.82rem;
    font-weight:600; backdrop-filter:blur(10px);
}

/* Seção título */
.sec-titulo {
    font-family:'Nunito',sans-serif; font-weight:800; font-size:1.05rem;
    color:rgba(255,255,255,0.9); margin:24px 0 14px 0;
    display:flex; align-items:center; gap:8px;
}
.sec-titulo::after {
    content:''; flex:1; height:1px;
    background:linear-gradient(to right,rgba(255,255,255,0.15),transparent);
    margin-left:8px; border-radius:2px;
}

/* Card produto no selectbox */
.prod-preview {
    background:rgba(255,255,255,0.05); border:1px solid rgba(255,255,255,0.1);
    border-radius:18px; padding:16px 20px; margin-bottom:16px;
    display:flex; gap:18px; align-items:center;
}
.prod-preview img {
    width:80px; height:80px; border-radius:12px;
    object-fit:contain; background:rgba(255,255,255,0.06);
    border:1px solid rgba(255,255,255,0.08); flex-shrink:0;
}
.prod-preview-ph {
    width:80px; height:80px; border-radius:12px;
    background:rgba(255,255,255,0.06); border:1px solid rgba(255,255,255,0.08);
    display:flex; align-items:center; justify-content:center;
    font-size:2rem; flex-shrink:0;
}
.prod-preview-info .nome { font-family:'Nunito',sans-serif; font-weight:800; font-size:1rem; color:#fff; }
.prod-preview-info .preco { font-size:1.2rem; font-weight:700; color:#4ade80; margin-top:4px; }
.prod-preview-info .est { font-size:0.75rem; color:rgba(255,255,255,0.4); margin-top:2px; }
.est-ok  { color:#4ade80 !important; }
.est-low { color:#fbbf24 !important; }
.est-neg { color:#f87171 !important; }

/* Cards do carrinho */
.cart-card {
    background:rgba(255,255,255,0.05); border:1px solid rgba(255,255,255,0.09);
    border-radius:16px; padding:14px 16px; margin-bottom:10px;
    display:flex; gap:14px; align-items:center;
}
.cart-img { width:62px; height:62px; border-radius:10px; object-fit:contain;
    background:rgba(255,255,255,0.06); border:1px solid rgba(255,255,255,0.08); flex-shrink:0; }
.cart-img-ph { width:62px; height:62px; border-radius:10px; background:rgba(255,255,255,0.06);
    display:flex; align-items:center; justify-content:center; font-size:1.6rem; flex-shrink:0; }
.cart-nome { font-weight:700; font-size:0.9rem; color:#fff; }
.cart-sub  { font-size:0.75rem; color:rgba(255,255,255,0.4); margin-top:2px; }

/* Totalizador */
.total-box {
    background:linear-gradient(135deg,rgba(74,222,128,0.12),rgba(34,211,238,0.08));
    border:1px solid rgba(74,222,128,0.25); border-radius:18px; padding:20px 24px;
}
.total-label { font-size:0.75rem; color:rgba(255,255,255,0.5); text-transform:uppercase; letter-spacing:0.5px; }
.total-val   { font-family:'Nunito',sans-serif; font-size:2rem; font-weight:900; color:#4ade80; }
.total-sub   { font-size:0.8rem; color:rgba(255,255,255,0.4); margin-top:2px; }

/* Histórico */
.hist-card {
    background:rgba(255,255,255,0.04); border:1px solid rgba(255,255,255,0.08);
    border-radius:14px; padding:14px 18px; margin-bottom:10px;
}
.hist-card.estorno { border-color:rgba(248,113,113,0.3); background:rgba(248,113,113,0.05); }
.hist-data  { font-size:0.8rem; color:rgba(255,255,255,0.45); }
.hist-id    { font-family:'Nunito',sans-serif; font-size:0.85rem; font-weight:700; color:#fff; }
.hist-valor { font-family:'Nunito',sans-serif; font-size:1.1rem; font-weight:800; color:#4ade80; }
.hist-forma { display:inline-block; background:rgba(96,165,250,0.15); color:#60a5fa;
    border:1px solid rgba(96,165,250,0.3); border-radius:8px;
    padding:2px 10px; font-size:0.72rem; font-weight:700; }
.hist-forma.fiado { background:rgba(251,191,36,0.15); color:#fbbf24; border-color:rgba(251,191,36,0.3); }
.hist-forma.estorno-badge { background:rgba(248,113,113,0.15); color:#f87171; border-color:rgba(248,113,113,0.3); }

/* Forma pagamento chips */
.forma-chip { cursor:pointer; }

button[kind="primary"] { border-radius:12px !important; font-weight:700 !important; }
div[data-testid="stNumberInput"] input { border-radius:10px !important; }
</style>
""", unsafe_allow_html=True)


# ──────────────────────────────────────────────
#  HELPERS GOOGLE SHEETS
# ──────────────────────────────────────────────
# carregar_aba importado de utils.sheets

def _first_col(df, cands):
    if df is None or df.empty: return None
    for c in cands:
        if c in df.columns: return c
    low = {c.lower(): c for c in df.columns}
    for c in cands:
        if c.lower() in low: return low[c.lower()]
    return None

def _to_num(x):
    if x is None: return 0.0
    if isinstance(x, (int, float)): return float(x)
    s = str(x).strip()
    if s == "" or s.lower() in ("nan","none"): return 0.0
    s = s.replace(".","").replace(",",".") if s.count(",") == 1 and s.count(".") > 1 else s.replace(",",".")
    try: return float(s)
    except: return 0.0

def _brl(v): return f"R$ {float(v):,.2f}".replace(",","X").replace(".",",").replace("X",".")
def _gerar_id(p="F"): return f"{p}-{datetime.now().strftime('%Y%m%d%H%M%S%f')[:-3]}"

def _garantir_aba(sh, nome, cols):
    try: ws = sh.worksheet(nome)
    except:
        ws = sh.add_worksheet(title=nome, rows=3000, cols=max(10,len(cols)))
        ws.update("A1",[cols]); return ws
    hdrs = [h.strip() for h in (ws.row_values(1) or [])]
    falt = [c for c in cols if c not in hdrs]
    if falt: ws.update("A1",[hdrs+falt])
    return ws

def _append_rows(ws, rows):
    hdrs = [h.strip() for h in ws.row_values(1)]
    ws.append_rows([[d.get(h,"") for h in hdrs] for d in rows], value_input_option="USER_ENTERED")


# ──────────────────────────────────────────────
#  TELEGRAM
# ──────────────────────────────────────────────
def _tg_on(): 
    try: return str(st.secrets.get("TELEGRAM_ENABLED","0")) == "1"
    except: return False

def _tg_conf():
    token   = st.secrets.get("TELEGRAM_TOKEN","")
    chat_id = st.secrets.get("TELEGRAM_CHAT_ID_LOJINHA","") or st.secrets.get("TELEGRAM_CHAT_ID","")
    return str(token or ""), str(chat_id or "")

def _tg_send(msg):
    if not _tg_on(): return
    token, cid = _tg_conf()
    if not token or not cid: return
    try: requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
        json={"chat_id":cid,"text":msg,"parse_mode":"HTML","disable_web_page_preview":True}, timeout=8)
    except: pass

def _tg_media(media):
    if not _tg_on(): return
    token, cid = _tg_conf()
    if not token or not cid or not media: return
    try: requests.post(f"https://api.telegram.org/bot{token}/sendMediaGroup",
        json={"chat_id":cid,"media":media[:10]}, timeout=12)
    except: pass


# ──────────────────────────────────────────────
#  CLIENTES
# ──────────────────────────────────────────────
ABA_CLIENTES = "Clientes"

def _strip_acc(s):
    return "".join(ch for ch in _ud.normalize("NFD",str(s or "")) if _ud.category(ch) != "Mn")

def _norm_cli(n): return re.sub(r"\s+"," ",(n or "").strip()).title()
def _cli_key(n):  return re.sub(r"\s+"," ",_strip_acc(_norm_cli(n)).lower()).strip()

def _carregar_clientes():
    try:
        dfc = carregar_aba(ABA_CLIENTES)
        if dfc.empty: return []
        col = "Cliente" if "Cliente" in dfc.columns else dfc.columns[0]
        vistos = {}
        for raw in dfc[col].dropna().astype(str):
            n = _norm_cli(raw); k = _cli_key(n)
            if k and k not in vistos: vistos[k] = n
        return sorted(vistos.values())
    except: return []

def _ensure_cliente(nome):
    nome = _norm_cli(nome)
    if not nome: return
    sh = conectar_sheets()
    ws = _garantir_aba(sh, ABA_CLIENTES, ["Cliente","Telefone","Obs"])
    try: dfc = carregar_aba(ABA_CLIENTES)
    except: dfc = pd.DataFrame(columns=["Cliente","Telefone","Obs"])
    if not dfc.empty:
        col = "Cliente" if "Cliente" in dfc.columns else dfc.columns[0]
        if any(_cli_key(r) == _cli_key(nome) for r in dfc[col].dropna().astype(str)): return
    _append_rows(ws, [{"Cliente":nome,"Telefone":"","Obs":""}])


# ──────────────────────────────────────────────
#  CATÁLOGO + ESTOQUE
# ──────────────────────────────────────────────
ABA_PROD, ABA_VEND = "Produtos", "Vendas"
ABA_COMPRAS, ABA_AJUSTES, ABA_MOVS, ABA_FIADO = "Compras","Ajustes","MovimentosEstoque","Fiado"
COLS_FIADO = ["ID","Data","Cliente","Valor","Vencimento","Status","Obs","DataPagamento","FormaPagamento","ValorPago"]

def _build_catalogo():
    try: dfp = carregar_aba(ABA_PROD)
    except: st.error("Erro ao abrir aba Produtos."); st.stop()

    col_id    = _first_col(dfp, ["ID","Codigo","Código","SKU"])
    col_nome  = _first_col(dfp, ["Nome","Produto","Descrição"])
    col_preco = _first_col(dfp, ["PreçoVenda","PrecoVenda","Preço","Preco"])
    col_unid  = _first_col(dfp, ["Unidade","Und"])
    col_custo = _first_col(dfp, ["Custo","PreçoCusto","PrecoCusto","CustoUnit","CustoMedio","CustoAtual"])
    col_foto  = _first_col(dfp, ["Foto","Imagem","Image","Photo","FotoURL","ImagemURL"])
    col_cat   = _first_col(dfp, ["Categoria","categoria"])
    col_emin  = _first_col(dfp, ["EstoqueMin","Estoque Min","EstMinimo"])

    if not col_id or not col_nome: st.error("Aba Produtos precisa de ID e Nome."); st.stop()

    # Dedupe labels
    dfp["_label"] = dfp[col_nome].astype(str).str.strip()
    cnt: Dict[str,int] = {}
    def _dd(l):
        c = cnt.get(l,0); cnt[l] = c+1
        return l if c == 0 else f"{l} ({c+1})"
    dfp["_label"] = dfp["_label"].map(_dd)

    use_cols = [c for c in [col_id,col_nome,col_preco,col_unid,col_foto,col_cat,col_custo,col_emin] if c]
    cat_map = dfp.set_index("_label")[use_cols].to_dict("index")
    labels  = ["(selecione)"] + sorted(cat_map.keys(), key=str.lower)

    id_nome: Dict[str,str]  = {}
    id_custo: Dict[str,float] = {}
    id_img: Dict[str,str]   = {}
    id_emin: Dict[str,float] = {}

    for _, r in dfp.iterrows():
        pid = str(r[col_id]).strip()
        if not pid: continue
        id_nome[pid]  = str(r.get(col_nome,"") or "").strip()
        if col_custo: id_custo[pid] = _to_num(r.get(col_custo))
        if col_foto:  id_img[pid]   = str(r.get(col_foto,"") or "").strip()
        if col_emin:  id_emin[pid]  = _to_num(r.get(col_emin))

    # ── Estoque — lê MovimentosEstoque (mesma fonte que Contagem de Estoque) ──
    def _norm_tipo_mov(t: str) -> str:
        """Classifica o tipo de movimento igual ao 05_Contagem_Estoque."""
        import re as _re
        raw = str(t or ""); low = "".join(ch for ch in unicodedata.normalize("NFKD", raw.lower()) if unicodedata.category(ch) != "Mn")
        if "fracion" in low:
            return "entrada" if "+" in raw else "saida" if "-" in raw else "outro"
        lowc = _re.sub(r"[^a-z]","",low)
        if "contagem" in lowc or "inventario" in lowc: return "ajuste"
        if "entrada" in lowc or "compra" in lowc or "estorno" in lowc: return "entrada"
        if "saida"   in lowc or "venda"  in lowc or "baixa"   in lowc: return "saida"
        if "ajuste"  in lowc: return "ajuste"
        return "outro"

    id_stock: Dict[str,float] = {}
    try:
        dmov = carregar_aba(ABA_MOVS)
        c_pid  = _first_col(dmov, ["IDProduto","ProdutoID","ID"])
        c_qtd  = _first_col(dmov, ["Qtd","Quantidade"])
        c_tipo = _first_col(dmov, ["Tipo","tipo"])
        if c_pid and c_qtd and c_tipo:
            for _, r in dmov.iterrows():
                pid   = str(r.get(c_pid,"")).strip()
                if not pid: continue
                tipo  = _norm_tipo_mov(r.get(c_tipo,""))
                qtd   = _to_num(r.get(c_qtd))
                cur   = id_stock.get(pid, 0.0)
                if tipo == "entrada":
                    id_stock[pid] = cur + qtd
                elif tipo == "saida":
                    id_stock[pid] = cur - qtd
                elif tipo == "ajuste":
                    id_stock[pid] = cur + qtd
    except: pass

    return dfp, cat_map, labels, id_nome, id_custo, id_stock, col_id, col_nome, col_preco, col_unid, id_img, id_emin

dfp, cat_map, labels, id_nome, id_custo, id_stock, col_id, col_nome_col, col_preco_col, col_unid_col, id_img, id_emin = _build_catalogo()


# ──────────────────────────────────────────────
#  IMAGEM
# ──────────────────────────────────────────────
def _resolve_img(raw):
    if not isinstance(raw, str) or not raw.strip(): return None
    v = raw.strip()
    if v.lower().startswith(("http://","https://")):
        if "drive.google.com" in v:
            m = re.search(r"/file/d/([^/]+)/view", v) or re.search(r"[?&]id=([^&]+)", v)
            return f"https://drive.google.com/uc?export=view&id={m.group(1)}" if m else v
        return v
    m = re.fullmatch(r"[A-Za-z0-9_-]{20,}", v)
    return f"https://drive.google.com/uc?export=view&id={v}" if m else None


# ──────────────────────────────────────────────
#  SESSION STATE
# ──────────────────────────────────────────────
_ss = st.session_state
for k, d in [("cart",[]),("forma","Dinheiro"),("obs",""),("data_venda",date.today()),
              ("desc",0.0),("cliente",""),("venc_fiado",date.today()+timedelta(days=30))]:
    if k not in _ss: _ss[k] = d

def _rerun():
    st.rerun()


# ──────────────────────────────────────────────
#  HEADER
# ──────────────────────────────────────────────
n_cart = len(_ss["cart"])
total_cart = sum(i["qtd"]*i["preco"] for i in _ss["cart"])

st.markdown(f"""
<div class="page-header">
  <div>
    <h1>🧾 Vendas Rápidas</h1>
    <div class="sub">Ebenezér Variedades · {datetime.now().strftime("%d/%m/%Y")}</div>
  </div>
  <div class="header-badge">🛒 {n_cart} {"item" if n_cart==1 else "itens"} · {_brl(total_cart)}</div>
</div>
""", unsafe_allow_html=True)


# ──────────────────────────────────────────────
#  LAYOUT PRINCIPAL
# ──────────────────────────────────────────────
col_esq, col_dir = st.columns([1.15, 1], gap="large")


# ═══════════════════════════════════════════════
#  COLUNA ESQUERDA — Adicionar produto + Carrinho
# ═══════════════════════════════════════════════
with col_esq:
    st.markdown('<div class="sec-titulo">🔍 Adicionar produto</div>', unsafe_allow_html=True)

    # Data
    _ss["data_venda"] = st.date_input("Data da venda", value=_ss["data_venda"])

    # Selectbox de produto
    sel = st.selectbox("Produto", labels, index=0, label_visibility="collapsed",
                       placeholder="Buscar produto...")

    # ── Preview do produto selecionado ──
    if sel != "(selecione)":
        info  = cat_map[sel]
        pid_s = str(info[col_id])
        foto_raw = info.get("Foto") or info.get("Imagem") or info.get("FotoURL") or ""
        img_url  = _resolve_img(str(foto_raw or ""))
        est_s    = id_stock.get(pid_s, 0.0)
        emin_s   = id_emin.get(pid_s, 0.0)
        preco_s  = _to_num(info.get(col_preco_col)) if col_preco_col else 0.0

        est_class = "est-ok" if est_s > emin_s else ("est-low" if est_s > 0 else "est-neg")
        est_icon  = "✅" if est_s > emin_s else ("⚠️" if est_s > 0 else "❌")

        if img_url:
            foto_tag = f'<img src="{img_url}" onerror="this.style.display=\'none\'">'
        else:
            foto_tag = '<div class="prod-preview-ph">📦</div>'

        st.markdown(f"""
        <div class="prod-preview">
          {foto_tag}
          <div class="prod-preview-info">
            <div class="nome">{sel}</div>
            <div class="preco">{_brl(preco_s)}</div>
            <div class="est {est_class}">{est_icon} Estoque: {int(est_s) if float(est_s).is_integer() else est_s}</div>
          </div>
        </div>
        """, unsafe_allow_html=True)

        # Qtd + Preço + Botão
        c1, c2 = st.columns([1, 1])
        with c1:
            qtd_add = st.number_input("Quantidade", min_value=1, step=1, value=1, key=f"qtd_add_{pid_s}")
        with c2:
            preco_add = st.number_input("Preço unit. (R$)", min_value=0.0,
                                        value=float(preco_s), step=0.10, format="%.2f", key=f"preco_add_{pid_s}")

        if st.button("➕  Adicionar ao carrinho", type="primary", use_container_width=True):
            _ss["cart"].append({
                "id":    pid_s,
                "nome":  str(info.get(col_nome_col, sel)),
                "unid":  str(info.get(col_unid_col, "un") or "un"),
                "foto":  str(foto_raw or ""),
                "qtd":   int(qtd_add),
                "preco": float(preco_add),
            })
            st.success(f"✅ {sel} adicionado!")
            _rerun()
    else:
        st.markdown("""
        <div style="background:rgba(255,255,255,0.03);border:1px dashed rgba(255,255,255,0.12);
        border-radius:16px;padding:32px;text-align:center;color:rgba(255,255,255,0.3);font-size:0.9rem">
          👆 Selecione um produto acima
        </div>
        """, unsafe_allow_html=True)

    # ── Carrinho ──
    st.markdown('<div class="sec-titulo">🛒 Carrinho</div>', unsafe_allow_html=True)

    if not _ss["cart"]:
        st.markdown("""
        <div style="background:rgba(255,255,255,0.03);border:1px dashed rgba(255,255,255,0.1);
        border-radius:14px;padding:28px;text-align:center;color:rgba(255,255,255,0.3);font-size:0.88rem">
          Carrinho vazio — adicione produtos acima
        </div>
        """, unsafe_allow_html=True)
    else:
        for idx, it in enumerate(_ss["cart"]):
            img_c = _resolve_img(it.get("foto",""))
            if img_c:
                foto_tag = f'<img src="{img_c}" class="cart-img" onerror="this.style.display=\'none\'">'
            else:
                foto_tag = '<div class="cart-img-ph">📦</div>'

            subtotal = it["qtd"] * it["preco"]
            st.markdown(f"""
            <div class="cart-card">
              {foto_tag}
              <div style="flex:1;min-width:0">
                <div class="cart-nome">{it['nome']}</div>
                <div class="cart-sub">x{it['qtd']} · {_brl(it['preco'])} = <b style="color:#4ade80">{_brl(subtotal)}</b></div>
              </div>
            </div>
            """, unsafe_allow_html=True)

            ci1, ci2, ci3, ci4 = st.columns([1.2, 1.4, 0.6, 0.6])
            with ci1:
                _ss["cart"][idx]["qtd"] = st.number_input(
                    "Qtd", key=f"q_{idx}", min_value=1, step=1,
                    value=int(it["qtd"]), label_visibility="collapsed")
            with ci2:
                _ss["cart"][idx]["preco"] = st.number_input(
                    "Preço", key=f"p_{idx}", min_value=0.0, step=0.10,
                    value=float(it["preco"]), format="%.2f", label_visibility="collapsed")
            with ci3:
                st.caption(f"Est: {int(id_stock.get(it['id'],0))}")
            with ci4:
                if st.button("🗑️", key=f"rm_{idx}"):
                    _ss["cart"].pop(idx); _rerun()


# ═══════════════════════════════════════════════
#  COLUNA DIREITA — Pagamento + Totais + Registrar
# ═══════════════════════════════════════════════
with col_dir:
    st.markdown('<div class="sec-titulo">💳 Pagamento</div>', unsafe_allow_html=True)

    formas = ["Dinheiro","Pix","Cartão Débito","Cartão Crédito","Fiado","Outros"]
    idx_f  = formas.index(_ss["forma"]) if _ss["forma"] in formas else 0
    _ss["forma"] = st.radio("Forma de pagamento", formas, index=idx_f, horizontal=True,
                             label_visibility="collapsed")

    # Cliente
    clientes_list = _carregar_clientes()
    if _ss["forma"] == "Fiado":
        st.markdown("**👤 Cliente** *(obrigatório para fiado)*")
        sel_cli = st.selectbox("Cliente cadastrado", ["(selecione)"] + clientes_list,
                               index=0, key="sel_cli_fiado")
        novo_cli = st.text_input("Ou cadastrar novo", key="novo_cli_fiado")
        escolhido = (novo_cli.strip() or (sel_cli if sel_cli != "(selecione)" else "")).strip()
        _ss["venc_fiado"] = st.date_input("Vencimento do fiado", value=_ss["venc_fiado"])
    else:
        sel_cli  = st.selectbox("Cliente (opcional)", ["(sem cliente)"] + clientes_list,
                                index=0, key="sel_cli_opt")
        novo_cli = st.text_input("Ou cadastrar novo", key="novo_cli_opt")
        escolhido = (novo_cli.strip() or (sel_cli if sel_cli != "(sem cliente)" else "")).strip()
    _ss["cliente"] = _norm_cli(escolhido)

    _ss["obs"]  = st.text_input("Observações", value=_ss["obs"], placeholder="Opcional...")
    _ss["desc"] = st.number_input("Desconto (R$)", min_value=0.0,
                                   value=float(_ss["desc"]), step=0.5, format="%.2f")

    # ── Totalizador visual ──
    total_bruto = sum(i["qtd"]*i["preco"] for i in _ss["cart"])
    total_liq   = max(0.0, total_bruto - float(_ss["desc"]))
    n_itens     = sum(i["qtd"] for i in _ss["cart"])

    _label_itens = "item" if n_itens == 1 else "itens"
    _desc_txt    = f"· Desconto {_brl(_ss['desc'])}" if _ss["desc"] > 0 else ""
    _forma_val   = _ss["forma"]
    if _forma_val == "Dinheiro":   _forma_emoji = "💸"
    elif _forma_val == "Pix":      _forma_emoji = "📱"
    elif "Cart" in _forma_val:     _forma_emoji = "💳"
    elif _forma_val == "Fiado":    _forma_emoji = "📒"
    else:                          _forma_emoji = "💰"

    st.markdown(f"""
    <div class="total-box" style="margin-top:16px">
      <div style="display:flex;justify-content:space-between;align-items:flex-end">
        <div>
          <div class="total-label">Total a receber</div>
          <div class="total-val">{_brl(total_liq)}</div>
          <div class="total-sub">{n_itens} {_label_itens} {_desc_txt}</div>
        </div>
        <div style="text-align:right">
          <div style="font-size:1.8rem">{_forma_emoji}</div>
          <div style="font-size:0.75rem;color:rgba(255,255,255,0.4);margin-top:4px">{_forma_val}</div>
        </div>
      </div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    col_btn1, col_btn2 = st.columns([1.6, 1])
    btn_registrar = col_btn1.button("🧾  Registrar venda", type="primary", use_container_width=True)
    btn_limpar    = col_btn2.button("🧹  Limpar", use_container_width=True)

    if btn_limpar:
        _ss["cart"] = []; st.info("Carrinho limpo."); _rerun()

    # ── Registrar venda ──
    if btn_registrar:
        if not _ss["cart"]:
            st.warning("Carrinho vazio.")
        elif _ss["forma"] == "Fiado" and not _ss["cliente"].strip():
            st.error("Informe o cliente para registrar fiado.")
        else:
            cli_nome = _ss["cliente"].strip()
            if cli_nome: _ensure_cliente(cli_nome)

            sh = conectar_sheets()
            try: ws_v = sh.worksheet(ABA_VEND)
            except:
                ws_v = sh.add_worksheet(title=ABA_VEND, rows=2000, cols=20)
                ws_v.update("A1:K1",[["Data","VendaID","IDProduto","Qtd","PrecoUnit","TotalLinha",
                                       "FormaPagto","Obs","Desconto","TotalCupom","CupomStatus"]])

            # Garante que a aba tem os cabeçalhos certos (sem recarregar tudo)
            ws_v = garantir_aba(ABA_VEND)

            try: ws_m = sh.worksheet(ABA_MOVS)
            except: ws_m = _garantir_aba(sh, ABA_MOVS,
                ["Data","IDProduto","Produto","Tipo","Qtd","Obs","ID","Documento/NF","Origem","SaldoApós"])

            venda_id  = "V-" + datetime.now().strftime("%Y%m%d%H%M%S")
            data_str  = _ss["data_venda"].strftime("%d/%m/%Y")
            desconto  = float(_ss["desc"])
            tot_cupom = max(0.0, total_bruto - desconto)

            # Fiado
            fiado_id = ""; fiado_msg = ""
            if _ss["forma"] == "Fiado":
                ws_f   = _garantir_aba(sh, ABA_FIADO, COLS_FIADO)
                fiado_id = _gerar_id("F")
                venc_s = _ss["venc_fiado"].strftime("%d/%m/%Y") if isinstance(_ss["venc_fiado"], date) else ""
                _append_rows(ws_f, [{"ID":fiado_id,"Data":data_str,"Cliente":cli_nome,
                    "Valor":float(tot_cupom),"Vencimento":venc_s,"Status":"Em aberto",
                    "Obs":_ss.get("obs",""),"DataPagamento":"","FormaPagamento":"","ValorPago":""}])
                fiado_msg = f"\n💳 <b>Fiado</b> — <b>{cli_nome}</b> · venc: {venc_s}"

            novas = []; movs = []
            sba   = {}   # stock before/after
            lucro = 0.0

            for it in _ss["cart"]:
                pid  = str(it["id"])
                qtd  = int(it["qtd"])
                pru  = float(it["preco"])
                sub  = qtd * pru
                lucro += qtd * (pru - id_custo.get(pid, 0.0))
                bef = id_stock.get(pid, 0.0)
                aft = bef - qtd
                sba[pid] = (bef, aft)
                id_stock[pid] = aft

                novas.append({"Data":data_str,"VendaID":venda_id,"IDProduto":pid,
                    "Qtd":str(qtd),"PrecoUnit":f"{pru:.2f}".replace(".",","),
                    "TotalLinha":f"{sub:.2f}".replace(".",","),"FormaPagto":_ss["forma"],
                    "Obs":_ss["obs"],"Desconto":f"{desconto:.2f}".replace(".",","),
                    "TotalCupom":f"{tot_cupom:.2f}".replace(".",","),
                    "CupomStatus":"OK","Cliente":cli_nome,"FiadoID":fiado_id})

                movs.append({"Data":data_str,"IDProduto":pid,"Produto":id_nome.get(pid,pid),
                    "Tipo":"B saída","Qtd":str(qtd),"Obs":_ss.get("obs",""),
                    "ID":venda_id,"Documento/NF":"","Origem":"Vendas rápidas",
                    "SaldoApós":str(int(aft))})

            # SEGURO: nunca faz clear() + reescrita completa — só acrescenta linhas
            append_rows(ws_v, novas)
            append_rows(ws_m, movs)

            # Telegram
            media_tg = []
            for it in _ss["cart"][:10]:
                pid  = str(it["id"])
                foto = id_img.get(pid,"") or it.get("foto","")
                if not foto: continue
                bef, aft = sba.get(pid,("–","–"))
                cap = f"{id_nome.get(pid,pid)}\nx{it['qtd']} @ R$ {it['preco']:.2f} = <b>R$ {it['qtd']*it['preco']:.2f}</b>\nEstoque: {int(bef) if bef!='–' else '–'} → <b>{int(aft) if aft!='–' else '–'}</b>"
                media_tg.append({"type":"photo","media":foto,"caption":cap.replace(".",","),"parse_mode":"HTML"})
            if media_tg: _tg_media(media_tg)

            itens_txt = "\n".join(
                f"• <b>{id_nome.get(str(x['IDProduto']),str(x['IDProduto']))}</b> — x{x['Qtd']} @ {_brl(_to_num(x['PrecoUnit']))} = <b>{_brl(_to_num(x['Qtd'])*_to_num(x['PrecoUnit']))}</b>"
                for x in novas)
            _tg_send(
                f"🧾 <b>Venda registrada</b>\n{data_str}\nForma: <b>{_ss['forma']}</b>"
                + (f"\n👤 {cli_nome}" if cli_nome else "")
                + f"\n{'─'*22}\n{itens_txt}\n{'─'*22}\n"
                + (f"Desconto: {_brl(desconto)}\n" if desconto > 0 else "")
                + f"Total: <b>{_brl(tot_cupom)}</b>"
                + (f"\n💰 Lucro est.: <b>{_brl(lucro)}</b>" if id_custo else "")
                + fiado_msg)

            _ss["cart"] = []
            st.cache_data.clear()
            st.success(f"✅ Venda registrada! Total: {_brl(tot_cupom)}")
            _rerun()

    # ── Histórico ──
    st.markdown('<div class="sec-titulo">📜 Últimas vendas</div>', unsafe_allow_html=True)

    try: vend = carregar_aba(ABA_VEND)
    except: vend = pd.DataFrame()

    if vend.empty:
        st.info("Nenhuma venda ainda.")
    else:
        cv_data  = _first_col(vend, ["Data"])
        cv_idp   = _first_col(vend, ["IDProduto","ProdutoID","ID"])
        cv_qtd   = _first_col(vend, ["Qtd","Quantidade"])
        cv_preco = _first_col(vend, ["PrecoUnit","PreçoUnitário","Preço","Preco"])
        cv_vid   = _first_col(vend, ["VendaID","Pedido","Cupom"])
        cv_forma = _first_col(vend, ["FormaPagto","FormaPagamento","Pagamento","Forma"])
        cv_cli   = _first_col(vend, ["Cliente"])

        vend["_bruto"] = vend.apply(lambda r:
            _to_num(r.get("TotalLinha")) if "TotalLinha" in vend.columns
            else (_to_num(r.get(cv_qtd))*_to_num(r.get(cv_preco)) if cv_qtd and cv_preco else 0.0), axis=1)
        vend["_desc"]  = vend["Desconto"].map(_to_num)  if "Desconto"  in vend.columns else 0.0
        vend["_total"] = vend["TotalCupom"].map(_to_num) if "TotalCupom" in vend.columns else vend["_bruto"]

        agg_cols = {cv_data:"first","_bruto":"sum","_desc":"max","_total":"max"}
        if cv_forma: agg_cols[cv_forma] = "first"
        if cv_cli:   agg_cols[cv_cli]   = "first"
        if "Obs" in vend.columns: agg_cols["Obs"] = "first"

        grp = vend.groupby(cv_vid, dropna=False).agg(agg_cols).reset_index()
        grp.rename(columns={cv_vid:"VendaID", cv_data:"Data"}, inplace=True)
        if cv_forma: grp.rename(columns={cv_forma:"Forma"}, inplace=True)
        if cv_cli:   grp.rename(columns={cv_cli:"Cliente"}, inplace=True)

        try: grp["_ord"] = pd.to_datetime(grp["Data"], format="%d/%m/%Y", errors="coerce")
        except: grp["_ord"] = pd.NaT
        grp = grp.sort_values(["_ord","VendaID"], ascending=[False,False]).head(10).reset_index(drop=True)

        for i, row in grp.iterrows():
            vid       = str(row["VendaID"])
            forma     = str(row.get("Forma","")) if "Forma" in row else "—"
            cli_h     = str(row.get("Cliente","")) if "Cliente" in row else ""
            total_h   = row["_total"] if row["_total"] > 0 else (row["_bruto"] - row["_desc"])
            cancelado = vid.startswith("CN-") or str(row.get("Obs","")).upper().startswith("ESTORNO")

            forma_class = "fiado" if forma=="Fiado" else ("estorno-badge" if cancelado else "")
            forma_icon  = "📒" if forma=="Fiado" else ("⛔" if cancelado else "")

            st.markdown(f"""
            <div class="hist-card {"estorno" if cancelado else ""}">
              <div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:8px">
                <div>
                  <div class="hist-data">{row['Data']}</div>
                  <div class="hist-id">{vid[:22]}{"…" if len(vid)>22 else ""}</div>
                  {f'<div style="font-size:0.75rem;color:rgba(255,255,255,0.4);margin-top:2px">👤 {cli_h}</div>' if cli_h else ""}
                </div>
                <div style="text-align:right">
                  <div class="hist-valor">{"⛔ " if cancelado else ""}{_brl(total_h)}</div>
                  <div style="margin-top:4px"><span class="hist-forma {forma_class}">{forma_icon} {forma}</span></div>
                </div>
              </div>
            </div>
            """, unsafe_allow_html=True)

            # Botões duplicar / cancelar
            def _load_cart(vid=vid):
                linhas = vend[vend[cv_vid]==vid].copy()
                cart = []
                for _, r in linhas.iterrows():
                    pid = str(r.get("IDProduto") or r.get("ProdutoID") or r.get("ID",""))
                    cart.append({"id":pid,"nome":id_nome.get(pid,""),"unid":"un",
                                 "foto":id_img.get(pid,""),
                                 "qtd":int(_to_num(r[cv_qtd])) if cv_qtd else 1,
                                 "preco":float(_to_num(r[cv_preco])) if cv_preco else 0.0})
                _ss["cart"]       = cart
                _ss["forma"]      = str(row.get("Forma","Dinheiro"))
                _ss["obs"]        = ""
                _ss["data_venda"] = date.today()
                _ss["desc"]       = float(row["_desc"]) if pd.notna(row["_desc"]) else 0.0

            def _cancelar(vid=vid):
                if vid.startswith("CN-"): st.warning("Já é estorno."); return
                if any(str(x).startswith(f"CN-{vid}") for x in vend[cv_vid].unique()):
                    st.warning("Estorno já lançado."); return
                linhas = vend[vend[cv_vid]==vid].copy()
                if linhas.empty: st.warning("Cupom não encontrado."); return
                sh2 = conectar_sheets()
                ws2 = sh2.worksheet(ABA_VEND)
                dfv2 = get_as_dataframe(ws2, evaluate_formulas=False, dtype=str, header=0).dropna(how="all")
                dfv2.columns = [c.strip() for c in dfv2.columns]
                for c in ["Desconto","TotalCupom","CupomStatus","Cliente","FiadoID"]:
                    if c not in dfv2.columns: dfv2[c] = None
                cn = f"CN-{vid}"; ds2 = date.today().strftime("%d/%m/%Y"); novas2 = []; tot_est = 0.0
                for _, r in linhas.iterrows():
                    pid  = str(r.get("IDProduto") or r.get("ProdutoID") or r.get("ID",""))
                    qtd2 = -abs(_to_num(r[cv_qtd])) if cv_qtd else -1
                    pru2 = _to_num(r[cv_preco]) if cv_preco else 0.0
                    tot_est += qtd2 * pru2
                    novas2.append({"Data":ds2,"VendaID":cn,"IDProduto":pid,
                        "Qtd":str(int(qtd2)),"PrecoUnit":f"{pru2:.2f}".replace(".",","),
                        "TotalLinha":f"{qtd2*pru2:.2f}".replace(".",","),
                        "FormaPagto":f"Estorno - {forma}","Obs":f"ESTORNO DE {vid}",
                        "Desconto":"0,00","TotalCupom":"0,00","CupomStatus":"ESTORNO",
                        "Cliente":str(r.get("Cliente","")),"FiadoID":""})
                # SEGURO: só acrescenta linhas de estorno
                append_rows(ws2, novas2)
                try: ws_m2 = sh2.worksheet(ABA_MOVS)
                except: ws_m2 = _garantir_aba(sh2, ABA_MOVS,
                    ["Data","IDProduto","Produto","Tipo","Qtd","Obs","ID","Documento/NF","Origem","SaldoApós"])
                movs2 = []
                for _, r in linhas.iterrows():
                    pid  = str(r.get("IDProduto") or r.get("ProdutoID") or r.get("ID",""))
                    qtd2 = int(abs(_to_num(r[cv_qtd]))) if cv_qtd else 1
                    bef2 = id_stock.get(pid, 0.0); aft2 = bef2 + qtd2
                    id_stock[pid] = aft2
                    movs2.append({"Data":ds2,"IDProduto":pid,"Produto":id_nome.get(pid,pid),
                        "Tipo":"B entrada","Qtd":str(qtd2),"Obs":f"ESTORNO DE {vid}",
                        "ID":cn,"Documento/NF":"","Origem":"Vendas rápidas","SaldoApós":str(int(aft2))})
                _append_rows(ws_m2, movs2)
                _tg_send(f"⛔ <b>Estorno lançado</b>\n{ds2}\n{_brl(abs(tot_est))}\nCupom: {vid}")
                st.cache_data.clear(); st.success("Estorno lançado."); _rerun()

            cb1, cb2 = st.columns([1, 1])
            cb1.button("🔁 Duplicar", key=f"dup_{i}", on_click=_load_cart, use_container_width=True)
            cb2.button("⛔ Cancelar", key=f"cn_{i}", disabled=cancelado,
                       on_click=_cancelar, use_container_width=True)
