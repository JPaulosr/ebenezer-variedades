# pages/06_fiado.py — Fiado simples para Ebenezér Variedades
# -*- coding: utf-8 -*-
import json, unicodedata, re
from datetime import datetime, date, timedelta

import streamlit as st
import pandas as pd
import gspread
from gspread_dataframe import get_as_dataframe
from gspread.utils import rowcol_to_a1
from google.oauth2.service_account import Credentials

# ---- Config UI ----
st.set_page_config(page_title="Fiado — Ebenezér Variedades", page_icon="💳", layout="wide")
st.title("💳 Fiado — lançar, quitar e acompanhar")

# =========================
# Autenticação / Sheets
# =========================
def _normalize_private_key(key: str) -> str:
    if not isinstance(key, str): return key
    key = key.replace("\\n", "\n")
    # remove controles invisíveis (mantém \n \r \t)
    key = "".join(ch for ch in key if unicodedata.category(ch)[0] != "C" or ch in ("\n","\r","\t"))
    return key

def _load_sa() -> dict:
    svc = st.secrets.get("GCP_SERVICE_ACCOUNT")
    if svc is None:
        st.error("🛑 GCP_SERVICE_ACCOUNT ausente no Secrets."); st.stop()
    if isinstance(svc, str):
        svc = json.loads(svc)
    svc = dict(svc)
    svc["private_key"] = _normalize_private_key(str(svc["private_key"]))
    return svc

@st.cache_resource
def conectar_sheets():
    scopes = ["https://www.googleapis.com/auth/spreadsheets",
              "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(_load_sa(), scopes=scopes)
    gc = gspread.authorize(creds)
    url_or_id = st.secrets.get("PLANILHA_URL", "")
    if not url_or_id:
        st.error("🛑 PLANILHA_URL ausente no Secrets."); st.stop()
    return gc.open_by_url(url_or_id) if str(url_or_id).startswith("http") else gc.open_by_key(url_or_id)

def _norm_key(s: str) -> str:
    return unicodedata.normalize("NFKC", str(s or "")).strip().casefold()

def _fmt_brl(v) -> str:
    try:
        return ("R$ "+f"{float(v):,.2f}").replace(",", "X").replace(".", ",").replace("X",".")
    except:
        return "R$ 0,00"

def _to_float(x, default=0.0):
    if x is None: return default
    s = str(x).strip()
    if s == "" or s.lower() in ("nan","none"): return default
    s = s.replace("R$","").replace(" ","")
    s = s.replace(",", ".")
    s = re.sub(r"[^0-9.\-]","", s)
    if s.count(".")>1:
        parts = s.split("."); s = "".join(parts[:-1]) + "." + parts[-1]
    try: return float(s)
    except: return default

# =========================
# Abas da planilha
# =========================
ABA_CLIENTES = "Clientes"
ABA_FIADO    = "Fiado"
ABA_PAGT     = "Fiado_Pagamentos"

COLS_CLIENTES = ["Cliente","Telefone","Obs"]
COLS_FIADO    = ["ID","Data","Cliente","Valor","Vencimento","Status","Obs","DataPagamento","FormaPagamento","ValorPago"]
COLS_PAGT     = ["PagamentoID","DataPagamento","Cliente","Forma","TotalPago","IDsFiado","Obs"]

def garantir_aba(ss, nome, cols_padrao):
    try:
        ws = ss.worksheet(nome)
    except gspread.WorksheetNotFound:
        ws = ss.add_worksheet(title=nome, rows=5000, cols=max(10,len(cols_padrao)))
        ws.append_row(cols_padrao)
        return ws
    # garante cabeçalhos (sem duplicatas, respeita nomes atuais)
    headers = ws.row_values(1)
    if not headers:
        ws.append_row(cols_padrao)
    else:
        # remove duplicatas mantendo primeira ocorrência
        fix = []
        seen = set()
        for h in headers:
            k = _norm_key(h)
            if k in seen: continue
            seen.add(k); fix.append(h.strip())
        if fix != headers:
            ws.update('A1', [fix])
        # adiciona colunas que estiverem faltando (por comparação normalizada)
        have = {_norm_key(h) for h in fix}
        missing = [c for c in cols_padrao if _norm_key(c) not in have]
        if missing:
            ws.update('A1', [fix + missing])
    return ws

def col_map(ws):
    headers = ws.row_values(1)
    mp = {}
    for i,h in enumerate(headers):
        k = _norm_key(h)
        if k and k not in mp: mp[k] = i+1
    return mp

@st.cache_data(ttl=20, show_spinner=False)
def load_df(aba: str) -> pd.DataFrame:
    sh = conectar_sheets()
    ws = garantir_aba(sh, aba, {
        ABA_CLIENTES: COLS_CLIENTES,
        ABA_FIADO:    COLS_FIADO,
        ABA_PAGT:     COLS_PAGT
    }[aba])
    df = get_as_dataframe(ws, evaluate_formulas=True, dtype=str, header=0).dropna(how="all")
    df.columns = [c.strip() for c in df.columns]
    # garante colunas
    base_cols = {ABA_CLIENTES: COLS_CLIENTES, ABA_FIADO: COLS_FIADO, ABA_PAGT: COLS_PAGT}[aba]
    for c in base_cols:
        if c not in df.columns: df[c] = ""
    df = df.loc[:, ~pd.Index(df.columns).duplicated(keep="first")]
    return df.fillna("")

def append_rows(ws, rows: list[dict]):
    headers = ws.row_values(1)
    if not headers:
        ws.append_row(list(rows[0].keys()))
        headers = ws.row_values(1)
    hdr_norm = [_norm_key(h) for h in headers]
    to_append = []
    for d in rows:
        dn = {_norm_key(k): v for k,v in d.items()}
        to_append.append([dn.get(hn, "") for hn in hdr_norm])
    if to_append:
        ws.append_rows(to_append, value_input_option="USER_ENTERED")

def gerar_id(prefixo="F"):
    # F-YYYYMMDDHHMMSSmmm
    return f"{prefixo}-{datetime.now().strftime('%Y%m%d%H%M%S%f')[:-3]}"

# =========================
# UI — Tabs
# =========================
tab_novo, tab_quitar, tab_abertos = st.tabs(["➕ Novo fiado", "💰 Registrar pagamento", "📋 Em aberto"])

# ---------- NOVO FIADO ----------
with tab_novo:
    st.subheader("➕ Lançar fiado")

    df_cli = load_df(ABA_CLIENTES)
    lista_clientes = sorted([c for c in df_cli["Cliente"].astype(str).str.strip().unique().tolist() if c])

    c1,c2 = st.columns([1,1])
    with c1:
        cliente_sel = st.selectbox("Cliente", options=[""] + lista_clientes, index=0)
        cliente_novo = st.text_input("Ou cadastrar novo cliente (nome)", value="")
        tel_novo = st.text_input("Telefone (opcional)", value="")
    with c2:
        data_fiado = st.date_input("Data do fiado", value=date.today())
        venc = st.date_input("Vencimento (opcional)", value=date.today())
        valor = st.number_input("Valor (R$)", min_value=0.0, step=1.0, format="%.2f")
        obs = st.text_input("Observações (opcional)", value="")

    if st.button("Salvar fiado", use_container_width=True):
        # valida nome
        cliente_final = (cliente_sel or "").strip() or (cliente_novo or "").strip()
        if not cliente_final:
            st.error("Informe o cliente (selecione ou cadastre).")
            st.stop()

        sh = conectar_sheets()
        ws_cli  = garantir_aba(sh, ABA_CLIENTES, COLS_CLIENTES)
        ws_fiado= garantir_aba(sh, ABA_FIADO, COLS_FIADO)

        # cria cliente se for novo
        if cliente_final not in lista_clientes and cliente_final:
            append_rows(ws_cli, [{"Cliente": cliente_final, "Telefone": tel_novo, "Obs": ""}])

        # salva fiado
        fid = gerar_id("F")
        linha = {
            "ID": fid,
            "Data": data_fiado.strftime("%d/%m/%Y"),
            "Cliente": cliente_final,
            "Valor": float(valor),
            "Vencimento": venc.strftime("%d/%m/%Y") if venc else "",
            "Status": "Em aberto",
            "Obs": obs,
            "DataPagamento": "",
            "FormaPagamento": "",
            "ValorPago": ""
        }
        append_rows(ws_fiado, [linha])
        st.success(f"Fiado lançado para **{cliente_final}** no valor de **{_fmt_brl(valor)}** (ID {fid}).")
        st.cache_data.clear()

# ---------- REGISTRAR PAGAMENTO ----------
with tab_quitar:
    st.subheader("💰 Registrar pagamento de fiados")

    df_fiado = load_df(ABA_FIADO)
    df_fiado["ValorNum"] = df_fiado["Valor"].apply(_to_float)
    abertos = df_fiado[df_fiado["Status"].astype(str).str.lower()=="em aberto"].copy()

    if abertos.empty:
        st.info("Nenhum fiado em aberto.")
    else:
        clientes_abertos = sorted(abertos["Cliente"].astype(str).str.strip().unique().tolist())
        c1,c2 = st.columns([1,1])
        with c1:
            cli = st.selectbox("Cliente", options=[""]+clientes_abertos, index=0)
        with c2:
            data_pag = st.date_input("Data do pagamento", value=date.today())

        subset = abertos if not cli else abertos[abertos["Cliente"]==cli].copy()

        if subset.empty:
            st.info("Nenhum lançamento em aberto para esse cliente.")
        else:
            # Multi-seleção por ID
            subset["Label"] = subset.apply(
                lambda r: f"{r['ID']} • {r['Data']} • {_fmt_brl(r['ValorNum'])} • Venc: {r.get('Vencimento','') or '-'} • {r.get('Obs','') or ''}",
                axis=1
            )
            ids = subset["ID"].tolist()
            labels = {row["ID"]: row["Label"] for _, row in subset.iterrows()}

            ids_sel = st.multiselect("Selecione os fiados a quitar", options=ids, format_func=lambda x: labels.get(x, x))

            forma = st.selectbox("Forma de pagamento", ["Dinheiro","Pix","Cartão","Transferência","Outro"], index=1)
            obs_pag = st.text_input("Observação (opcional)", value="")

            total_sel = float(subset[subset["ID"].isin(ids_sel)]["ValorNum"].sum()) if ids_sel else 0.0
            st.metric("Total selecionado", _fmt_brl(total_sel))

            can_save = bool(ids_sel) and total_sel > 0
            if st.button("Quitar selecionados", use_container_width=True, disabled=not can_save):
                sh = conectar_sheets()
                ws_fiado = garantir_aba(sh, ABA_FIADO, COLS_FIADO)
                ws_pagt  = garantir_aba(sh, ABA_PAGT,  COLS_PAGT)

                # atualiza linhas selecionadas
                cmap = col_map(ws_fiado)
                updates = []
                for _, row in df_fiado[df_fiado["ID"].isin(ids_sel)].iterrows():
                    idx = int(row.name) + 2  # cabeçalho na linha 1
                    # campos a escrever
                    pairs = {
                        "Status": "Pago",
                        "DataPagamento": data_pag.strftime("%d/%m/%Y"),
                        "FormaPagamento": forma,
                        "ValorPago": _to_float(row["Valor"])
                    }
                    for k,v in pairs.items():
                        c = cmap.get(_norm_key(k))
                        if c:
                            updates.append({"range": rowcol_to_a1(idx, c), "values": [[v]]})
                if updates:
                    ws_fiado.batch_update(updates, value_input_option="USER_ENTERED")

                # escreve resumo do pagamento
                pid = gerar_id("P")
                append_rows(ws_pagt, [{
                    "PagamentoID": pid,
                    "DataPagamento": data_pag.strftime("%d/%m/%Y"),
                    "Cliente": cli or "(vários)",
                    "Forma": forma,
                    "TotalPago": total_sel,
                    "IDsFiado": ";".join(ids_sel),
                    "Obs": obs_pag
                }])

                st.success(f"Pagamento registrado: **{_fmt_brl(total_sel)}** ({forma}).")
                st.cache_data.clear()

# ---------- EM ABERTO ----------
with tab_abertos:
    st.subheader("📋 Fiados em aberto")

    df_fiado = load_df(ABA_FIADO)
    if df_fiado.empty:
        st.info("Sem registros.")
    else:
        df_fiado["ValorNum"] = df_fiado["Valor"].apply(_to_float)
        em_aberto = df_fiado[df_fiado["Status"].astype(str).str.lower()=="em aberto"].copy()

        c1,c2 = st.columns([1,1])
        with c1:
            filtro_cli = st.text_input("Filtrar por cliente", "")
        with c2:
            so_vencidos = st.checkbox("Somente vencidos", value=False)

        if filtro_cli.strip():
            em_aberto = em_aberto[
                em_aberto["Cliente"].astype(str).str.contains(filtro_cli.strip(), case=False, na=False)
            ]

        # atraso
        def _as_date(s):
            try: return datetime.strptime(str(s), "%d/%m/%Y").date()
            except: return None
        hoje = date.today()
        em_aberto["Venc_d"] = em_aberto["Vencimento"].apply(_as_date)
        em_aberto["AtrasoDias"] = em_aberto["Venc_d"].apply(lambda d: (hoje - d).days if (d and hoje>d) else 0)
        if so_vencidos:
            em_aberto = em_aberto[em_aberto["AtrasoDias"] > 0]

        # resumo por cliente
        resumo = em_aberto.groupby("Cliente", as_index=False)["ValorNum"].sum().rename(columns={"ValorNum":"Total"})
        k1,k2 = st.columns(2)
        with k1: st.metric("Clientes com fiado", f"{len(resumo)}")
        with k2: st.metric("Total em aberto", _fmt_brl(resumo["Total"].sum()))

        st.markdown("**Por cliente**")
        st.dataframe(resumo.sort_values("Total", ascending=False), use_container_width=True, hide_index=True)

        st.markdown("---")
        st.markdown("**Lançamentos**")
        cols_show = ["ID","Data","Cliente","Valor","Vencimento","AtrasoDias","Obs"]
        cols_show = [c for c in cols_show if c in em_aberto.columns]
        st.dataframe(
            em_aberto.sort_values(["AtrasoDias","ValorNum"], ascending=[False,False])[cols_show],
            use_container_width=True, hide_index=True
        )

        # export
        csv_bytes = em_aberto[cols_show].to_csv(index=False).encode("utf-8-sig")
        st.download_button("⬇️ Exportar (CSV)", data=csv_bytes, file_name="fiado_em_aberto.csv")
