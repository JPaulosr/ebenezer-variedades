# pages/00_setup_planilha.py
# -*- coding: utf-8 -*-
import streamlit as st
import gspread
import re
import pandas as pd
from google.oauth2.service_account import Credentials
from datetime import datetime

st.set_page_config(page_title="Setup/Migração da Planilha — Ebenezér Variedades", page_icon="🧼", layout="centered")
st.title("🧼 Setup da Planilha — criar abas / MIGRAR cabeçalhos")

# ===== 1) CONFIG =====
sheet_id_default = st.secrets.get("SHEET_ID", "")
SHEET_ID = st.text_input(
    "Google Sheet ID",
    value=sheet_id_default,
    help="Cole apenas o ID (trecho entre /d/ e /edit)."
)

# Novo padrão (SEM SKU/EAN)
COLS_PRODUTOS = [
    "ID", "Nome", "Categoria", "Unidade", "Fornecedor",
    "CustoAtual", "PreçoVenda", "Markup %", "Margem %",
    "EstoqueAtual", "EstoqueMin", "LeadTimeDias", "Ativo?"
]
ABAS = {
    "Produtos": COLS_PRODUTOS,
    "Compras": [
        "Data","NF/Ref","Fornecedor","ID","Qtd","CustoUnit",
        "FreteRateado","OutrosCustos","Obs"
    ],
    "Vendas": [
        "Data","Documento","ID","Qtd","PreçoUnit","Canal",
        "Pagamento","Taxa %","Desconto R$","Cliente/Obs"
    ],
    "MovimentosEstoque": [
        "Data","ID","Tipo","Qtd","Documento/NF","Origem","Obs","SaldoApós"
    ],
    "Ajustes": [
        "Data","ID","Qtd","Motivo","Responsável","Obs"
    ],
    "Fornecedores": [
        "Nome","CNPJ/CPF","Contato","Telefone","Email","PrazoDias","Observações"
    ],
    "Config": [
        "Parametro","Valor"
    ],
}
CONFIG_INICIAIS = [
    ("taxa_cartao_padrao_pct","0.023"),
    ("margem_alvo_padrao_pct","0.35"),
    ("canal_padrao","balcao"),
]

# ===== 2) CONEXÃO =====
@st.cache_resource(show_spinner=False)
def conectar_sheets(sheet_id: str):
    svc = st.secrets.get("GCP_SERVICE_ACCOUNT", None)
    if not svc:
        st.error("🚫 Falta o JSON da Service Account em st.secrets['GCP_SERVICE_ACCOUNT'].")
        st.stop()
    scopes = ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(svc, scopes=scopes)
    client = gspread.authorize(creds)
    return client.open_by_key(sheet_id)

def obter_aba(sh, nome):
    try: return sh.worksheet(nome)
    except gspread.WorksheetNotFound: return None

def criar_aba(sh, nome, cols):
    ws = sh.add_worksheet(title=nome, rows=1000, cols=max(len(cols)+3, 20))
    ws.update("A1", [cols])
    return ws

def garantir_config_inicial(ws):
    vals = ws.get_all_values()
    existentes = {linha[0] for linha in vals[1:] if linha and len(linha)>=1}
    novas = [[k,v] for k,v in CONFIG_INICIAIS if k not in existentes]
    if novas: ws.append_rows(novas)

def proximo_id_sequencial(existing_ids):
    """Retorna gerador de PRO-0001, PRO-0002… considerando IDs já existentes."""
    pad = re.compile(r"PRO-(\d{4})$")
    usados = []
    for x in existing_ids:
        m = pad.match(str(x).strip())
        if m:
            try: usados.append(int(m.group(1)))
            except: pass
    base = max(usados) if usados else 0
    def _next():
        nonlocal base
        base += 1
        return f"PRO-{base:04d}"
    return _next

def escrever_dataframe(ws, df):
    # Limpa e reescreve (header + dados)
    values = [df.columns.tolist()] + df.fillna("").astype(str).values.tolist()
    ws.clear()
    ws.update("A1", values)

# ===== 3) UI =====
st.caption("Modo padrão: cria abas e cabeçalhos se não existirem. Modo MIGRAÇÃO: reescreve cabeçalhos p/ novo padrão e ajusta dados.")
forcar_migracao = st.toggle("⚠️ Modo MIGRAÇÃO (reescreve cabeçalhos e reordena dados da aba Produtos)")

if SHEET_ID:
    st.info(f"📄 Planilha: https://docs.google.com/spreadsheets/d/{SHEET_ID}/edit")

selecionadas = st.multiselect(
    "Quais abas criar/verificar (ou migrar)?",
    options=list(ABAS.keys()),
    default=list(ABAS.keys())
)

if st.button("🚀 Executar", type="primary", use_container_width=True):
    if not SHEET_ID:
        st.error("Cole o **Google Sheet ID**.")
        st.stop()

    try:
        sh = conectar_sheets(SHEET_ID)
    except gspread.SpreadsheetNotFound:
        st.error("❌ Planilha não encontrada (ID errado ou sem compartilhamento com a Service Account).")
        st.stop()
    except Exception as e:
        st.error(f"❌ Erro ao conectar: {e}")
        st.stop()

    criadas, atualizadas, ok = [], [], []

    # 3.1 Criar/verificar (padrão)
    for nome in selecionadas:
        cols = ABAS[nome]
        ws = obter_aba(sh, nome)
        if ws is None:
            ws = criar_aba(sh, nome, cols)
            if nome == "Config": garantir_config_inicial(ws)
            criadas.append(nome)
        else:
            # Cabeçalhos padrão (sem forçar)
            vals = ws.get_all_values()
            if not vals or not vals[0] or len(vals[0]) < len(cols):
                ws.update("A1", [cols])
                if nome == "Config": garantir_config_inicial(ws)
                atualizadas.append(nome)
            else:
                ok.append(nome)

    # 3.2 MIGRAÇÃO (sobrescrever cabeçalhos + reordenar dados)
    if forcar_migracao:
        # a) PRODUTOS — mapeia SKU/EAN => ID e reordena pro novo padrão
        ws_prod = obter_aba(sh, "Produtos")
        if ws_prod:
            raw = ws_prod.get_all_values()
            if raw:
                old_cols = raw[0]
                df_old = pd.DataFrame(raw[1:], columns=old_cols)
                # Normaliza nomes (remove espaços e case)
                norm = {c: c.strip() for c in old_cols}

                # Monta df_new com as novas colunas na ordem desejada
                df_new = pd.DataFrame(columns=COLS_PRODUTOS)

                # 1) ID: usa 'ID' se existir; se não, tenta 'SKU'; se vazio, gera
                cand_id = None
                if "ID" in df_old.columns: cand_id = df_old["ID"].astype(str)
                elif "SKU" in df_old.columns: cand_id = df_old["SKU"].astype(str)
                else: cand_id = pd.Series([""]*len(df_old))
                df_new["ID"] = cand_id

                # 2) Copia colunas compatíveis por nome
                copy_map = {
                    "Nome":"Nome",
                    "Categoria":"Categoria",
                    "Unidade":"Unidade",
                    "Fornecedor":"Fornecedor",
                    "CustoAtual":"CustoAtual",
                    "PreçoVenda":"PreçoVenda",
                    "Markup %":"Markup %",
                    "Margem %":"Margem %",
                    "EstoqueAtual":"EstoqueAtual",
                    "EstoqueMin":"EstoqueMin",
                    "LeadTimeDias":"LeadTimeDias",
                    "Ativo?":"Ativo?",
                }
                for old, new in copy_map.items():
                    if old in df_old.columns:
                        df_new[new] = df_old[old]
                    else:
                        df_new[new] = ""

                # 3) Gera IDs vazios considerando existentes
                gen = proximo_id_sequencial(df_new["ID"].tolist())
                df_new["ID"] = [x if str(x).strip() not in ("", "nan", "None") else gen() for x in df_new["ID"]]

                # 4) Recalcula Markup/Margem se possível
                def to_num(s):
                    try: return float(str(s).replace(",", "."))
                    except: return None
                mk, mg = [], []
                for cst, prc, mk0, mg0 in zip(df_new["CustoAtual"], df_new["PreçoVenda"], df_new["Markup %"], df_new["Margem %"]):
                    c, p = to_num(cst), to_num(prc)
                    if c is not None and p is not None and c >= 0 and p > 0:
                        mk.append((p - c) / c)
                        mg.append((p - c) / p)
                    else:
                        # mantém o que já tinha
                        try: mk.append(float(str(mk0).replace(",", ".")))
                        except: mk.append("")
                        try: mg.append(float(str(mg0).replace(",", ".")))
                        except: mg.append("")
                df_new["Markup %"] = mk
                df_new["Margem %"] = mg

                # 5) Escreve (limpa e grava ordenado) na própria aba
                escrever_dataframe(ws_prod, df_new)
                st.success("🧩 Migração concluída na aba **Produtos** (cabeçalhos atualizados e dados reordenados).")

        # b) Outras abas — apenas renomear cabeçalho SKU -> ID, se existir
        for nome in ["Compras","Vendas","MovimentosEstoque","Ajustes"]:
            ws = obter_aba(sh, nome)
            if ws:
                vals = ws.get_all_values()
                if vals and vals[0]:
                    headers = vals[0]
                    if "SKU" in headers and "ID" not in headers:
                        headers = ["ID" if h=="SKU" else h for h in headers]
                        vals[0] = headers
                        ws.clear()
                        ws.update("A1", vals)
                        st.info(f"🔁 Cabeçalho ajustado em **{nome}** (SKU → ID).")

    st.success("✅ Finalizado!")
    if criadas: st.write("🆕 **Criadas**:", ", ".join(criadas))
    if atualizadas: st.write("🔁 **Cabeçalhos atualizados**:", ", ".join(atualizadas))
    if ok: st.write("👌 **Já estavam corretas**:", ", ".join(ok))

with st.expander("🔎 Diagnóstico"):
    if st.button("Listar abas"):
        try:
            sh = conectar_sheets(SHEET_ID)
            st.write([w.title for w in sh.worksheets()])
        except Exception as e:
            st.error(f"Falhou: {e}")
