# pages/00_setup_planilha.py
# -*- coding: utf-8 -*-
import streamlit as st
import gspread
from google.oauth2.service_account import Credentials

st.set_page_config(page_title="Setup da Planilha — Ebenezér Variedades", page_icon="🧼", layout="centered")
st.title("🧼 Setup da Planilha — cria abas e colunas automaticamente")

# ===== 1) CONFIG =====
sheet_id_default = st.secrets.get("SHEET_ID", "")
SHEET_ID = st.text_input(
    "Google Sheet ID",
    value=sheet_id_default,
    help="Cole apenas o ID (o trecho entre /d/ e /edit)."
)

ABAS = {
    "Produtos": [
        "SKU","EAN","Nome","Categoria","Unidade","Fornecedor",
        "CustoAtual","PreçoVenda","Markup %","Margem %","EstoqueAtual",
        "EstoqueMin","LeadTimeDias","Ativo?"
    ],
    "Compras": [
        "Data","NF/Ref","Fornecedor","SKU","Qtd","CustoUnit",
        "FreteRateado","OutrosCustos","Obs"
    ],
    "Vendas": [
        "Data","Documento","SKU","Qtd","PreçoUnit","Canal",
        "Pagamento","Taxa %","Desconto R$","Cliente/Obs"
    ],
    "MovimentosEstoque": [
        "Data","SKU","Tipo","Qtd","Documento/NF","Origem","Obs","SaldoApós"
    ],
    "Ajustes": [
        "Data","SKU","Qtd","Motivo","Responsável","Obs"
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

# ===== 2) Conexão =====
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
    ws = sh.add_worksheet(title=nome, rows=1000, cols=max(len(cols)+3, 8))
    ws.update("A1", [cols])
    return ws

def garantir_cabecalhos(ws, cols):
    vals = ws.get_all_values()
    if not vals or not vals[0] or all(c.strip()=="" for c in vals[0]) or len(vals[0]) < len(cols):
        ws.update("A1", [cols])
        return True
    return False

def garantir_config_inicial(ws):
    vals = ws.get_all_values()
    existentes = {linha[0] for linha in vals[1:] if linha and len(linha)>=1}
    novas = [[k,v] for k,v in CONFIG_INICIAIS if k not in existentes]
    if novas: ws.append_rows(novas)

# ===== 3) UI =====
if SHEET_ID:
    st.info(f"📄 Planilha: https://docs.google.com/spreadsheets/d/{SHEET_ID}/edit")

sel = st.multiselect("Quais abas criar/verificar?", options=list(ABAS.keys()), default=list(ABAS.keys()))

if st.button("🚀 Criar / Verificar Estrutura", type="primary", use_container_width=True):
    if not SHEET_ID:
        st.error("Cole o **Google Sheet ID**.")
        st.stop()
    try:
        with st.spinner("Conectando..."):
            sh = conectar_sheets(SHEET_ID)
    except gspread.SpreadsheetNotFound:
        st.error("❌ Planilha não encontrada (ID errado ou sem compartilhamento com a Service Account).")
        st.stop()
    except Exception as e:
        st.error(f"❌ Erro ao conectar: {e}")
        st.stop()

    criadas, atualizadas, ok = [], [], []
    for nome, cols in ((n, ABAS[n]) for n in sel):
        ws = obter_aba(sh, nome)
        if ws is None:
            criar_aba(sh, nome, cols)
            criadas.append(nome)
        else:
            if garantir_cabecalhos(ws, cols): atualizadas.append(nome)
            else: ok.append(nome)
        if nome == "Config":
            garantir_config_inicial(obter_aba(sh, "Config"))

    st.success("✅ Estrutura pronta!")
    if criadas: st.write("🆕 **Criadas**:", ", ".join(criadas))
    if atualizadas: st.write("🔁 **Cabeçalhos atualizados**:", ", ".join(atualizadas))
    if ok: st.write("👌 **Já estavam corretas**:", ", ".join(ok))

with st.expander("🔎 Diagnóstico"):
    if st.button("Listar abas da planilha"):
        try:
            sh = conectar_sheets(SHEET_ID)
            st.write([w.title for w in sh.worksheets()])
        except Exception as e:
            st.error(f"Falhou: {e}")
