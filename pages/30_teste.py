# pages/00_setup_planilha.py
# -*- coding: utf-8 -*-
import streamlit as st
import gspread
from google.oauth2.service_account import Credentials

st.set_page_config(page_title="Setup da Planilha - Ebenezér Variedades", page_icon="🧼", layout="centered")
st.title("🧼 Setup da Planilha — Ebenezér Variedades")

# =========================================
# 1) CONFIG: ID da planilha + colunas por aba
# =========================================
# Se você já colocou o SHEET_ID nos secrets, ele aparece aqui automaticamente:
sheet_id_default = st.secrets.get("SHEET_ID", "COLE_AQUI_O_ID_DA_SUA_PLANILHA")

SHEET_ID = st.text_input(
    "Google Sheet ID",
    value=sheet_id_default,
    help="Ex.: em https://docs.google.com/spreadsheets/d/1AbC...XYZ/edit, o ID é 1AbC...XYZ",
)

ABAS = {
    "Produtos": [
        "SKU", "EAN", "Nome", "Categoria", "Unidade",
        "Fornecedor", "CustoAtual", "PreçoVenda", "Markup %",
        "Margem %", "EstoqueAtual", "EstoqueMin", "LeadTimeDias",
        "Ativo?"
    ],
    "Compras": [
        "Data", "NF/Ref", "Fornecedor", "SKU", "Qtd",
        "CustoUnit", "FreteRateado", "OutrosCustos", "Obs"
    ],
    "Vendas": [
        "Data", "Documento", "SKU", "Qtd", "PreçoUnit",
        "Canal", "Pagamento", "Taxa %", "Desconto R$", "Cliente/Obs"
    ],
    "MovimentosEstoque": [
        "Data", "SKU", "Tipo", "Qtd", "Documento/NF",
        "Origem", "Obs", "SaldoApós"
    ],
    "Ajustes": [
        "Data", "SKU", "Qtd", "Motivo", "Responsável", "Obs"
    ],
    "Fornecedores": [
        "Nome", "CNPJ/CPF", "Contato", "Telefone", "Email", "PrazoDias", "Observações"
    ],
    "Config": [
        "Parametro", "Valor"
    ],
}

CONFIG_INICIAIS = [
    ("taxa_cartao_padrao_pct", "0.023"),
    ("margem_alvo_padrao_pct", "0.35"),
    ("canal_padrao", "balcao"),
]

# =========================================
# 2) Conexão com Google Sheets
# =========================================
@st.cache_resource(show_spinner=False)
def conectar_sheets(sheet_id: str):
    info = st.secrets.get("GCP_SERVICE_ACCOUNT", None)
    if not info:
        st.error("🚫 Faltam as credenciais em st.secrets['GCP_SERVICE_ACCOUNT'].")
        st.stop()
    scopes = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    client = gspread.authorize(creds)
    return client.open_by_key(sheet_id)

def obter_aba(planilha, nome):
    try:
        return planilha.worksheet(nome)
    except gspread.WorksheetNotFound:
        return None

def criar_aba(planilha, nome, cols):
    linhas = 1000
    colunas = max(len(cols) + 3, 8)
    ws = planilha.add_worksheet(title=nome, rows=linhas, cols=colunas)
    ws.update("A1", [cols])
    return ws

def garantir_cabecalhos(ws, cols):
    try:
        dados = ws.get_all_values()
    except Exception:
        dados = []
    if not dados:
        ws.update("A1", [cols])
        return True
    primeira = dados[0] if len(dados) > 0 else []
    # Se a 1ª linha estiver vazia ou com menos colunas que o necessário, reescreve cabeçalhos
    if (not primeira) or all((c.strip() == "" for c in primeira)) or (len(primeira) < len(cols)):
        ws.update("A1", [cols])
        return True
    return False

def garantir_config_inicial(ws):
    try:
        dados = ws.get_all_values()
    except Exception:
        dados = []
    existentes = {linha[0] for linha in dados[1:] if linha and len(linha) >= 1}
    novas = [[k, v] for k, v in CONFIG_INICIAIS if k not in existentes]
    if novas:
        ws.append_rows(novas)

# =========================================
# 3) UI
# =========================================
if SHEET_ID and SHEET_ID != "COLE_AQUI_O_ID_DA_SUA_PLANILHA":
    st.info(f"📄 Planilha: https://docs.google.com/spreadsheets/d/{SHEET_ID}/edit")

opcoes = st.multiselect(
    "Quais abas você quer criar/verificar?",
    options=list(ABAS.keys()),
    default=list(ABAS.keys()),
)

if st.button("🚀 Criar / Verificar Estrutura", type="primary", use_container_width=True):
    if not SHEET_ID or SHEET_ID == "COLE_AQUI_O_ID_DA_SUA_PLANILHA":
        st.error("Informe o **Google Sheet ID** antes de continuar.")
        st.stop()
    try:
        with st.spinner("Conectando ao Google Sheets..."):
            planilha = conectar_sheets(SHEET_ID)
    except gspread.SpreadsheetNotFound:
        st.error("❌ Planilha não encontrada. Confirme o ID e o compartilhamento com o e-mail da service account.")
        st.stop()
    except Exception as e:
        st.error(f"❌ Erro de conexão: {e}")
        st.stop()

    criadas, atualizadas, ja_ok = [], [], []
    for nome in opcoes:
        cols = ABAS[nome]
        ws = obter_aba(planilha, nome)
        if ws is None:
            ws = criar_aba(planilha, nome, cols)
            criadas.append(nome)
        else:
            if garantir_cabecalhos(ws, cols):
                atualizadas.append(nome)
            else:
                ja_ok.append(nome)

        if nome == "Config":
            garantir_config_inicial(ws)

    st.success("✅ Estrutura pronta!")
    if criadas:
        st.write("🆕 Abas **criadas**:", ", ".join(criadas))
    if atualizadas:
        st.write("🔁 Abas com **cabeçalhos atualizados**:", ", ".join(atualizadas))
    if ja_ok:
        st.write("👌 Abas **já estavam corretas**:", ", ".join(ja_ok))

st.caption("Depois use as páginas **Produtos**, **Compras** e **Vendas** para operar o sistema.")
