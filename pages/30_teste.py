# pages/00_setup_planilha.py
# -*- coding: utf-8 -*-
import streamlit as st
import gspread
from gspread_dataframe import get_as_dataframe
from google.oauth2.service_account import Credentials

st.set_page_config(page_title="Setup da Planilha - Ebenezér Variedades", page_icon="🧼", layout="centered")
st.title("🧼 Setup da Planilha — Ebenezér Variedades")

# ============================================================
# 1) CONFIG
# ============================================================
# 👉 Troque pelo ID da sua planilha (o trecho entre /d/ e /edit na URL)
SHEET_ID = st.text_input(
    "Google Sheet ID",
    value="COLE_AQUI_O_ID_DA_SUA_PLANILHA",
    help="Ex.: em https://docs.google.com/spreadsheets/d/1AbC...XYZ/edit, o ID é '1AbC...XYZ'"
)

# Abas e colunas padrão
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

# Sugestões iniciais para Config (opcional)
CONFIG_INICIAIS = [
    ("taxa_cartao_padrao_pct", "0.023"),
    ("margem_alvo_padrao_pct", "0.35"),
    ("canal_padrao", "balcao"),
]

# ============================================================
# 2) Conexão com Google Sheets
# ============================================================
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
    # cria com linhas/colunas suficientes
    ws = planilha.add_worksheet(title=nome, rows=1000, cols=max(len(cols), 8))
    # escreve cabeçalhos
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

    # Se a primeira linha estiver vazia ou incompleta, escreve cabeçalhos
    primeira = dados[0] if len(dados) > 0 else []
    if all(c.strip() == "" for c in primeira) or len(primeira) < len(cols):
        ws.update("A1", [cols])
        return True

    return False

def garantir_config_inicial(ws):
    try:
        dados = ws.get_all_values()
    except Exception:
        dados = []

    linhas_existentes = set()
    for linha in dados[1:]:
        if linha and len(linha) >= 1:
            linhas_existentes.add(linha[0])

    novas = []
    for k, v in CONFIG_INICIAIS:
        if k not in linhas_existentes:
            novas.append([k, v])

    if novas:
        ws.append_rows(novas)

# ============================================================
# 3) UI
# ============================================================
if SHEET_ID and SHEET_ID != "COLE_AQUI_O_ID_DA_SUA_PLANILHA":
    st.info(f"📄 Planilha: https://docs.google.com/spreadsheets/d/{SHEET_ID}/edit")

opcoes = st.multiselect(
    "Quais abas você quer criar/verificar?",
    options=list(ABAS.keys()),
    default=list(ABAS.keys())
)

if st.button("🚀 Criar / Verificar Estrutura", type="primary", use_container_width=True):
    if not SHEET_ID or SHEET_ID == "COLE_AQUI_O_ID_DA_SUA_PLANILHA":
        st.error("Informe o **Google Sheet ID** antes de continuar.")
        st.stop()

    with st.spinner("Conectando ao Google Sheets..."):
        planilha = conectar_sheets(SHEET_ID)

    criadas = []
    atualizadas = []
    ja_ok = []

    for nome_aba in opcoes:
        cols = ABAS[nome_aba]
        ws = obter_aba(planilha, nome_aba)
        if ws is None:
            ws = criar_aba(planilha, nome_aba, cols)
            criadas.append(nome_aba)
        else:
            if garantir_cabecalhos(ws, cols):
                atualizadas.append(nome_aba)
            else:
                ja_ok.append(nome_aba)

        # Popular config inicial
        if nome_aba == "Config":
            garantir_config_inicial(ws)

    # Resultado
    st.success("✅ Estrutura conferida!")
    if criadas:
        st.write("🆕 Abas **criadas**:", ", ".join(criadas))
    if atualizadas:
        st.write("🔁 Abas com **cabeçalhos atualizados**:", ", ".join(atualizadas))
    if ja_ok:
        st.write("👌 Abas **já estavam corretas**:", ", ".join(ja_ok))

    st.caption("Pronto! Agora você pode começar a cadastrar produtos em **Produtos** e lançar **Compras**/**Vendas**.")

st.divider()
st.markdown("**Dica:** depois deste setup, crie as páginas `Produtos`, `Compras`, `Vendas` e o `Dashboard` para começar a operar o sistema.")
