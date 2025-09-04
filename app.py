# === EMERGÊNCIA: Criar abas/colunas direto no app.py ===
import streamlit as st
import gspread
from google.oauth2.service_account import Credentials

st.subheader("⚙️ Setup rápido da planilha (emergencial)")

SHEET_ID = st.text_input(
    "Google Sheet ID",
    value=st.secrets.get("SHEET_ID", ""),
    help="Cole apenas o ID (trecho entre /d/ e /edit)."
)

ABAS = {
    "Produtos": [
        "ID", "Nome", "Categoria", "Unidade", "Fornecedor",
        "CustoAtual", "PreçoVenda", "Markup %", "Margem %",
        "EstoqueAtual", "EstoqueMin", "LeadTimeDias", "Ativo?"
    ],
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

CONFIG_INICIAIS = [("taxa_cartao_padrao_pct","0.023"),("margem_alvo_padrao_pct","0.35"),("canal_padrao","balcao")]

def conectar_sheets(sheet_id: str):
    info = st.secrets.get("GCP_SERVICE_ACCOUNT", None)
    if not info:
        st.error("🚫 Falta st.secrets['GCP_SERVICE_ACCOUNT'].")
        st.stop()
    scopes = ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
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

if st.button("🚀 Criar / Verificar Estrutura AGORA", use_container_width=True):
    if not SHEET_ID:
        st.error("Cole o Google Sheet ID.")
    else:
        try:
            sh = conectar_sheets(SHEET_ID)
            criadas, atualizadas, ok = [], [], []
            for nome, cols in ABAS.items():
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
            if criadas: st.write("🆕 Criadas:", ", ".join(criadas))
            if atualizadas: st.write("🔁 Cabeçalhos atualizados:", ", ".join(atualizadas))
            if ok: st.write("👌 Já estavam corretas:", ", ".join(ok))
            st.info(f"Abra a planilha: https://docs.google.com/spreadsheets/d/{SHEET_ID}/edit")
        except Exception as e:
            st.error(f"❌ Erro: {e}")
# === FIM DO BLOCO EMERGENCIAL ===
