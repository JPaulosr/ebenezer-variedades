# pages/Contagem_Estoque.py — Definir nível de estoque (com delta automático)
# -*- coding: utf-8 -*-

import json, re, unicodedata as _ud
from datetime import datetime
import streamlit as st
import pandas as pd
import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from google.oauth2.service_account import Credentials

st.set_page_config(page_title="Contagem de estoque (definir nível)", page_icon="📋", layout="wide")
st.title("📋 Contagem de estoque (definir nível)")

# ======================================================
# Helpers de acesso (mesmo padrão do 01_produtos.py)
# ======================================================
def _normalize_private_key(key: str) -> str:
    if not isinstance(key, str): return key
    key = key.replace("\\n", "\n")
    key = "".join(ch for ch in key if _ud.category(ch)[0] != "C" or ch in ("\n","\r","\t"))
    return key

def _load_sa():
    svc = st.secrets.get("GCP_SERVICE_ACCOUNT")
    if svc is None:
        st.error("🛑 GCP_SERVICE_ACCOUNT ausente."); st.stop()
    if isinstance(svc, str): svc = json.loads(svc)
    svc = dict(svc); svc["private_key"] = _normalize_private_key(svc["private_key"])
    return svc

@st.cache_resource
def _sheet():
    scopes = ["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(_load_sa(), scopes=scopes)
    gc = gspread.authorize(creds)
    url_or_id = st.secrets.get("PLANILHA_URL")
    if not url_or_id:
        st.error("🛑 PLANILHA_URL ausente."); st.stop()
    return gc.open_by_url(url_or_id) if str(url_or_id).startswith("http") else gc.open_by_key(url_or_id)

@st.cache_data(ttl=10, show_spinner=False)
def carregar_aba(nome_aba: str) -> pd.DataFrame:
    ws = _sheet().worksheet(nome_aba)
    df = get_as_dataframe(ws, evaluate_formulas=True, dtype=str, header=0).dropna(how="all")
    df.columns = [c.strip() for c in df.columns]
    return df.fillna("")

def _first_col(df: pd.DataFrame, cands: list[str]) -> str | None:
    for c in cands:
        if c in df.columns: return c
    lower = {c.lower(): c for c in df.columns}
    for c in cands:
        if c.lower() in lower: return lower[c.lower()]
    return None

# ======================================================
# Helpers de normalização (idênticos ao 01_produtos.py)
# ======================================================
def _strip_accents_low(s: str) -> str:
    s = _ud.normalize("NFKD", str(s or ""))
    s = "".join(ch for ch in s if _ud.category(ch) != "Mn")
    return s.lower().strip()

def _to_num(x) -> float:
    """Converte string/num para float preservando sinal negativo.
       Suporta formatos: -6, -6,0, (6), 1.234,56, 'R$ -1.234,56' e '−6' (unicode minus)."""
    if x is None: 
        return 0.0
    s = str(x).strip()
    if s == "" or s.lower() in ("nan","none"):
        return 0.0

    # Normaliza variações de menos e parênteses negativos
    s = s.replace("−", "-")  # unicode minus -> ascii
    neg_paren = False
    if s.startswith("(") and s.endswith(")"):
        s = s[1:-1]
        neg_paren = True

    # Remove símbolos e espaços; trata separadores BR/US
    s = s.replace("R$", "").replace(" ", "")
    # Se tiver vírgula (padrão BR), remove pontos de milhar e troca vírgula por ponto
    if "," in s:
        s = s.replace(".", "")
        s = s.replace(",", ".")
    # Mantém apenas dígitos, 1 ponto decimal e um '-' no início
    # Remove sinais '-' que não estejam na posição inicial
    s = re.sub(r"(?<!^)-", "", s)
    s = re.sub(r"[^0-9\.\-]", "", s)
    # Garante apenas um '-'
    if s.count("-") > 1:
        s = "-" + s.replace("-", "")
    # Conserta múltiplos pontos decimais (mantém o último como decimal)
    if s.count(".") > 1:
        parts = s.split(".")
        s = "".join(parts[:-1]) + "." + parts[-1]

    try:
        v = float(s)
    except:
        v = 0.0

    if neg_paren:
        v = -abs(v)
    return v

def _norm_tipo(t: str) -> str:
    """
    Normaliza tipos de movimento:
      - 'entrada' (compra, estorno, fracionamento +)
      - 'saida'   (venda, baixa, fracionamento -)
      - 'ajuste'  (ajuste, contagem/inventário)
    """
    raw = str(t or "")
    low = _strip_accents_low(raw)
    if "fracion" in low:
        if "+" in raw: return "entrada"
        if "-" in raw: return "saida"
        return "outro"
    lowc = re.sub(r"[^a-z]", "", low)
    if "contagem" in lowc or "inventario" in lowc:  # 👈 contagem é ajuste
        return "ajuste"
    if "entrada" in lowc or "compra" in lowc or "estorno" in lowc:
        return "entrada"
    if "saida"   in lowc or "venda"  in lowc or "baixa"   in lowc:
        return "saida"
    if "ajuste"  in lowc:
        return "ajuste"
    return "outro"

def _nz(x):
    if x is None: return ""
    try:
        if pd.isna(x): return ""
    except: 
        pass
    s = str(x).strip()
    return "" if s.lower() in ("nan","none") else s

def _prod_key_from(prod_id, prod_nome):
    pid = _nz(prod_id)
    if pid: return pid
    return f"nm:{_strip_accents_low(_nz(prod_nome))}"

# ======================================================
# Constantes de abas
# ======================================================
ABA_PRODUTOS = "Produtos"
ABA_MOV      = "MovimentosEstoque"

# ======================================================
# Carregamento bases
# ======================================================
try:
    df_prod = carregar_aba(ABA_PRODUTOS)
except Exception as e:
    st.error("Erro ao abrir a aba Produtos."); st.code(str(e)); st.stop()

try:
    df_mov = carregar_aba(ABA_MOV)
except Exception:
    df_mov = pd.DataFrame(columns=["Data","IDProduto","Produto","Tipo","Qtd","Obs"])

# Colunas de interesse
col_id   = _first_col(df_prod, ["ID","Id","Codigo","Código","SKU"])
col_nome = _first_col(df_prod, ["Nome","Produto","Descrição","Descricao"])

# Pré-processamento dos movimentos
for c in ["Tipo","Qtd","IDProduto","Produto"]:
    if c not in df_mov.columns: df_mov[c] = ""

if not df_mov.empty:
    df_mov["Tipo_norm"] = df_mov["Tipo"].apply(_norm_tipo)
    df_mov["Qtd_num"]   = df_mov["Qtd"].map(_to_num)   # 👈 agora preserva negativos
    df_mov["__key"]     = df_mov.apply(lambda r: _prod_key_from(r.get("IDProduto",""), r.get("Produto","")), axis=1)
else:
    df_mov["Tipo_norm"] = []
    df_mov["Qtd_num"]   = []
    df_mov["__key"]     = []

def estoque_atual_chave(ch) -> float:
    if df_mov.empty: return 0.0
    g = df_mov[df_mov["__key"] == ch].groupby("Tipo_norm")["Qtd_num"].sum()
    return float(g.get("entrada",0.0) - g.get("saida",0.0) + g.get("ajuste",0.0))

# ======================================================
# UI — seleção de produto
# ======================================================
st.markdown("Selecione um produto e **defina o nível** desejado; o sistema grava **1 ajuste** com o delta necessário.")

# Opções de produto (mostrar "ID — Nome" quando existir ID)
df_prod["_id"]   = df_prod[col_id]   if col_id else ""
df_prod["_nome"] = df_prod[col_nome] if col_nome else ""
df_prod["__key"] = df_prod.apply(lambda r: _prod_key_from(r.get(col_id,""), r.get(col_nome,"")), axis=1)
df_prod["__label"] = df_prod.apply(
    lambda r: f"{r['_id']} — {r['_nome']}" if r["_id"] else str(r["_nome"]),
    axis=1
)

opt = st.selectbox(
    "Produto",
    options=df_prod["__key"],
    format_func=lambda k: df_prod.loc[df_prod["__key"]==k, "__label"].iloc[0] if (df_prod["__key"]==k).any() else k,
    index=0 if not df_prod.empty else None
)

row = df_prod[df_prod["__key"]==opt].iloc[0] if not df_prod.empty else None
prod_id   = row.get("_id","")
prod_nome = row.get("_nome","")

# Estoque atual real (Entradas − Saídas ± Ajustes)
estoque_atual = estoque_atual_chave(opt)

st.info(f"**Estoque atual (calculado): {int(estoque_atual) if float(estoque_atual).is_integer() else estoque_atual}**", icon="ℹ️")

c1, c2 = st.columns([1,1])
with c1:
    alvo = st.number_input("Definir estoque para", min_value=0.0, step=1.0, value=float(estoque_atual))
with c2:
    motivo = st.text_input("Motivo", value="Contagem")

responsavel = st.text_input("Responsável", value="")
obs = st.text_area("Observações (opcional)", value="")

delta = alvo - estoque_atual

st.caption(f"Δ (delta) que será registrado como **Ajuste**: **{delta:.0f}**" if float(delta).is_integer() else f"Δ (delta): **{delta}**")

# ======================================================
# Persistência no MovimentosEstoque
# ======================================================
def _ws_mov():
    return _sheet().worksheet(ABA_MOV)

def _ensure_headers(ws, headers: list[str]):
    try:
        cur = ws.row_values(1)
    except Exception:
        cur = []
    if not cur:
        ws.update("A1", [headers])
        return headers
    return cur

def _append_movimento(data_str, id_prod, nome_prod, tipo, qtd_str, obs_str):
    ws = _ws_mov()
    headers = _ensure_headers(ws, ["Data","IDProduto","Produto","Tipo","Qtd","Obs"])
    row_map = {
        "Data": data_str,
        "IDProduto": id_prod,
        "Produto": nome_prod,
        "Tipo": tipo,
        "Qtd": qtd_str,
        "Obs": obs_str,
    }
    linha = [row_map.get(h, "") for h in headers]
    ws.append_row(linha, value_input_option="USER_ENTERED")

# ======================================================
# Ação
# ======================================================
btn = st.button("💾 Salvar contagem", type="primary", use_container_width=True)

if btn:
    if row is None:
        st.error("Selecione um produto válido.")
        st.stop()

    if delta == 0:
        st.success("Nada a fazer: o estoque já está no valor desejado.")
        st.stop()

    data_str = datetime.now().strftime("%d/%m/%Y")
    qtd_str  = str(delta).replace(".", ",")  # mantém padrão PT-BR

    # Sempre grava como AJUSTE (classe correta para contagem/inventário)
    obs_final = f"{motivo or 'Contagem'} por {responsavel}".strip()
    if obs:
        obs_final = (obs_final + " — " + obs) if obs_final else obs

    try:
        _append_movimento(data_str, prod_id, prod_nome, "Ajuste", qtd_str, obs_final)
        st.success(
            f"Ajuste gravado com sucesso! Δ = {delta:.0f} → estoque esperado = {alvo:.0f}"
            if float(delta).is_integer() else
            f"Ajuste gravado! Δ = {delta} → estoque esperado = {alvo}"
        )
        st.session_state["_force_refresh"] = True
        st.cache_data.clear()
        # st.rerun()  # habilite se quiser recarregar imediatamente
    except Exception as e:
        st.error("Falha ao gravar ajuste na aba MovimentosEstoque.")
        st.code(str(e))
