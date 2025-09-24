# pages/upload_fotos.py
# -*- coding: utf-8 -*-
import json, time, hashlib, re
import unicodedata as _ud
from typing import Optional

import streamlit as st
import pandas as pd
import gspread, requests
from gspread_dataframe import get_as_dataframe
from google.oauth2.service_account import Credentials

st.set_page_config(page_title="Upload de Fotos (Produtos)", page_icon="🖼️", layout="wide")
st.title("🖼️ Upload/URL de Foto para Produtos")

# ======================================================================
# Config / Conexão
# ======================================================================
ABA_PRODUTOS = "Produtos"  # nome da sua aba de catálogo

def _normalize_private_key(key: str) -> str:
    if not isinstance(key, str): return key
    key = key.replace("\\n", "\n")
    return "".join(ch for ch in key if _ud.category(ch)[0] != "C" or ch in ("\n","\r","\t"))

def _load_sa():
    svc = st.secrets.get("GCP_SERVICE_ACCOUNT")
    if svc is None:
        st.error("🛑 Falta o secret GCP_SERVICE_ACCOUNT.")
        st.stop()
    if isinstance(svc, str):
        svc = json.loads(svc)
    svc = dict(svc)
    svc["private_key"] = _normalize_private_key(svc["private_key"])
    return svc

@st.cache_resource
def _sheet():
    scopes = ["https://www.googleapis.com/auth/spreadsheets",
              "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(_load_sa(), scopes=scopes)
    gc = gspread.authorize(creds)
    url_or_id = st.secrets.get("PLANILHA_URL") or st.secrets.get("PLANILHA_ID")
    if not url_or_id:
        st.error("🛑 Coloque PLANILHA_URL ou PLANILHA_ID nos Secrets.")
        st.stop()
    return gc.open_by_url(url_or_id) if str(url_or_id).startswith("http") else gc.open_by_key(url_or_id)

def _headers(ws) -> list[str]:
    try:
        return [h.strip() for h in ws.row_values(1)]
    except Exception:
        return []

def _find_col(headers: list[str], candidates: list[str]) -> Optional[int]:
    """retorna índice 1-based da primeira coluna que casar (case-insensitive)."""
    if not headers: return None
    lower = {h.lower(): i+1 for i, h in enumerate(headers)}
    for c in candidates:
        if c.lower() in lower:
            return lower[c.lower()]
    return None

def _ensure_foto_col(ws) -> tuple[str, int]:
    """Garante que exista uma coluna para foto. Retorna (nome, índice 1-based)."""
    hdrs = _headers(ws)
    foto_idx = _find_col(hdrs, ["Foto","FotoURL","Imagem","Image","Link","UrlFoto","URL_Foto"])
    if foto_idx:
        return hdrs[foto_idx-1], foto_idx
    # cria uma nova no fim chamada "Foto"
    new_idx = len(hdrs) + 1 if hdrs else 1
    ws.update_cell(1, new_idx, "Foto")
    return "Foto", new_idx

@st.cache_data(ttl=20, show_spinner=False)
def carregar_produtos() -> pd.DataFrame:
    ws = _sheet().worksheet(ABA_PRODUTOS)
    df = get_as_dataframe(ws, evaluate_formulas=True, dtype=str, header=0).dropna(how="all")
    df.columns = [c.strip() for c in df.columns]
    return df.fillna("")

# ======================================================================
# Cloudinary helpers (upload assinado sem SDK)
# ======================================================================
def _cloudinary_conf() -> dict:
    cfg = st.secrets.get("CLOUDINARY", {})
    return {
        "cloud_name": (cfg.get("cloud_name") or "").strip(),
        "api_key":    (cfg.get("api_key") or "").strip(),
        "api_secret": (cfg.get("api_secret") or "").strip(),
        "folder":     (cfg.get("folder") or "Produtos").strip(),
    } if cfg else {}

def _slug(s: str) -> str:
    if not s: return "produto"
    s = "".join(c for c in _ud.normalize("NFKD", s) if not _ud.combining(c))
    s = re.sub(r"[^A-Za-z0-9\-_\.]+", "_", s).strip("_")
    return s or "produto"

def _nfc(s: str) -> str:
    """Normaliza para NFC e tira espaços nas pontas (evita diferenças invisíveis)."""
    if s is None: return ""
    return _ud.normalize("NFC", str(s)).strip()

def _sign_cloudinary(params: dict, api_secret: str) -> tuple[str, str]:
    """
    Assina exatamente os params enviados (alfabético; sem file/api_key/signature).
    Retorna (string_to_sign, signature_hex).
    """
    filtered = {
        k: v for k, v in params.items()
        if v not in (None, "", []) and k not in ("file", "api_key", "signature")
    }
    parts = [f"{k}={filtered[k]}" for k in sorted(filtered.keys())]  # ordem alfabética por chave
    string_to_sign = "&".join(parts)
    signature = hashlib.sha1((string_to_sign + api_secret).encode("utf-8")).hexdigest()
    return string_to_sign, signature

def _cloudinary_upload(file_bytes: bytes, filename: str, *, folder: str, public_id: str,
                       overwrite: bool = True, invalidate: bool = True, debug: bool = True) -> dict:
    """
    Upload assinado para Cloudinary, incluindo overwrite/invalidate na assinatura.
    Se debug=True, mostra string_to_sign e assinatura local para comparação.
    """
    conf = _cloudinary_conf()
    cloud = (conf.get("cloud_name") or "").strip()
    key   = (conf.get("api_key") or "").strip()
    secret= (conf.get("api_secret") or "").strip()
    if not (cloud and key and secret):
        raise RuntimeError("Config do Cloudinary ausente/incompleta em st.secrets['CLOUDINARY'].")

    # Normaliza entradas (acentos/espacos invisíveis)
    folder    = _nfc(folder)
    public_id = _nfc(public_id)

    url = f"https://api.cloudinary.com/v1_1/{cloud}/image/upload"
    ts = str(int(time.time()))

    # Os mesmos parâmetros que iremos enviar no body precisam ser assinados
    params = {
        "folder": folder,
        "public_id": public_id,
        "timestamp": ts,
        "overwrite": "true" if overwrite else "false",
        "invalidate": "true" if invalidate else "false",
    }

    string_to_sign, signature = _sign_cloudinary(params, secret)

    if debug:
        with st.expander("🛠️ Diagnóstico da assinatura (local)"):
            st.write("string_to_sign enviada:")
            st.code(string_to_sign, language="text")
            st.write("signature (sha1):")
            st.code(signature, language="text")
            st.caption("Compare a string acima com a 'String to sign' retornada pelo erro do Cloudinary. "
                       "Se forem idênticas e ainda der 'Invalid Signature', a causa é API key/secret incorretas "
                       "ou secrets antigos em execução (reinicie o app).")

    files = {"file": (filename, file_bytes)}
    data = dict(params)
    data.update({"api_key": key, "signature": signature})

    r = requests.post(url, files=files, data=data, timeout=30)
    r.raise_for_status()
    return r.json()

# ======================================================================
# UI — escolher produto (mostrar só o NOME)
# ======================================================================
dfp = carregar_produtos()
if dfp.empty:
    st.info("Nenhum produto cadastrado na aba **Produtos**.")
    st.stop()

# tenta adivinhar a coluna de nome e a de ID
NOME_CANDS = ["Nome", "Produto", "Descrição", "Descricao", "Título", "Titulo"]
ID_CANDS   = ["ID", "Sku", "SKU", "Codigo", "Código"]
col_nome = next((c for c in NOME_CANDS if c in dfp.columns), None)
col_id   = next((c for c in ID_CANDS if c in dfp.columns), None)
if not col_nome:
    st.error("Não encontrei uma coluna de nome (ex.: Nome/Produto/Descrição) na aba Produtos.")
    st.stop()

st.subheader("Selecione o produto")
opcoes = dfp.index.tolist()
sel_idx = st.selectbox(
    "Produto",
    options=opcoes,
    format_func=lambda i: str(dfp.loc[i, col_nome] or "(sem nome)"),
)
nome_prod = str(dfp.loc[sel_idx, col_nome] or "").strip()
sku = str(dfp.loc[sel_idx, col_id]).strip() if col_id else ""

# ======================================================================
# URL manual de imagem
# ======================================================================
st.divider()
st.subheader("Colar URL da imagem")
url = st.text_input("URL da imagem (https…)", value="", placeholder="https://…")
preview_size = st.slider("Tamanho da prévia (px)", 120, 600, 320, 10)

col1, col2 = st.columns([0.5, 0.5])
with col1:
    if st.button("💾 Salvar URL manual no catálogo", type="primary"):
        if not url.strip():
            st.warning("Cole uma URL primeiro.")
        else:
            try:
                sh = _sheet()
                ws = sh.worksheet(ABA_PRODUTOS)
                _, foto_col = _ensure_foto_col(ws)
                target_row = int(sel_idx) + 2  # DF 0-based; planilha começa na 2
                ws.update_cell(target_row, foto_col, url.strip())
                st.success("✅ URL salva no catálogo!")
                st.session_state["_force_refresh"] = True
            except Exception as e:
                st.error(f"Erro ao salvar: {e}")

with col2:
    if url.strip():
        st.image(url.strip(), width=preview_size, caption=nome_prod)

st.caption("A URL fica registrada na coluna **Foto** da aba Produtos. O arquivo continua hospedado no endereço da URL.")

# ======================================================================
# Upload do computador → Cloudinary (e salvar URL)
# ======================================================================
st.divider()
st.subheader("Ou envie um arquivo do computador (Cloudinary)")

conf = _cloudinary_conf()
if not conf:
    with st.expander("Configurar Cloudinary (clique para ver)"):
        st.markdown(
            """
            Adicione aos **Secrets**:
            ```toml
            [CLOUDINARY]
            cloud_name = "SEU_CLOUD_NAME"
            api_key    = "SUA_API_KEY"
            api_secret = "SUA_API_SECRET"
            folder     = "Ebenézer Variedades"
            ```
            """
        )
    st.warning("Configure o Cloudinary nos Secrets para habilitar o upload.")
else:
    up_col1, up_col2, up_col3 = st.columns([0.45, 0.35, 0.20])
    with up_col1:
        arquivo = st.file_uploader("Selecionar imagem", type=["jpg","jpeg","png","webp","gif"], accept_multiple_files=False)
    with up_col2:
        default_pid = _slug(sku or nome_prod)
        public_id = st.text_input("Nome do arquivo no Cloudinary (public_id)", value=default_pid, help="Sem extensão.")
    with up_col3:
        folder = st.text_input("Pasta", value=conf.get("folder") or "Produtos")
    overwrite = st.checkbox("Substituir se já existir (overwrite)", value=True)
    invalidate = st.checkbox("Invalidar CDN após overwrite (invalidate)", value=True)

    if st.button("↑ Enviar para Cloudinary e salvar na planilha", type="primary", use_container_width=True):
        if not arquivo:
            st.warning("Selecione um arquivo para enviar.")
        else:
            try:
                with st.spinner("Enviando para o Cloudinary…"):
                    data = _cloudinary_upload(
                        file_bytes=arquivo.read(),
                        filename=arquivo.name,
                        folder=folder,
                        public_id=public_id,
                        overwrite=overwrite,
                        invalidate=invalidate,
                        debug=True,  # mostra a string_to_sign local
                    )
                secure_url = data.get("secure_url") or data.get("url")
                if not secure_url:
                    raise RuntimeError(f"Resposta sem URL: {data}")

                # salva URL na aba Produtos
                sh = _sheet()
                ws = sh.worksheet(ABA_PRODUTOS)
                _, foto_col = _ensure_foto_col(ws)
                target_row = int(sel_idx) + 2
                ws.update_cell(target_row, foto_col, secure_url)

                st.success("✅ Upload concluído e URL salva no catálogo!")
                st.image(secure_url, width=preview_size, caption=f"{nome_prod} (Cloudinary)")
                st.code(secure_url, language="text")
                st.session_state["_force_refresh"] = True
            except requests.HTTPError as he:
                try:
                    err = he.response.json()
                except Exception:
                    err = he.response.text
                st.error(f"Erro HTTP no upload: {err}")
            except Exception as e:
                st.error(f"Erro no upload/salvamento: {e}")

# ======================================================================
# Preview do que está salvo (se já houver)
# ======================================================================
st.divider()
st.subheader("Pré-visualização do que está salvo")
try:
    ws = _sheet().worksheet(ABA_PRODUTOS)
    hdrs = _headers(ws)
    foto_idx = _find_col(hdrs, ["Foto","FotoURL","Imagem","Image","Link","UrlFoto","URL_Foto"])
    if foto_idx:
        foto_url_salva = dfp.iloc[sel_idx, foto_idx-1] if (foto_idx-1) < len(dfp.columns) else ""
        if str(foto_url_salva).strip():
            st.image(str(foto_url_salva).strip(), width=preview_size, caption=f"{nome_prod} (salvo)")
        else:
            st.info("Este produto ainda não tem foto salva.")
    else:
        st.info("A planilha ainda não tem a coluna **Foto**.")
except Exception:
    st.info("Não consegui carregar a foto salva agora, mas a URL foi gravada.")
