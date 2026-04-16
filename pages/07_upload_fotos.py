# pages/upload_fotos.py
# -*- coding: utf-8 -*-
import json, re, unicodedata as _ud
from typing import Optional

import streamlit as st
import pandas as pd
import gspread
from gspread_dataframe import get_as_dataframe
from google.oauth2.service_account import Credentials

# >>> Cloudinary (SDK oficial, assinatura automática)
import cloudinary
import cloudinary.uploader
import cloudinary.api

st.set_page_config(page_title="Upload de Fotos (Produtos)", page_icon="🖼️", layout="wide")
st.title("🖼️ Upload/URL de Foto para Produtos")

# ======================================================================
# Config / Conexão
# ──────────────────────────────────────────────
#  CONEXÃO / HELPERS  (centralizados em utils/sheets.py)
# ──────────────────────────────────────────────
from utils.sheets import (
    sheet, carregar_aba, garantir_aba, append_rows,
    to_num, brl, safe_cost, first_col, fmt_num,
    norm_tipo_mov, calcular_estoque, tg_send, tg_media, gerar_id, parse_date,
    ABA_PROD, ABA_VEND, ABA_COMP, ABA_MOVS, ABA_CLIEN, ABA_FIADO, ABA_FPAGT,
)
# Aliases de compatibilidade
_to_num = to_num; _to_float = to_num; _brl = brl; _fmt_brl = brl
_first_col = first_col; _fmt_num = fmt_num; _parse_date_any = parse_date
_tg_send = tg_send; _tg_media = tg_media; _norm_tipo_mov = norm_tipo_mov
_gerar_id = gerar_id; _parse_date = parse_date; _norm_tipo = norm_tipo_mov
def _canon_id(x):
    import re as _re; return _re.sub(r"[^0-9]", "", str(x or ""))
def conectar_sheets(): return sheet()

ABA_PRODUTOS = "Produtos"  # nome da sua aba de catálogo

def _sheet():
    return sheet()

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
# Cloudinary (SDK) — mesma lógica do arquivo que funciona
# ======================================================================
def _cloud_cfg():
    cfg = st.secrets.get("CLOUDINARY", {}) or {}
    if not (cfg.get("cloud_name") and cfg.get("api_key") and cfg.get("api_secret")):
        return None
    cloudinary.config(
        cloud_name = cfg["cloud_name"],
        api_key    = cfg["api_key"],
        api_secret = cfg["api_secret"],
        secure     = True,
    )
    pasta = (cfg.get("folder") or "Produtos").strip()  # pode ter acento/espaço
    return {"folder": pasta}

def _slug(s: str) -> str:
    if not s: return "produto"
    s = "".join(c for c in _ud.normalize("NFKD", s) if not _ud.combining(c))
    s = re.sub(r"[^A-Za-z0-9\-_\.]+", "_", s).strip("_").lower()
    return s or "produto"

def _nrm_name(s: str) -> str:
    return str(s or "").strip()

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
    format_func=lambda i: _nrm_name(dfp.loc[i, col_nome]) or "(sem nome)",
)
nome_prod = _nrm_name(dfp.loc[sel_idx, col_nome])
sku = _nrm_name(dfp.loc[sel_idx, col_id]) if col_id else ""

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
# Upload do computador → Cloudinary (SDK) e salvar URL
# ======================================================================
st.divider()
st.subheader("Ou envie um arquivo do computador (Cloudinary)")

cfg = _cloud_cfg()
if not cfg:
    with st.expander("Configurar Cloudinary (clique para ver)"):
        st.markdown(
            """
            Adicione aos **Secrets**:
            ```toml
            [CLOUDINARY]
            cloud_name = "SEU_CLOUD_NAME"
            api_key    = "SUA_API_KEY"
            api_secret = "SUA_API_SECRET"
            folder     = "Ebenézer Variedades"  # pode ter espaço/acentos
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
        folder = st.text_input("Pasta", value=cfg["folder"])

    # Verifica se já existe imagem no Cloudinary ou link na planilha
    pid_path = f"{folder}/{public_id}".strip("/")
    existe_url = None
    try:
        r = cloudinary.api.resource(pid_path)
        existe_url = r.get("secure_url")
    except Exception:
        # tenta ler o que está na planilha (se houver coluna Foto)
        try:
            ws = _sheet().worksheet(ABA_PRODUTOS)
            hdrs = _headers(ws)
            foto_idx = _find_col(hdrs, ["Foto","FotoURL","Imagem","Image","Link","UrlFoto","URL_Foto"])
            if foto_idx:
                # dfp é cacheado — pegamos da mesma linha/coluna
                existe_url = str(dfp.iloc[sel_idx, foto_idx-1]).strip()
        except Exception:
            pass

    if existe_url:
        st.image(existe_url, width=220, caption=f"Imagem atual — {nome_prod}")
        st.warning("Este produto já possui imagem. Marque a caixa abaixo para substituir.")
        must_confirm = not st.checkbox("Confirmo que desejo substituir a imagem existente.")
    else:
        st.info("Este produto ainda não possui imagem cadastrada.")
        must_confirm = False

    colA, colB = st.columns([0.55, 0.45])
    with colA:
        if st.button("↑ Enviar para Cloudinary e salvar na planilha", type="primary", use_container_width=True, disabled=must_confirm):
            if not arquivo:
                st.warning("Selecione um arquivo para enviar.")
            else:
                try:
                    with st.spinner("Enviando para o Cloudinary…"):
                        up = cloudinary.uploader.upload(
                            arquivo,                     # o SDK aceita o file-like
                            folder=folder,               # pode ter acento/espaço
                            public_id=public_id,
                            overwrite=True,
                            invalidate=True,
                            resource_type="image",
                        )
                    secure_url = up.get("secure_url")
                    if not secure_url:
                        raise RuntimeError(f"Resposta sem URL: {up}")

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
                except Exception as e:
                    st.error(f"Erro ao enviar/salvar imagem: {e}")

    with colB:
        if existe_url and st.button("🗑️ Deletar imagem", use_container_width=True):
            try:
                try:
                    cloudinary.uploader.destroy(pid_path, resource_type="image")
                    st.success("Imagem deletada do Cloudinary.")
                except Exception:
                    pass
                # limpar link na planilha
                sh = _sheet()
                ws = sh.worksheet(ABA_PRODUTOS)
                _, foto_col = _ensure_foto_col(ws)
                target_row = int(sel_idx) + 2
                ws.update_cell(target_row, foto_col, "")
                st.success("Link removido da planilha.")
                st.rerun()
            except Exception as e:
                st.error(f"Erro ao deletar imagem: {e}")

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
        # dfp foi carregado antes; pode estar desatualizado se acabou de salvar.
        # Em um app real, você pode recarregar; aqui só tentamos exibir.
        try:
            foto_url_salva = dfp.iloc[sel_idx, foto_idx-1]
        except Exception:
            foto_url_salva = ""
        if str(foto_url_salva).strip():
            st.image(str(foto_url_salva).strip(), width=preview_size, caption=f"{nome_prod} (salvo)")
        else:
            st.info("Este produto ainda não tem foto salva.")
    else:
        st.info("A planilha ainda não tem a coluna **Foto**.")
except Exception:
    st.info("Não consegui carregar a foto salva agora, mas a URL foi gravada.")

# ======================================================================
# Galeria (opcional) — mostra miniaturas dos produtos com foto
# ======================================================================
st.markdown("---")
st.subheader("🖼️ Galeria rápida (Produtos com foto)")

cols = st.columns(6)
i = 0
try:
    ws = _sheet().worksheet(ABA_PRODUTOS)
    hdrs = _headers(ws)
    foto_idx = _find_col(hdrs, ["Foto","FotoURL","Imagem","Image","Link","UrlFoto","URL_Foto"])
    if foto_idx:
        for i_row in range(len(dfp)):
            nome = _nrm_name(dfp.loc[i_row, col_nome])
            url_foto = str(dfp.iloc[i_row, foto_idx-1]).strip()
            if url_foto:
                with cols[i % 6]:
                    st.image(url_foto, width=110, caption=nome)
                i += 1
    if i == 0:
        st.caption("Sem fotos salvas na planilha ainda.")
except Exception:
    st.caption("Não foi possível carregar a galeria agora.")
