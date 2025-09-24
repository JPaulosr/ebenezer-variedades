# pages/02_upload_fotos.py — Upload de fotos e salvar no catálogo (aba Produtos)
# -*- coding: utf-8 -*-
import io, json, mimetypes, re, unicodedata as _ud
import streamlit as st
import pandas as pd
import gspread
from gspread_dataframe import get_as_dataframe
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload

st.set_page_config(page_title="Upload de Fotos de Produtos", page_icon="📸", layout="wide")
st.title("📸 Upload de Fotos — salvar no catálogo")

# =============== helpers de auth/planilha/drive ===============
def _normalize_private_key(key: str) -> str:
    if not isinstance(key, str):
        return key
    key = key.replace("\\n", "\n")
    key = "".join(ch for ch in key if _ud.category(ch)[0] != "C" or ch in ("\n", "\r", "\t"))
    return key

def _load_sa():
    svc = st.secrets.get("GCP_SERVICE_ACCOUNT")
    if svc is None:
        st.error("🛑 Falta secret GCP_SERVICE_ACCOUNT."); st.stop()
    if isinstance(svc, str):
        svc = json.loads(svc)
    svc = dict(svc)
    svc["private_key"] = _normalize_private_key(svc["private_key"])
    return svc

@st.cache_resource
def _sheet():
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(_load_sa(), scopes=scopes)
    gc = gspread.authorize(creds)
    url_or_id = st.secrets.get("PLANILHA_URL")
    if not url_or_id:
        st.error("🛑 Falta secret PLANILHA_URL (pode ser a URL completa ou só o ID)."); st.stop()
    return gc.open_by_url(url_or_id) if str(url_or_id).startswith("http") else gc.open_by_key(url_or_id)

@st.cache_resource
def _drive():
    creds = Credentials.from_service_account_info(_load_sa(), scopes=["https://www.googleapis.com/auth/drive"])
    return build("drive", "v3", credentials=creds)

def _first_col(df: pd.DataFrame, cands: list[str]) -> str | None:
    for c in cands:
        if c in df.columns:
            return c
    lower = {c.lower(): c for c in df.columns}
    for c in cands:
        if c.lower() in lower:
            return lower[c.lower()]
    return None

# =============== carregar catálogo de produtos ===============
ABA_PRODUTOS = "Produtos"  # ajuste se tiver outro nome
try:
    ws = _sheet().worksheet(ABA_PRODUTOS)
    dfp = get_as_dataframe(ws, evaluate_formulas=True, dtype=str, header=0).fillna("")
    dfp.columns = [c.strip() for c in dfp.columns]
except Exception as e:
    st.error("Não consegui abrir a aba Produtos."); st.code(str(e)); st.stop()

col_id   = _first_col(dfp, ["ID","Id","Codigo","Código","SKU"])
col_nome = _first_col(dfp, ["Nome","Produto","Descrição","Descricao"])
col_foto = _first_col(dfp, ["Foto","FotoURL","Imagem","Image","LinkFoto","URLFoto","URL_Foto","foto","foto_url","img","photo"])

if not col_nome:
    st.error("Aba **Produtos** precisa ter pelo menos a coluna de nome (ex.: Nome/Produto).")
    st.stop()

# =============== UI: escolher produto ===============
lista = dfp[[c for c in [col_id, col_nome] if c]].copy()
lista["__label"] = lista.apply(lambda r: f"{(r.get(col_id) or '—')} — {r.get(col_nome)}", axis=1)
sel = st.selectbox("Produto", lista["__label"].tolist())

if not sel:
    st.stop()

row = lista[lista["__label"] == sel].iloc[0]
pid = str(row.get(col_id, "") or "").strip()
pnm = str(row.get(col_nome, "") or "").strip()

# foto atual
foto_atual = ""
if col_foto and col_foto in dfp.columns:
    mask = (dfp[col_id].astype(str).str.strip() == pid) if (col_id and pid) else (dfp[col_nome].astype(str).str.strip() == pnm)
    if mask.any():
        foto_atual = str(dfp.loc[mask, col_foto].iloc[0] or "").strip()

col_prev, col_form = st.columns([0.35, 0.65], vertical_alignment="top")
with col_prev:
    st.markdown("**Prévia atual**")
    if foto_atual:
        st.image(foto_atual, use_container_width=True)
        st.code(foto_atual, language="text")
    else:
        st.info("Sem foto salva ainda.")

with col_form:
    st.subheader("Enviar nova imagem")
    up = st.file_uploader("Selecione a imagem (JPG/PNG/WebP)", type=["jpg","jpeg","png","webp"], key="upfoto")
    st.caption("Dica: use imagens leves (até ~1MB).")

    st.markdown("Ou cole uma URL de imagem (Drive, Imgur, etc.)")
    url_manual = st.text_input("URL direta da foto", placeholder="https://...")

    # pasta do Drive para upload
    FOLDER_ID = st.secrets.get("DRIVE_FOTOS_FOLDER_ID", "").strip()
    if up:
        if not FOLDER_ID:
            st.error("Falta secret DRIVE_FOTOS_FOLDER_ID (ID da pasta no Drive que já está compartilhada com o service account).")
        else:
            if st.button("📤 Enviar para o Drive e salvar no catálogo", type="primary", key="btn_upload"):
                try:
                    drive = _drive()
                    data = up.read()
                    fname = up.name
                    mime  = mimetypes.guess_type(fname)[0] or "application/octet-stream"

                    media = MediaIoBaseUpload(io.BytesIO(data), mimetype=mime, resumable=False)
                    meta  = {"name": fname, "parents": [FOLDER_ID]}
                    created = drive.files().create(body=meta, media_body=media, fields="id").execute()
                    file_id = created["id"]

                    # deixa público por link (leitura)
                    try:
                        drive.permissions().create(
                            fileId=file_id, body={"role":"reader","type":"anyone"}
                        ).execute()
                    except Exception:
                        pass  # se já estiver herdando permissões, segue

                    direct_url = f"https://drive.google.com/uc?id={file_id}"

                    # cria coluna de foto se não existe
                    headers = [h.strip() for h in ws.row_values(1)]
                    foto_header = col_foto or "Foto"
                    if foto_header not in headers:
                        ws.add_cols(1)
                        ws.update_cell(1, len(headers)+1, foto_header)
                        headers.append(foto_header)
                        col_foto = foto_header  # passa a existir

                    # acha linha do produto e atualiza
                    dfp2 = get_as_dataframe(ws, evaluate_formulas=True, dtype=str, header=0).fillna("")
                    dfp2.columns = [c.strip() for c in dfp2.columns]
                    if col_id and pid:
                        mask2 = (dfp2[col_id].astype(str).str.strip() == pid)
                    else:
                        mask2 = (dfp2[col_nome].astype(str).str.strip() == pnm)
                    if not mask2.any():
                        st.error("Não achei o produto para escrever a foto.")
                    else:
                        row_idx = dfp2[mask2].index[0] + 2  # +2: header + 1-based
                        col_idx = headers.index(col_foto) + 1
                        ws.update_cell(row_idx, col_idx, direct_url)
                        st.success("Imagem enviada e URL salva no catálogo!")
                        st.image(direct_url, caption="Nova foto", use_container_width=True)
                        st.session_state["_force_refresh"] = True

                except Exception as e:
                    st.error(f"Falha no upload/salvar: {e}")

    st.divider()
    if st.button("💾 Salvar URL manual no catálogo", key="btn_url_manual") and url_manual.strip():
        try:
            # garante coluna
            headers = [h.strip() for h in ws.row_values(1)]
            foto_header = col_foto or "Foto"
            if foto_header not in headers:
                ws.add_cols(1)
                ws.update_cell(1, len(headers)+1, foto_header)
                headers.append(foto_header)
                col_foto = foto_header

            dfp2 = get_as_dataframe(ws, evaluate_formulas=True, dtype=str, header=0).fillna("")
            dfp2.columns = [c.strip() for c in dfp2.columns]
            if col_id and pid:
                mask2 = (dfp2[col_id].astype(str).str.strip() == pid)
            else:
                mask2 = (dfp2[col_nome].astype(str).str.strip() == pnm)
            if not mask2.any():
                st.error("Não achei o produto para escrever a foto.")
            else:
                row_idx = dfp2[mask2].index[0] + 2
                col_idx = headers.index(col_foto) + 1
                ws.update_cell(row_idx, col_idx, url_manual.strip())
                st.success("URL salva no catálogo!")
                st.image(url_manual.strip(), use_container_width=True)
                st.session_state["_force_refresh"] = True
        except Exception as e:
            st.error(f"Falha ao salvar URL: {e}")

st.caption("""
• Crie uma pasta no Google Drive, compartilhe com o e-mail do Service Account como **Editor** e copie o ID da pasta (secreto `DRIVE_FOTOS_FOLDER_ID`).
• As imagens são tornadas públicas por link (somente leitura) e a URL direta é gravada na coluna **Foto** (ou equivalente) da aba **Produtos**.
• A página **Produtos** só precisa ler e exibir a coluna de foto.
""")
