# app.py
import streamlit as st
from pathlib import Path

st.set_page_config(page_title="Ebenezér Variedades", page_icon="🛒", layout="wide")
st.title("🛒 Ebenezér Variedades")

st.write("Bem-vindo! Use a navegação da **barra lateral** (Sidebar).")
st.write("Procure pela página **`00_setup_planilha`** para criar/verificar as abas.")

# Verificação amigável da estrutura
pages_dir = Path(__file__).parent / "pages"
setup_page = pages_dir / "00_setup_planilha.py"

if setup_page.exists():
    st.success("Página encontrada: `pages/00_setup_planilha.py`")

    # Botão opcional para tentar abrir diretamente (se sua versão do Streamlit suportar)
    if st.button("➡️ Abrir 'Setup da Planilha'", use_container_width=True):
        try:
            # Nem todas as versões têm switch_page; por isso o try/except
            st.switch_page("pages/00_setup_planilha.py")
        except Exception:
            st.info("Se o botão não abrir, use a barra lateral (Sidebar) para navegar.")
else:
    st.error("Não encontrei `pages/00_setup_planilha.py`.")
    st.caption("Crie a pasta `pages/` na RAIZ do projeto e salve lá o arquivo `00_setup_planilha.py`.")
    st.code("mkdir -p pages  # depois salve o arquivo dentro desta pasta", language="bash")
