# app.py
import streamlit as st

st.set_page_config(page_title="Ebenezér Variedades", page_icon="🛒", layout="wide")
st.title("🛒 Ebenezér Variedades")

st.write("Bem-vindo! Comece pela página de setup para criar/verificar as abas da planilha.")
st.page_link("pages/00_setup_planilha.py", label="➡️ Setup da Planilha", icon="🧼")

st.divider()
st.caption("Depois adicionaremos: Produtos, Compras, Vendas, Relatórios, Reposição…")
