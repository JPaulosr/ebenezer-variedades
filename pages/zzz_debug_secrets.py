import streamlit as st
st.set_page_config(page_title="Debug Secrets", page_icon="🔎")

st.title("🔎 Debug Secrets")
keys = list(st.secrets.keys())
st.write("Chaves presentes em st.secrets:", keys)

svc = st.secrets.get("GCP_SERVICE_ACCOUNT", None)
st.write("Tipo de GCP_SERVICE_ACCOUNT:", type(svc).__name__)

if isinstance(svc, dict):
    st.write("Campos do GCP_SERVICE_ACCOUNT:", list(svc.keys()))
elif isinstance(svc, str):
    st.code((svc[:400] + "...") if len(svc) > 400 else svc)
else:
    st.write("Valor:", svc)

st.write("PLANILHA_URL:", st.secrets.get("PLANILHA_URL"))
