# =========================
# ✏️ Editar / 🗑️ Apagar registros
# =========================
st.divider()
st.subheader("✏️ Editar / 🗑️ Apagar registros")

# Helpers locais (idempotentes)
def _load_with_rownums(aba: str, headers: list[str]):
    ws = _ensure_ws(aba, headers)
    df = get_as_dataframe(ws, evaluate_formulas=True, dtype=str, header=0).dropna(how="all")
    if df.empty:
        df = pd.DataFrame(columns=headers)
    df = df.fillna("")
    df["__Linha"] = (df.index + 2).astype(int)  # cabeçalho = linha 1
    for h in headers:
        if h not in df.columns:
            df[h] = ""
    cols = ["__Linha"] + [c for c in df.columns if c != "__Linha"]
    return df[cols].copy(), ws

def _save_df_over(ws, df: pd.DataFrame):
    df2 = df.drop(columns=[c for c in df.columns if c == "__Linha"], errors="ignore")
    ws.clear()
    set_with_dataframe(ws, df2, include_index=False, include_column_header=True, resize=True)

tab_edit_comp, tab_edit_mov = st.tabs(["🧾 Editar Compras", "📦 Editar Movimentos"], key="tabs_edit_apagar")

# ---------- Editar / Apagar COMPRAS ----------
with tab_edit_comp:
    dfc, ws_c = _load_with_rownums(COMPRAS_ABA, COMPRAS_HEADERS)
    if dfc.empty:
        st.info("Sem compras registradas ainda.")
    else:
        c1, c2 = st.columns(2)
        with c1:
            filt_prod = st.text_input("Filtrar por Produto (contém)", "", key="filt_prod_compras")
        with c2:
            filt_data = st.text_input("Filtrar por Data (dd/mm/aaaa, contém)", "", key="filt_data_compras")

        mask = pd.Series(True, index=dfc.index)
        if filt_prod.strip():
            mask &= dfc["Produto"].astype(str).str.contains(filt_prod.strip(), case=False, na=False)
        if filt_data.strip():
            mask &= dfc["Data"].astype(str).str.contains(filt_data.strip(), case=False, na=False)

        dfc_view = dfc[mask].copy().sort_values("__Linha", ascending=False)
        st.dataframe(dfc_view, use_container_width=True, hide_index=True)

        linhas_opts = dfc_view["__Linha"].tolist()
        if not linhas_opts:
            st.info("Nenhuma linha corresponde ao filtro.")
        else:
            linha_sel = st.selectbox("Escolha a linha para editar/apagar", options=linhas_opts, key="linha_comp_sel")
            row_cur = dfc.loc[dfc["__Linha"] == linha_sel].iloc[0].copy()

            with st.form("form_edit_compra", clear_on_submit=False):
                st.markdown(f"**Linha {linha_sel}** — edite os campos e salve")
                e1, e2, e3 = st.columns(3)
                with e1:
                    data_n = st.text_input("Data", row_cur.get("Data",""), key="cmp_data")
                    prod_n = st.text_input("Produto", row_cur.get("Produto",""), key="cmp_prod")
                    unid_n = st.text_input("Unidade", row_cur.get("Unidade",""), key="cmp_unid")
                with e2:
                    forn_n = st.text_input("Fornecedor", row_cur.get("Fornecedor",""), key="cmp_forn")
                    qtd_n  = st.text_input("Qtd", row_cur.get("Qtd",""), key="cmp_qtd")
                    custo_n= st.text_input("Custo Unitário", row_cur.get("Custo Unitário",""), key="cmp_custo")
                with e3:
                    total_n= st.text_input("Total", row_cur.get("Total",""), key="cmp_total")
                    idp_n  = st.text_input("IDProduto", row_cur.get("IDProduto",""), key="cmp_idp")
                    obs_n  = st.text_input("Obs", row_cur.get("Obs",""), key="cmp_obs")

                colb1, colb2 = st.columns([1,1])
                salvar_ed = colb1.form_submit_button("💾 Salvar alterações", use_container_width=True)
                apagar_ln = colb2.form_submit_button("🗑️ Apagar esta linha", use_container_width=True)

            if salvar_ed:
                idx_real = dfc.index[dfc["__Linha"] == linha_sel][0]
                for k, v in {
                    "Data": data_n, "Produto": prod_n, "Unidade": unid_n, "Fornecedor": forn_n,
                    "Qtd": qtd_n, "Custo Unitário": custo_n, "Total": total_n,
                    "IDProduto": idp_n, "Obs": obs_n
                }.items():
                    dfc.at[idx_real, k] = v
                _save_df_over(ws_c, dfc)
                st.success(f"Linha {linha_sel} salva.")
                st.cache_data.clear()
                st.rerun()

            if apagar_ln:
                dfc_drop = dfc[dfc["__Linha"] != linha_sel].copy()
                _save_df_over(ws_c, dfc_drop)
                st.success(f"Linha {linha_sel} apagada.")
                st.cache_data.clear()
                st.rerun()

# ---------- Editar / Apagar MOVIMENTOS ----------
with tab_edit_mov:
    dfm, ws_m = _load_with_rownums(MOVS_ABA, MOV_HEADERS)
    if dfm.empty:
        st.info("Sem movimentos registrados ainda.")
    else:
        c1, c2 = st.columns(2)
        with c1:
            filt_prod_m = st.text_input("Filtrar por Produto (contém)", "", key="filt_prod_mov")
        with c2:
            filt_data_m = st.text_input("Filtrar por Data (dd/mm/aaaa, contém)", "", key="filt_data_mov")

        maskm = pd.Series(True, index=dfm.index)
        if filt_prod_m.strip():
            maskm &= dfm["Produto"].astype(str).str.contains(filt_prod_m.strip(), case=False, na=False)
        if filt_data_m.strip():
            maskm &= dfm["Data"].astype(str).str.contains(filt_data_m.strip(), case=False, na=False)

        dfm_view = dfm[maskm].copy().sort_values("__Linha", ascending=False)
        st.dataframe(dfm_view, use_container_width=True, hide_index=True)

        linhas_opts_m = dfm_view["__Linha"].tolist()
        if not linhas_opts_m:
            st.info("Nenhuma linha corresponde ao filtro.")
        else:
            linha_sel_m = st.selectbox("Escolha a linha para editar/apagar (Movimentos)", options=linhas_opts_m, key="linha_mov_sel")
            row_mov = dfm.loc[dfm["__Linha"] == linha_sel_m].iloc[0].copy()

            with st.form("form_edit_mov", clear_on_submit=False):
                st.markdown(f"**Linha {linha_sel_m}** — edite campos e salve")
                e1, e2, e3 = st.columns(3)
                with e1:
                    data2  = st.text_input("Data", row_mov.get("Data",""), key="mov_data")
                    idp2   = st.text_input("IDProduto", row_mov.get("IDProduto",""), key="mov_idp")
                    prod2  = st.text_input("Produto", row_mov.get("Produto",""), key="mov_prod")
                with e2:
                    tipo2  = st.text_input("Tipo", row_mov.get("Tipo",""), key="mov_tipo")
                    qtd2   = st.text_input("Qtd", row_mov.get("Qtd",""), key="mov_qtd")
                    obs2   = st.text_input("Obs", row_mov.get("Obs",""), key="mov_obs")
                with e3:
                    id2    = st.text_input("ID", row_mov.get("ID",""), key="mov_id")
                    doc2   = st.text_input("Documento/NF", row_mov.get("Documento/NF",""), key="mov_doc")
                    org2   = st.text_input("Origem", row_mov.get("Origem",""), key="mov_org")
                saldo2 = st.text_input("SaldoApós", row_mov.get("SaldoApós",""), key="mov_saldo")

                colb1, colb2 = st.columns([1,1])
                salvar_ed2 = colb1.form_submit_button("💾 Salvar alterações", use_container_width=True)
                apagar_ln2 = colb2.form_submit_button("🗑️ Apagar esta linha", use_container_width=True)

            if salvar_ed2:
                idx_real = dfm.index[dfm["__Linha"] == linha_sel_m][0]
                for k, v in {
                    "Data": data2, "IDProduto": idp2, "Produto": prod2, "Tipo": tipo2,
                    "Qtd": qtd2, "Obs": obs2, "ID": id2, "Documento/NF": doc2,
                    "Origem": org2, "SaldoApós": saldo2
                }.items():
                    dfm.at[idx_real, k] = v
                _save_df_over(ws_m, dfm)
                st.success(f"Linha {linha_sel_m} salva.")
                st.cache_data.clear()
                st.rerun()

            if apagar_ln2:
                dfm_drop = dfm[dfm["__Linha"] != linha_sel_m].copy()
                _save_df_over(ws_m, dfm_drop)
                st.success(f"Linha {linha_sel_m} apagada.")
                st.cache_data.clear()
                st.rerun()
