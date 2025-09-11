# =========================
# 🧪 Fracionar granel → fracionados
# =========================
st.divider()
st.subheader("🧪 Fracionar — converter GRANEL (L) em fracionados")

try:
    produtos = _load_df(PRODUTOS_ABA)
except Exception:
    produtos = pd.DataFrame(columns=["ID","Nome","Unidade"])

COL_ID   = COL["id"] or "ID"
COL_NOME = COL["nome"] or "Nome"
COL_UNID = COL["unid"] or "Unidade"

if produtos.empty or COL_UNID not in produtos.columns:
    st.info("Cadastre produtos primeiro (incluindo um SKU granel em **L**).")
else:
    # Filtra candidatos
    df_granel = produtos[produtos[COL_UNID].astype(str).str.strip().str.lower().eq("l")].copy()
    df_un     = produtos[produtos[COL_UNID].astype(str).str.strip().str.lower().eq("un")].copy()

    if df_granel.empty:
        st.warning("Nenhum produto granel (Unidade = L) encontrado.")
    elif df_un.empty:
        st.warning("Nenhum produto fracionado (Unidade = un) encontrado.")
    else:
        # Helpers de seleção
        def _fmt_opt(r):
            return f"{_nz(r.get(COL_NOME,''))}  ·  {_nz(r.get(COL_ID,''))}".strip()

        # Seleção do GRANEL
        idx_g = st.selectbox(
            "Matéria-prima (granel em L)", 
            options=range(len(df_granel)),
            format_func=lambda i: _fmt_opt(df_granel.iloc[i])
        )
        row_g = df_granel.iloc[idx_g]
        gid   = _nz(row_g.get(COL_ID,""))
        gnome = _nz(row_g.get(COL_NOME,""))

        # Estoque atual do granel (em L), só para referência
        estoque_g = _estoque_atual(pid=gid, nome=gnome)
        st.caption(f"📦 Estoque atual (granel): {estoque_g if isinstance(estoque_g,(int,float)) else 0} L")

        # Seleção dos SKUs fracionados (pode escolher os que quiser)
        c1, c2 = st.columns(2)
        with c1:
            idx_1 = st.selectbox(
                "SKU fracionado A (ex.: 1 L)", 
                options=range(len(df_un)),
                format_func=lambda i: _fmt_opt(df_un.iloc[i])
            )
            qtd_1 = st.number_input("Qtd frascos A", min_value=0, step=1, value=0)
            vol_1_l = st.number_input("Volume por frasco A (em L) — ex.: 1.0", min_value=0.0, step=0.1, value=1.0, format="%.3f")
        with c2:
            idx_2 = st.selectbox(
                "SKU fracionado B (ex.: 500 ml)", 
                options=range(len(df_un)),
                format_func=lambda i: _fmt_opt(df_un.iloc[i]),
                index=0
            )
            qtd_2 = st.number_input("Qtd frascos B", min_value=0, step=1, value=0)
            vol_2_l = st.number_input("Volume por frasco B (em L) — ex.: 0.5", min_value=0.0, step=0.1, value=0.5, format="%.3f")

        # Você pode adicionar mais linhas (C, D...) repetindo o padrão acima, se quiser.

        # Calcula equivalência total
        total_litros = (qtd_1 * vol_1_l) + (qtd_2 * vol_2_l)

        st.write(f"🔁 Litros a baixar do granel: **{total_litros:.3f} L**")

        confirmar = st.button("Registrar fracionamento", use_container_width=True)

        if confirmar:
            if total_litros <= 0:
                st.error("Informe quantidades > 0 para fracionar.")
                st.stop()

            # Valida estoque suficiente do granel (se disponível)
            if isinstance(estoque_g, (int, float)) and estoque_g < total_litros - 1e-9:
                st.error("Estoque do granel insuficiente para este fracionamento.")
                st.stop()

            # Abas necessárias
            ws_mov = _ensure_ws(MOVS_ABA, MOV_HEADERS)

            data_str = date.today().strftime("%d/%m/%Y")

            # 1) Saída do granel (em L, negativa)
            _append_row(ws_mov, {
                "Data": data_str,
                "IDProduto": gid,
                "Produto": gnome,
                "Tipo": "C fracionamento -",
                "Qtd": str(total_litros).replace(".", ","),
                "Obs": f"Fracionamento para SKUs vendáveis",
                "ID": "",
                "Documento/NF": "",
                "Origem": "Fracionamento",
                "SaldoApós": ""  # opcional (pode deixar vazio; o saldo é calculado em relatórios)
            })

            # 2) Entradas nos fracionados (em unidades, positivas)
            # A
            if qtd_1 > 0:
                r1 = df_un.iloc[idx_1]
                _append_row(ws_mov, {
                    "Data": data_str,
                    "IDProduto": _nz(r1.get(COL_ID,"")),
                    "Produto": _nz(r1.get(COL_NOME,"")),
                    "Tipo": "C fracionamento +",
                    "Qtd": str(qtd_1),
                    "Obs": f"Fracionamento: {vol_1_l:.3f} L/frasco",
                    "ID": "",
                    "Documento/NF": "",
                    "Origem": "Fracionamento",
                    "SaldoApós": ""
                })
            # B
            if qtd_2 > 0:
                r2 = df_un.iloc[idx_2]
                _append_row(ws_mov, {
                    "Data": data_str,
                    "IDProduto": _nz(r2.get(COL_ID,"")),
                    "Produto": _nz(r2.get(COL_NOME,"")),
                    "Tipo": "C fracionamento +",
                    "Qtd": str(qtd_2),
                    "Obs": f"Fracionamento: {vol_2_l:.3f} L/frasco",
                    "ID": "",
                    "Documento/NF": "",
                    "Origem": "Fracionamento",
                    "SaldoApós": ""
                })

            st.success("Fracionamento registrado com sucesso! ✅")
            st.toast("Movimentos de fracionamento lançados", icon="✅")
