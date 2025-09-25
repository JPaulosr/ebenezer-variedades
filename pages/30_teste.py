# =========================================================
# 🛠️ Modo Simples — Corrigir lançamento (Editar / Apagar)
# =========================================================
st.divider()
st.subheader("🛠️ Corrigir lançamento (modo simples)")

TIPO_OP = st.radio(
    "O que você quer corrigir?",
    options=["Compra / Entrada", "Movimento de Estoque"],
    horizontal=True,
    key=f"modo_simples_tipo_{BUMP}",
)

# Carrega base + worksheet conforme o tipo
if "Compra" in TIPO_OP:
    BASE_ABA, BASE_HEADERS = COMPRAS_ABA, COMPRAS_HEADERS
else:
    BASE_ABA, BASE_HEADERS = MOVS_ABA, MOV_HEADERS

df_base, ws_base = _load_with_rownums(BASE_ABA, BASE_HEADERS)

# Busca simples (produto/tipo/data)
c1, c2 = st.columns([2, 1])
with c1:
    termo = st.text_input("🔎 Buscar por Produto/Tipo/Data", key=f"modo_simples_busca_{BUMP}")
with c2:
    limite = st.number_input("Mostrar últimos (n)", min_value=5, max_value=200, value=50, step=5)

def _mask_busca(df: pd.DataFrame, termo: str) -> pd.Series:
    if not termo.strip():
        return pd.Series([True]*len(df), index=df.index)
    t = termo.strip().lower()
    cols = [c for c in ["Produto","Tipo","Data","Nome"] if c in df.columns]
    if not cols:
        return pd.Series([True]*len(df), index=df.index)
    m = False
    for c in cols:
        m = (m | df[c].astype(str).str.lower().str.contains(re.escape(t), na=False)) if isinstance(m, pd.Series) else df[c].astype(str).str.lower().str.contains(re.escape(t), na=False)
    return m

mask = _mask_busca(df_base, termo)
df_view = df_base[mask].copy()

# Ordena por Data se existir, senão por índice desc
if "Data" in df_view.columns:
    try:
        df_view["_d"] = pd.to_datetime(df_view["Data"], format="%d/%m/%Y", errors="coerce")
        df_view = df_view.sort_values("_d", ascending=False)
    except Exception:
        df_view = df_view.sort_index(ascending=False)
else:
    df_view = df_view.sort_index(ascending=False)

df_view = df_view.head(int(limite)).reset_index(drop=True)

if df_view.empty:
    st.info("Nenhum lançamento encontrado.")
else:
    # Monta rótulo amigável para seleção
    def _rotulo(row: pd.Series) -> str:
        d = row.get("Data", "")
        prod = row.get("Produto", row.get("Nome", ""))
        tipo = row.get("Tipo", "")
        qtd = row.get("Qtd", "")
        lr = row.get("__Linha", "?")
        return f"Linha {lr} • {d} • {prod}" + (f" • {tipo}" if tipo else "") + (f" • Qtd {qtd}" if qtd else "")

    opcoes = list(range(len(df_view)))
    escolha = st.selectbox(
        "Escolha o lançamento",
        options=opcoes,
        format_func=lambda i: _rotulo(df_view.iloc[i]),
        key=f"modo_simples_select_{BUMP}",
    )
    rec = df_view.iloc[int(escolha)].to_dict()
    linha_real = int(rec["__Linha"])

    st.markdown("**Lançamento selecionado:**")
    st.json({k:v for k,v in rec.items() if k != "__Linha"})

    st.markdown("### Editar campos")
    # Campos básicos por tipo
    if "Compra" in TIPO_OP:
        c1, c2, c3 = st.columns(3)
        with c1:
            e_data = st.text_input("Data", value=_nz(rec.get("Data","")))
            e_prod = st.text_input("Produto", value=_nz(rec.get("Produto","")))
            e_unid = st.text_input("Unidade", value=_nz(rec.get("Unidade","")))
        with c2:
            e_forn = st.text_input("Fornecedor", value=_nz(rec.get("Fornecedor","")))
            e_qtd  = st.text_input("Qtd", value=_nz(rec.get("Qtd","")))
            e_cu   = st.text_input("Custo Unitário", value=_nz(rec.get("Custo Unitário","")))
        with c3:
            e_total = st.text_input("Total", value=_nz(rec.get("Total","")))
            e_idp   = st.text_input("IDProduto", value=_nz(rec.get("IDProduto","")))
            e_obs   = st.text_input("Obs", value=_nz(rec.get("Obs","")))
        campos_update = {
            "Data": e_data, "Produto": e_prod, "Unidade": e_unid, "Fornecedor": e_forn,
            "Qtd": e_qtd, "Custo Unitário": e_cu, "Total": e_total, "IDProduto": e_idp, "Obs": e_obs,
        }
    else:
        c1, c2, c3 = st.columns(3)
        with c1:
            m_data = st.text_input("Data", value=_nz(rec.get("Data","")))
            m_idp  = st.text_input("IDProduto", value=_nz(rec.get("IDProduto","")))
            m_prod = st.text_input("Produto", value=_nz(rec.get("Produto","")))
        with c2:
            m_tipo = st.text_input("Tipo", value=_nz(rec.get("Tipo","")))
            m_qtd  = st.text_input("Qtd", value=_nz(rec.get("Qtd","")))
            m_obs  = st.text_input("Obs", value=_nz(rec.get("Obs","")))
        with c3:
            m_id   = st.text_input("ID", value=_nz(rec.get("ID","")))
            m_doc  = st.text_input("Documento/NF", value=_nz(rec.get("Documento/NF","")))
            m_org  = st.text_input("Origem", value=_nz(rec.get("Origem","")))
        m_saldo = st.text_input("SaldoApós", value=_nz(rec.get("SaldoApós","")))
        campos_update = {
            "Data": m_data, "IDProduto": m_idp, "Produto": m_prod, "Tipo": m_tipo,
            "Qtd": m_qtd, "Obs": m_obs, "ID": m_id, "Documento/NF": m_doc,
            "Origem": m_org, "SaldoApós": m_saldo,
        }

    c_save, c_del = st.columns([1,1])
    with c_save:
        ok_save = st.checkbox("✔️ Confirmo salvar alterações", key=f"chk_save_{BUMP}")
        if st.button("💾 Salvar", use_container_width=True, disabled=not ok_save, key=f"btn_save_simple_{BUMP}"):
            base = df_base.copy()
            pos = base.index[base["__Linha"] == linha_real]
            if len(pos) != 1:
                st.error("Não achei a linha na planilha.")
            else:
                idx_base = pos[0]
                for k, v in campos_update.items():
                    if k in base.columns:
                        base.at[idx_base, k] = v
                _save_df_over(ws_base, base)
                st.success("Alterações salvas!")
                st.toast("Lançamento atualizado", icon="✅")
                _refresh_now()

    with c_del:
        ok_del = st.checkbox("🗑️ Confirmo apagar este lançamento", key=f"chk_del_{BUMP}")
        if st.button("Apagar", use_container_width=True, type="primary", disabled=not ok_del, key=f"btn_del_simple_{BUMP}"):
            base = df_base.copy()
            pos = base.index[base["__Linha"] == linha_real]
            if len(pos) != 1:
                st.error("Não achei a linha na planilha.")
            else:
                idx_base = pos[0]
                base = base.drop(index=idx_base).reset_index(drop=True)
                _save_df_over(ws_base, base)
                st.success("Lançamento apagado!")
                st.toast("Registro removido", icon="✅")
                _refresh_now()
