
    key = key.replace("\\n", "\n")
    key = "".join(ch for ch in key if unicodedata.category(ch)[0] != "C" or ch in ("\n", "\r", "\t"))
    return key

def _load_sa():
    svc = st.secrets.get("GCP_SERVICE_ACCOUNT")
    if svc is None:
        st.error("🛑 GCP_SERVICE_ACCOUNT ausente."); st.stop()
    if isinstance(svc, str):
        svc = json.loads(svc)
    svc = dict(svc)
    svc["private_key"] = _normalize_private_key(svc["private_key"])
    return svc

@st.cache_resource
def conectar_sheets():
    svc = _load_sa()
    scopes = ["https://www.googleapis.com/auth/spreadsheets",
              "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(svc, scopes=scopes)
    gc = gspread.authorize(creds)
    url_or_id = st.secrets.get("PLANILHA_URL")
    if not url_or_id:
        st.error("🛑 PLANILHA_URL ausente."); st.stop()
    return gc.open_by_url(url_or_id) if str(url_or_id).startswith("http") else gc.open_by_key(url_or_id)

@st.cache_data
def carregar_aba(nome_aba: str) -> pd.DataFrame:
    sh = conectar_sheets()
    ws = sh.worksheet(nome_aba)
    df = get_as_dataframe(ws, evaluate_formulas=True, dtype=str, header=0).dropna(how="all")
    df.columns = [c.strip() for c in df.columns]
    return df

def _first_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for c in candidates:
        if c in df.columns:
            return c
    lower_map = {c.lower(): c for c in df.columns}
    for c in candidates:
        if c.lower() in lower_map:
            return lower_map[c.lower()]
    return None

def _to_num(s):
    if s is None:
        return 0.0
    if isinstance(s, (int, float)):
        return float(s)
    s = str(s).strip()
    if s == "" or s.lower() in ("nan", "none"):
        return 0.0
    # trata vírgula como decimal
    s = s.replace(".", "").replace(",", ".") if s.count(",") == 1 and s.count(".") > 1 else s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return 0.0

# =========================
# Nomes das abas
# =========================
ABA_PRODUTOS = "Produtos"
ABA_COMPRAS  = "Compras"
ABA_VENDAS   = "Vendas"
ABA_AJUSTES  = "Ajustes"   # opcional

# =========================
# Carrega abas
# =========================
try:
    df_prod = carregar_aba(ABA_PRODUTOS)
except Exception as e:
    st.error("Erro ao abrir a aba Produtos.")
    with st.expander("Detalhes técnicos"):
        st.code(str(e))
    st.stop()

try:
    df_comp = carregar_aba(ABA_COMPRAS)
except Exception:
    df_comp = pd.DataFrame()

try:
    df_vend = carregar_aba(ABA_VENDAS)
except Exception:
    df_vend = pd.DataFrame()

try:
    df_aj = carregar_aba(ABA_AJUSTES)
except Exception:
    df_aj = pd.DataFrame()

# =========================
# Identifica colunas importantes
# =========================
# Produtos
col_id_prod    = _first_col(df_prod, ["ID", "Id", "Codigo", "Código", "SKU"])
col_nome       = _first_col(df_prod, ["Nome", "Produto", "Descrição"])
col_cat        = _first_col(df_prod, ["Categoria"])
col_forn_prod  = _first_col(df_prod, ["Fornecedor"])
col_estq_min   = _first_col(df_prod, ["EstoqueMin", "Estoque Mínimo", "EstqMin"])
col_custo_atual= _first_col(df_prod, ["CustoAtual", "Custo Médio", "CustoMedio"])
col_preco      = _first_col(df_prod, ["PreçoVenda", "PrecoVenda", "Preço", "Preco"])

# Compras
col_comp_idprod = _first_col(df_comp, ["IDProduto", "IdProduto", "ProdutoID", "ID Prod", "ID_Produto"])
col_comp_qtd    = _first_col(df_comp, ["Qtd", "Quantidade", "Qtde", "Qde"])
col_comp_custo  = _first_col(df_comp, ["Custo Unitário", "CustoUnitário", "CustoUnit", "Custo Unit", "Custo"])

# Vendas
col_vend_idprod = _first_col(df_vend, ["IDProduto", "IdProduto", "ProdutoID", "ID Prod", "ID_Produto"])
col_vend_qtd    = _first_col(df_vend, ["Qtd", "Quantidade", "Qtde", "Qde"])

# Ajustes (opcional)
col_aj_idprod = _first_col(df_aj, ["IDProduto", "IdProduto", "ProdutoID", "ID Prod", "ID_Produto"])
col_aj_qtd    = _first_col(df_aj, ["Qtd", "Quantidade", "Qtde", "Qde", "Ajuste"])

# =========================
# Calcula entradas/saídas/ajustes e custo médio
# =========================
entradas = pd.Series(dtype=float)
custo_medio = pd.Series(dtype=float)

if not df_comp.empty and col_comp_idprod and col_comp_qtd:
    comp = df_comp[[col_comp_idprod, col_comp_qtd]].copy()
    comp[col_comp_qtd] = comp[col_comp_qtd].map(_to_num)
    entradas = comp.groupby(col_comp_idprod, dropna=True)[col_comp_qtd].sum()

    if col_comp_custo:
        comp_cost = df_comp[[col_comp_idprod, col_comp_qtd, col_comp_custo]].copy()
        comp_cost[col_comp_qtd]  = comp_cost[col_comp_qtd].map(_to_num)
        comp_cost[col_comp_custo]= comp_cost[col_comp_custo].map(_to_num)
        comp_cost["parcial"] = comp_cost[col_comp_qtd] * comp_cost[col_comp_custo]
        soma_parcial = comp_cost.groupby(col_comp_idprod)["parcial"].sum()
        soma_qtd     = comp_cost.groupby(col_comp_idprod)[col_comp_qtd].sum().replace(0, pd.NA)
        custo_medio = (soma_parcial / soma_qtd).fillna(0.0)

saidas = pd.Series(dtype=float)
if not df_vend.empty and col_vend_idprod and col_vend_qtd:
    vend = df_vend[[col_vend_idprod, col_vend_qtd]].copy()
    vend[col_vend_qtd] = vend[col_vend_qtd].map(_to_num)
    saidas = vend.groupby(col_vend_idprod, dropna=True)[col_vend_qtd].sum()

ajustes = pd.Series(dtype=float)
if not df_aj.empty and col_aj_idprod and col_aj_qtd:
    aj = df_aj[[col_aj_idprod, col_aj_qtd]].copy()
    aj[col_aj_qtd] = aj[col_aj_qtd].map(_to_num)  # positivos entram, negativos saem
    ajustes = aj.groupby(col_aj_idprod, dropna=True)[col_aj_qtd].sum()

calc = pd.DataFrame({
    "Entradas": entradas,
    "Saidas":   saidas,
    "Ajustes":  ajustes
}).fillna(0.0)

calc["EstoqueCalc"] = calc["Entradas"] - calc["Saidas"] + calc["Ajustes"]
calc["CustoMedio"]  = custo_medio if not custo_medio.empty else 0.0
calc = calc.reset_index().rename(columns={"index": "ID_join"})

# =========================
# Merge com Produtos
# =========================
if not col_id_prod:
    st.error("Não encontrei a coluna de ID na aba Produtos (ex.: 'ID')."); st.stop()

df_prod["_ID_join"] = df_prod[col_id_prod].astype(str)
df_merge = df_prod.merge(calc, how="left", left_on="_ID_join", right_on="ID_join").drop(columns=["ID_join"])

# Colunas calculadas (internas)
df_merge["EstoqueAtual_calc"] = df_merge["EstoqueCalc"].fillna(0.0).map(float)
df_merge["CustoAtual_calc"]   = df_merge["CustoMedio"].fillna(0.0).map(float)

# Para filtros
col_cat  = col_cat or _first_col(df_merge, ["Categoria"])
col_forn = col_forn_prod or _first_col(df_merge, ["Fornecedor"])

# =========================
# Filtros de busca
# =========================
top, mid = st.columns([2.5, 1.5])
with top:
    termo = st.text_input("🔎 Buscar", placeholder="ID, nome, fornecedor, categoria...").strip()
with mid:
    only_low = st.checkbox("⚠️ Somente baixo estoque", value=False,
                           help="Mostra itens com EstoqueAtual ≤ EstoqueMin (se a coluna EstoqueMin existir).")

cat_col, forn_col = st.columns(2)
with cat_col:
    if col_cat and col_cat in df_merge.columns:
        cats = ["(todas)"] + sorted(pd.Series(df_merge[col_cat].dropna().astype(str).unique()).tolist())
        cat = st.selectbox("Categoria", cats)
    else:
        cat = "(todas)"
with forn_col:
    if col_forn and col_forn in df_merge.columns:
        forns = ["(todos)"] + sorted(pd.Series(df_merge[col_forn].dropna().astype(str).unique()).tolist())
        forn = st.selectbox("Fornecedor", forns)
    else:
        forn = "(todos)"

mask = pd.Series(True, index=df_merge.index)
if termo:
    t = termo.lower()
    mask &= df_merge.apply(lambda r: t in " ".join([str(x).lower() for x in r.values]), axis=1)
if col_cat and cat != "(todas)" and col_cat in df_merge.columns:
    mask &= (df_merge[col_cat].astype(str) == cat)
if col_forn and forn != "(todos)" and col_forn in df_merge.columns:
    mask &= (df_merge[col_forn].astype(str) == forn)

# Filtro de baixo estoque
if only_low and col_estq_min and col_estq_min in df_merge.columns:
    try:
        estq_min_num = df_merge[col_estq_min].map(_to_num)
        estq_atual_num = df_merge["EstoqueAtual_calc"]
        mask &= (estq_atual_num <= estq_min_num)
    except Exception:
        pass

dfv = df_merge[mask].reset_index(drop=True)

# =========================
# Exibição (corrigido para evitar KeyError)
# =========================
rename_map = {}
if "EstoqueAtual_calc" in dfv.columns:
    rename_map["EstoqueAtual_calc"] = "EstoqueAtual"
if "CustoAtual_calc" in dfv.columns:
    rename_map["CustoAtual_calc"] = "CustoAtual"
dfv = dfv.rename(columns=rename_map)

cols_candidatas = [
    col_id_prod, col_nome, col_cat, col_forn, col_preco,
    col_estq_min, "EstoqueAtual", "Entradas", "Saidas", "Ajustes", "CustoAtual"
]
cols_show = [c for c in cols_candidatas if c and c in dfv.columns]

if cols_show:
    st.dataframe(dfv[cols_show], use_container_width=True, hide_index=True)
else:
    st.dataframe(dfv, use_container_width=True, hide_index=True)

st.caption("""
• **EstoqueAtual** = Compras − Vendas ± Ajustes (calculado em tempo real).  
• **CustoAtual** = custo médio ponderado das compras.  
• Use a aba **Compras** e **Vendas** para registrar movimentos; **Ajustes** é opcional (quebra, perda, acerto).
""")
