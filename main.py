import streamlit as st
import pandas as pd
import st_helper as hp
import pyBetterSql as q
import streamlit.components.v1 as components
import matplotlib.pyplot as plt
import ETL_ficheiros as f
import io 
import base64

st.set_page_config(layout="wide")
hp.remove_footer(st)
hp.remove_menu(st)

# CSS to inject contained in a string
Hide_dataframe_row_index = """
            <style>
            .row_heading.level0 {display:none}
            .blank {display:none}
            </style>
        """
st.markdown(Hide_dataframe_row_index, unsafe_allow_html=True)  


def demonstracao_resultados():
    df_agregado = df_query.groupby(['conta'])[['ano_0', 'ano_1', 'ano_2']].max().reset_index()
    df_agregado['conta']=df_agregado['conta'].astype(str)

    df_resultado = df_mapa.merge(df_agregado, on='conta', how='left').set_index('id')
    df_resultado = df_resultado.drop('calc', axis=1)
    df_resultado = df_resultado.fillna(0)


    # QUOTAS
    x = df_resultado.query("conta=='78882'")[['ano_0','ano_1','ano_2']]
    y = df_resultado.query("conta=='76'")[['ano_0','ano_1','ano_2']]

    df_resultado.loc[df_resultado['conta']=='QUOTAS',['ano_0','ano_1','ano_2']]= abs(x.values + y.values)


    # Diversos (78 - QUOTAS)
    x = df_resultado.query("conta=='78'")[['ano_0','ano_1','ano_2']]
    y = df_resultado.query("conta=='QUOTAS'")[['ano_0','ano_1','ano_2']]
    df_resultado.loc[df_resultado['conta']=='78',['ano_0','ano_1','ano_2']]= abs( y.values - x.values)

    # conta PO - 'Proveitos Operacionais' 
    df_selecionado = df_resultado.query("conta in ['QUOTAS', '721', '751','788883','711', '78']")[['ano_0', 'ano_1', 'ano_2']]
    df_PO = df_selecionado.sum()

    # atribua os valores da soma à conta 'PO'
    df_resultado.loc[df_resultado['conta'] == 'PO', ['ano_0', 'ano_1', 'ano_2']] = df_PO.values

    # Conta 61 - CMVMC
    x = df_resultado.query("conta=='61'")[['ano_0','ano_1','ano_2']].sum()
    df_resultado.loc[df_resultado['conta'] == 'CMVMC', ['ano_0', 'ano_1', 'ano_2']] = x.values

    #FS
    x = df_resultado.query("conta in ('6221','6222','6223','6224','6226','6231','6232','6233','6234','6241','6242','6243','6248','6251','6252','6261','6262','6263','6265','6266','6267','6268')")[['ano_0','ano_1','ano_2']].sum()
    df_resultado.loc[df_resultado['conta'] == 'FS', ['ano_0', 'ano_1', 'ano_2']] = x.values

    #FSE
    x = df_resultado.query("conta in ('621','FS')")[['ano_0','ano_1','ano_2']].sum()
    df_resultado.loc[df_resultado['conta'] == 'FSE', ['ano_0', 'ano_1', 'ano_2']] = x.values

    ### PESSOAL
    # Subsidios
    x = df_resultado.query("conta in ('6322','6323')")[['ano_0','ano_1','ano_2']].sum()
    df_resultado.loc[df_resultado['conta'] == 'SUBS', ['ano_0', 'ano_1', 'ano_2']] = x.values

    # CUSTOS COM PESSOAL
    x = df_resultado.query("conta in ('6321','635','6322','6323','636','637','638')")[['ano_0','ano_1','ano_2']].sum()
    df_resultado.loc[df_resultado['conta'] == 'CP', ['ano_0', 'ano_1', 'ano_2']] = x.values

    # CUSTOS OPERACIONAIS
    x = df_resultado.query("conta in ('CMVMC','FSE','CP','688')")[['ano_0','ano_1','ano_2']].sum()
    df_resultado.loc[df_resultado['conta'] == 'CO', ['ano_0', 'ano_1', 'ano_2']] = x.values

    # RESULTADOS
    x = df_resultado.query("conta in ('PO')")[['ano_0','ano_1','ano_2']].sum()
    y= df_resultado.query("conta in ('CO')")[['ano_0','ano_1','ano_2']].sum()
    df_EBITDA= (x-y)
    df_resultado.loc[df_resultado['conta'] == 'EBITDA', ['ano_0', 'ano_1', 'ano_2']] = df_EBITDA.values

    #RLE
    x = df_resultado.query("conta in ('EBITDA')")[['ano_0','ano_1','ano_2']].sum()
    y = df_resultado.query("conta in ('681','64','65','IRC')")[['ano_0','ano_1','ano_2']].sum()
    df_resultado.loc[df_resultado['conta'] == 'RLE', ['ano_0', 'ano_1', 'ano_2']] = x.values - y.values

    df_resultado['delta_perc'] =round(((df_resultado['ano_0'] - df_resultado['ano_1']) / df_resultado['ano_1']) * 100,2)
    df_resultado['delta_perc'] = df_resultado['delta_perc'].astype(str)
    df_resultado['delta_perc'] = df_resultado['delta_perc'] + '%'
    df_resultado['delta_valor']= abs((df_resultado['ano_0'] - df_resultado['ano_1']))
    
    df_resultado = df_resultado[['conta', 'nome', 'ano_0', 'ano_1', 'delta_perc','delta_valor','ano_2']].fillna(0)
    df_resultado[['ano_0','ano_1','ano_2','delta_valor']]=df_resultado[['ano_0','ano_1','ano_2','delta_valor']].round()

    df_resultado.rename(columns={'nome':' ','ano_0': f'{mes_abrev} {ano_max}','ano_1':f'{mes_abrev} {ano_max-1}','ano_2':f'{mes_abrev} {ano_max-2}','delta_perc':u'Δ' + f'({str(ano_max)[-2:]},{str(ano_max-1)[-2:]})','delta_valor':'Valor'}, inplace=True)
    
    return df_resultado

@st.cache(allow_output_mutation=True)
def obter_saldo_conta (classe_conta, ano, mes):
    gc  = len(classe_conta)
    
    lista_anos=''
    for i in ano:
        lista_anos=lista_anos + str(i) + ','
    lista_anos=lista_anos[:-1]
    return   q.sqldf(f"""
        select ano, max(mes) as mes, conta_{gc} as conta, df_plano.nome,abs(round(sum(debito)-sum(credito))) as valor
        from df_movim left join df_plano on conta_{gc} = df_plano.conta 
        where     conta_{gc} ={classe_conta}
                  and mes<={mes}
                  and ano in ({lista_anos})
        group by ano

    """)

@st.cache(allow_output_mutation=True)
def obter_saldo_contas (contas, ano, mes):
    sql=''
    resultado_exec=[]
    lista_contas=''
    for ct in contas:
        gc  = len(str(ct))
        lista_anos=''
        for i in ano:
            lista_anos=lista_anos + str(i) + ','
        lista_anos=lista_anos[:-1]
        
        sql=f"""
            select ano, max(mes) as mes, conta_{gc} as conta, df_plano.nome,abs((sum(debito)-sum(credito))) as valor
            from df_movim left join df_plano on conta_{gc} = df_plano.conta 
            where     conta_{gc} ={ct}
                      and mes<={mes}
                      and ano in ({lista_anos})
            group by ano
        """
        df_resultado=q.sqldf(sql)
        if not df_resultado.empty:
            resultado_exec.append(df_resultado)
    try:        
        return  pd.concat(resultado_exec)
    except Exception as e:
        return pd.DataFrame([])

@st.cache(allow_output_mutation=True)
def obter_mov_conta (classe_conta, ano, mes):
    gc=len(classe_conta)
    return  q.sqldf(f"""

            select ano,mes,conta_{gc} as conta, df_plano.nome,df_movim.conta as subconta,df_plano.nome as conta_designacao,df_movim.descricao,round(sum(debito)-sum(credito)) as saldo
            from df_movim left join df_plano on conta_{gc} = df_plano.conta 
            where     conta_{gc} ={classe_conta}
                      and mes<={mes}
                      and ano in ({ano})
            group by ano,mes,nint
            order by ano,mes,conta,subconta,nint
        """)    

@st.cache(allow_output_mutation=True) 
def load_extrato(uploaded_file):
    df = pd.read_excel(uploaded_file, sheet_name='extrato')
    return df

@st.cache(allow_output_mutation=True)
def load_plano(uploaded_file):
    df = pd.read_excel(uploaded_file, sheet_name='plano')
    return df

@st.cache(allow_output_mutation=True)    
def load_mapa(uploaded_file):
    df = pd.read_excel(uploaded_file, sheet_name='mapa')
    return df

def filtrar_df(opcoes_selecionadas):
    contas_selecionadas = [int(opcao.split(' - ')[0]) for opcao in opcoes_selecionadas]
    return df[df['conta'].isin(contas_selecionadas)]

def selecionar_ano(df):
    anos = sorted(df['ano'].unique(), reverse=True)
    return st.multiselect('Ano', anos, default=anos[0])

def selecionar_mes(df):   
    meses = df['mes'].unique()
    return st.selectbox('Mês', list(meses))

def parse_selecao(opcoes_selecionadas):
    contas_selecionadas = [opcao.split('-')[0].strip() for opcao in opcoes_selecionadas]
    return df_plano[df_plano['conta'].isin(contas_selecionadas)]

def abreviatura_mes(mes):
    meses={1:'Jan',2:'Fev',3:'Mar',4:'Abr',5:'Mai',6:'Jun',7:'Jul',8:'Ago',9:'Set',10:'Out',11:'Nov',12:'Dez'}
    return meses.get(mes,'Erro!')
########################################################################################################################################
##          M  A I N
########################################################################################################################################
st.sidebar.header("Carregar Ficheiro CP-FSE")
uploaded_file = st.sidebar.file_uploader("Ficheiro CP-FSE", type=['xlsx'])
if uploaded_file is not None:
    try:
        df_movim = load_extrato(uploaded_file)
        df_plano = load_plano  (uploaded_file) 
        df_mapa  = load_mapa   (uploaded_file) 
        
        #Validação do Ficheiro
        ano_max = df_movim['ano'].max()
        mes_max = df_movim.query(f'ano=={ano_max}')['mes'].max()
        #mes_max = 4
        mes_abrev = abreviatura_mes(mes_max)
        st.sidebar.write("Posição:", f"**<span style='color:red'>{mes_abrev} - {str(ano_max)} </span>**", unsafe_allow_html=True)
        st.sidebar.success("Importação concluída com sucesso")


        # definir conta como string 
        df_movim['conta'] = df_movim['conta'].astype(str)
        df_plano['conta'] = df_plano['conta'].astype(str)
        df_mapa ['conta']  = df_mapa['conta'].astype(str)
        #Cria columas para facilitar pesquisa de contas por class de conta
        for i in range (0,8):
            df_movim['conta_{i+1}'] =df_movim['conta'].str.slice(0, {i+1})

        lista_contas=list(df_mapa[df_mapa['calc'] == 'n']['conta'])
        df_saldos = obter_saldo_contas(lista_contas,[ano_max,ano_max-1,ano_max-2],mes_max)

        df_query  = q.sqldf(f"""
            Select conta,mes, (case when ano={ano_max} then valor else 0 end) as ano_0,
                            (case when ano={ano_max-1} then valor else 0 end) as ano_1,
                            (case when ano={ano_max-2} then valor else 0 end) as ano_2 
            from df_saldos
            """)

        df_resultado = demonstracao_resultados()
        
        # Função para aplicar o estilo CSS
        def highlight_row(row):
            if row['conta'] in ('PO','CO','EBITDA','RLE'):
                return ['background-color: #D6EAF8;font-weight: bold;'] * len(row)
            if row['conta'] in ('QUOTAS','CMVMC','FSE','CP','688'):
                return ['background-color:  #E8F8F5 ; font-weight: bold; '] * len(row)
            if row['conta'] in ('SUBS','FS'):
                return [' font-weight: bold; '] * len(row)
            
            return [''] * len(row)
    
    
        styled_df = df_resultado.style.apply(highlight_row, axis=1).set_table_styles([{'selector': 'th', 'props': [('min-width', '100px')]}])
        styled_df = styled_df.hide_columns(['conta'])

    
        # Convertendo o dataframe estilizado para HTML
        html = styled_df.hide_index().render()
        html_css = '''
        <style type="text/css">
        table {
        color: #333;
        font-family:verdana;
        font-size:12px;
    
        border-collapse:
        collapse; 
        border-spacing: 2;
        }

        td, th {
        border: 1px solid transparent; /* No more visible border */
        height: 20px;
        }

        th {
        background: NAvy; /* Darken header a bit */
        color: white;
        height: 30px;
        }
        th:nth-child(4) {
        background: orange; 
        border: 1px Inset white;
        }

        th:nth-child(5) {
        background: orange; 
        border: 1px Inset white;
        }
    
        td:nth-child(2) {
        text-align: right;
        }

        td:nth-child(3) {
        text-align: right;
        }

        td:nth-child(4) {
        text-align: right;
        }
        td:nth-child(5) {
        text-align: right;
        }
        td:nth-child(6) {
        text-align: right;
        }
        td:nth-child(4), td:nth-child(5) {
        border: 1px Ridge navy;
    }

        </style>
        ''' 
        with st.expander('Demonstração de Resultados'):
            #################################################################################################################
            ####     DEMONSTRACAO DE RESULTADOS                                                                          ####

            st.subheader("Análise Comparativa últimos 3 anos")
            # Exibindo o HTML no Streamlit
            components.html(html_css+html,height=1280)

        with st.expander('Análise Comparativa'):
            #################################################################################################################
            ####     GRAFICOS  MARGEM BRUTA                                                                              ####
             
            options = [
                ('MOVIMENTOS EXTRATOS'),
                ('MARGEM OPERACIONAL'),
                ('PROVEITOS, CUSTOS E COMPORTAMENTO MARGEM'),
                ('PRINCIPAIS RÚBRICAS FSE')
            ]
            c1,c2 =st.columns([1,0.3])
            
            selected_option = c1.selectbox('', options)
            if selected_option=='MOVIMENTOS EXTRATOS':
                #################################################################################################################
                ####     MOVIMENTOS EXTRATO                                                                                  ####

                st.subheader("Pesquisa de movimentos e contas")
                show_resultado=False
                show_resultado2=False
                c1,c2,c3 =st.columns([1.5,0.5,3])

                with c1:
                    anos_selecionados = selecionar_ano(df_movim)
                    anos_selecionados= sorted(anos_selecionados, reverse=True)

                with c2:
                    mes_selecionado = selecionar_mes(df_movim)      
                with c3:
                    df_plano['conta'] = df_plano['conta'].astype(str)
                    df_filtrado = df_plano[df_plano['conta'].str.startswith(('6', '7'))]
                    df_distinto = df_filtrado.drop_duplicates(subset=['conta', 'nome']).sort_values(by=['conta', 'nome'])
                    opcoes_multiselect = [f"{row['conta']}-{row['nome']}" for index, row in df_distinto.iterrows()]
                    # Seleção múltipla de contas com nomes
                    selecao = st.multiselect('Selecione conta(s):', opcoes_multiselect)
                    saldo_contas = obter_saldo_contas(list(parse_selecao(selecao)['conta']), anos_selecionados, mes_selecionado)

                    mostra_tabelas=False
                    if not saldo_contas.empty:
                        mostra_tabelas=True
                    
                    else:
                        st.write("Sem resultados.")     

                if mostra_tabelas==True:
                    c1,c2=st.columns([0.5,1]) 
                    c1.write('#### Saldos de Contas')
                    # st.write(saldo_contas.reset_index().style.format(subset=['valor'], formatter="{:.2f}"))  

                    if len(anos_selecionados)>=3:    
                        df_saldoscontas  = q.sqldf(f"""
                        Select conta,mes, nome, (case when ano={anos_selecionados[0]} then valor else 0 end)   as  '{anos_selecionados[0]}',
                                    (case when ano={anos_selecionados[1]} then valor else 0 end) as  '{anos_selecionados[1]}',
                                    (case when ano={anos_selecionados[2]} then valor else 0 end) as  '{anos_selecionados[2]}' 
                            from saldo_contas
                            """)
                        df_saldoscontas=df_saldoscontas.groupby(['conta','nome'])[[f'{anos_selecionados[0]}',f'{anos_selecionados[1]}',f'{anos_selecionados[2]}']].max().round().reset_index()
                    if len(anos_selecionados)==2:    
                        df_saldoscontas  = q.sqldf(f"""
                        Select conta,mes, nome, (case when ano={anos_selecionados[0]} then valor else 0 end)   as  '{anos_selecionados[0]}',
                                    (case when ano={anos_selecionados[1]} then valor else 0 end) as  '{anos_selecionados[1]}'
                        
                            from saldo_contas
                            """)
                        df_saldoscontas=df_saldoscontas.groupby(['conta','nome'])[[f'{anos_selecionados[0]}',f'{anos_selecionados[1]}']].max().round().reset_index()
                    if len(anos_selecionados)==1:    
                        df_saldoscontas  = q.sqldf(f"""
                        Select conta,mes, nome, (case when ano={anos_selecionados[0]} then valor else 0 end) as '{anos_selecionados[0]}'
                            from saldo_contas
                            """)
                        df_saldoscontas=df_saldoscontas.groupby(['conta','nome'])[[f'{anos_selecionados[0]}']].max().round().reset_index()

                    c2.write(df_saldoscontas.style.format(subset=[f'{anos_selecionados[0]}'], formatter="{:.0f}"))

                    st.write('#### Extrato de Movimentos de Contas')
                    c1,c2=st.columns([0.3,1])
                    ano_extrato = c1.selectbox('Ano',anos_selecionados)
                    conta_extrato = c2.selectbox('Conta',selecao).split('-')[0].strip()
                    c1,c2,c3=st.columns([0.3,0.2,1])
                    df_mov_conta = obter_mov_conta(conta_extrato,ano_extrato,mes_selecionado).reset_index()
                
                    quadro=df_mov_conta[['ano','mes','descricao','conta_designacao','saldo']].style.format(subset=['saldo'], formatter="{:.2f}")
                    st.dataframe(quadro)

            if selected_option=='MARGEM OPERACIONAL':
                st.write('##### Proveitos Operacionais')
                df_PO=(df_resultado.query("conta in ('QUOTAS','721','711','78')"))
                df_PO = df_PO.iloc[:, [1,2,3,6]]
                st.write(df_PO.style,width=200)
                st.write('##### Custos Operacionais')
                df_CO=(df_resultado.query("conta in ('CMVMC','FSE','CP','688')"))
                df_CO = df_CO.iloc[:, [1,2,3,6]]
                st.write(df_CO.style,width=200)

                st.write('#### Margem Operacional')
                # Calculo da Margem Bruta
                x1= df_resultado.query("conta in ('721','711')").iloc[:, [2,3,6]].sum()
                x2= df_resultado.query("conta in ('CMVMC')").iloc[:, [2,3,6]]
            
                margem_operacional = (x1.values / x2.values) - 1
                
                c1,c2,c3,c4,c5=st.columns([0.5,0.5,0.5,0.5,0.5])

                c2.metric(f"Ano {ano_max}",  value=f'{margem_operacional[0,0]:.2f}  %') 
                c3.metric(f"Ano {ano_max-1}",value=f'{margem_operacional[0,1]:.2f}  %') 
                c4.metric(f"Ano {ano_max-2}",value=f'{margem_operacional[0,2]:.2f}  %') 

            if selected_option=='PROVEITOS, CUSTOS E COMPORTAMENTO MARGEM':
                df_PO_CO=(df_resultado.query("conta in ('PO','CO')"))
                df_PO_CO = df_PO_CO.iloc[:, [1,2,3,6]]
                st.write(df_PO_CO.style,width=500)
                st.write('#### Margem Operacional')

                # Calculo da Margem Bruta
                x1= df_resultado.query("conta in ('721','711')").iloc[:, [2,3,6]].sum()
                x2= df_resultado.query("conta in ('CMVMC')").iloc[:, [2,3,6]]
            
                margem_operacional = (x1.values / x2.values) - 1
                
                c1,c2,c3,c4,c5=st.columns([0.5,0.5,0.5,0.5,0.5])

                c2.metric(f"Ano {ano_max}",value=f'{margem_operacional[0,0]:.2f}  %') 
                c3.metric(f"Ano {ano_max-1}",value=f'{margem_operacional[0,1]:.2f}  %') 
                c4.metric(f"Ano {ano_max-2}",value=f'{margem_operacional[0,2]:.2f}  %') 

                
                c1,c2=st.columns([0.1,1])
            

                
                
    #------------------------------------------------------------------------------------------------
    # Gráfico 
    #            
                import streamlit as st
                import altair as alt

                # Dados
                data = {
                    "ano": [2023,2023,2022,2022,2021,2021,],
                    "valor": [542883, 574269, 503345,553566, 377077,339741],
                    "Conta": ['Proveitos Operacionais','Custos Operacionais', 'Proveitos Operacionais','Custos Operacionais','Proveitos Operacionais','Custos Operacionais'], 
                    
                }

                df=pd.DataFrame(data)
                c2.subheader("Resultados Comparativos 3 anos")
                chart = alt.Chart(df).mark_bar().encode(
                    x=alt.X('ano:N'     , axis=alt.Axis(title='Ano'),sort="-x") ,
                    y=alt.Y('valor:Q'   , axis=alt.Axis(title='Valor')) ,
                    color = 'ano:N', column=alt.Column('Conta:N',title=None),
                ).properties(width=200)
                
                c2.write("")
                c2.write(chart)
    #------------------------------------------------------------------------------------------------
                
            if selected_option=='PRINCIPAIS RÚBRICAS FSE':
                import altair as alt
                df_PrincipaisFSE=(df_resultado.query("conta in ('621','6223','6224','6226','6241','6242','6261','6267','6268')"))
                df_PrincipaisFSE = df_PrincipaisFSE.iloc[:, [1,2,3,6]]

                # st.write(df_PrincipaisFSE.style,width=200)

                a1= df_PrincipaisFSE.iloc[:, 0].tolist()
                a2= df_PrincipaisFSE.iloc[:, 1].tolist()
                a3= df_PrincipaisFSE.iloc[:, 2].tolist()
                a4= df_PrincipaisFSE.iloc[:, 3].tolist()
            
                # Crie um dataframe com os dados
                df = pd.DataFrame({
                    'FSE'         : a1,
                    f'{ano_max}'  : a2,
                    f'{ano_max-1}': a3,
                    f'{ano_max-2}': a4,
                })

                # fig, ax = plt.subplots(figsize=(5, 4))
                # df.plot(kind='barh', x='FSE', y=[f'{ano_max}',f'{ano_max-1}',f'{ano_max-2}'], ax=ax)
                # ax.set_title('Principais Rúbricas FSE')
                c1,c2=st.columns([0.1,1])
                # c1.pyplot(fig)

                
                #conta, ano, valor
                sql1=q.sqldf("""Select FSE as conta,'2023' as ano,[2023] as valor from df a """)
                sql2=q.sqldf("""Select FSE as conta,'2022' as ano,[2022] as valor from df a """)
                sql3=q.sqldf("""Select FSE as conta,'2021' as ano,[2021] as valor from df a """)

                sql=sql1.append(sql2)
                sql=sql.append(sql3)

                chart = alt.Chart(sql).mark_bar().encode(
                    x=alt.X('sum(valor):Q', title=None),
                    y=alt.Y('ano:O', title=None),
                    color='ano:N',
                    row=alt.Row('conta:N', header=alt.Header(labelAngle=0,labelAlign='left'),title=None)
                ).properties(width=500,height=40, title='Principais Rúbricas FSE')
                c2.write("")
                c2.write(chart)
    except:
        st.sidebar.warning("Ficheiro Inválido")
    

      
st.sidebar.header("Importar novos movimentos de extrato")
upfileclasse6=st.sidebar.file_uploader("Extrato Custos", type=['xlsx','xls'])
upfileclasse7=st.sidebar.file_uploader("Extrato Proveitos", type=['xlsx','xls'])
if upfileclasse6:
    df1 = f.ImportarNovoFicheiro(upfileclasse6)
if upfileclasse7:
    df2 = f.ImportarNovoFicheiro(upfileclasse7)
if st.sidebar.button("Gerar Novo Ficheiro CP-FSE"):
    if upfileclasse6 and upfileclasse7:
     
        dfs = [df1, df2]
        result = pd.concat(dfs)
        df_result= f.GerarDataFrameExtrato(result)

        df_result['conta']=pd.to_numeric(df_result['conta'], errors='coerce')
        df_result['dr']=pd.to_numeric(df_result['dr'], errors='coerce')
        df_result['nint']=pd.to_numeric(df_result['nint'], errors='coerce')
        df_result['debito']=pd.to_numeric(df_result['debito'], errors='coerce')
        df_result['credito']=pd.to_numeric(df_result['credito'], errors='coerce')
        df_result['saldo_devedor']=pd.to_numeric(df_result['saldo_devedor'], errors='coerce')
        df_result['saldo_credor']=pd.to_numeric(df_result['saldo_credor'], errors='coerce')

        df_result.fillna(0, inplace=True)
        #df_result[['ano','mes','conta','conta_designacao','descricao','debito','nint','credito','saldo_devedor','saldo_credor']].to_excel('teste_mov_importados.xlsx',index=None)
        df_resposta = df_result[['ano','mes','conta','conta_designacao','descricao','debito','nint','credito','saldo_devedor','saldo_credor']]
        nome_ficheiro='CP-SFE-2023-v1'
        ficheiro = io.BytesIO()
        
        download_file = df_resposta.to_excel(ficheiro, encoding='utf-8', index=False, header=True)
        ficheiro.seek(0)  # reset do pointer
        b64 = base64.b64encode(ficheiro.read()).decode()  
        link_download= f'''<div style=" background-color:white; 
                                        margin-top:35px;
                                        text-align:center;
                                        text-decoration:none;
                                        background-color:#F6F9F6;
                                        padding:10px 5px 5px 5px; 
                                        border: 1px solid green; 
                                        border-radius:10px 10px 10px 10px;">
                                <a href="data:application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;base64,{b64}" 
                                        download="{nome_ficheiro}.xlsx">Download</a>
                            </div>'''
        st.sidebar.markdown(link_download, unsafe_allow_html=True)
    else:
        st.sidebar.warning('É necessário importar 2 ficheiros de movimentos extrato.')
