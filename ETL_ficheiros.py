import pyBetterSql as query 
import pandas as pd
import re


# layout o ficheiro de extratos as importar 
NUM_MAX_COLUNAS=9


def ImportarNovoFicheiro(path):
   # importar ficheiro 

   df = pd.read_excel (path, header=None,usecols=range(9))
   df.columns=['data','dr','nint','descricao','documento','debito','credito','saldo_devedor','saldo_credor']
   df = query.sqldf("select * from df where data not in ('Data','Total:','Transporte:')")
   return df

def GerarDataFrameExtrato(df):
    dados_novo_df = []

    valor_numerico = ''
    valor_designacao=''

    for index, row in df.iterrows():
        if  pd.isna(row['dr']):
            valor_numerico_match = re.match(r'\d+', row['data'])
            if valor_numerico_match:
                valor_numerico   = valor_numerico_match.group()
                valor_designacao = row['data'].split('-', 1)[1].strip()  # remove_numero_e_hifen
            else:
                valor_numerico = None
        else:
            
            if row['dr'] in ('4','5'):
                dados_novo_df.append( [valor_numerico] +  [valor_designacao] +row.tolist() )
                
    # Cria um novo DataFrame usando a lista de dados que criados acima e especifica os nomes das colunas
    colunas =  ['conta'] + ['conta_designacao'] +list(df.columns) 
    df_extrato = pd.DataFrame(dados_novo_df, columns=colunas)

    df_extrato = df_extrato.fillna('')

    df_extrato['data'] = pd.to_datetime(df_extrato['data'], format='%d-%m-%Y')
    df_extrato.insert(0, 'ano', pd.to_datetime(df_extrato['data']).dt.year)
    df_extrato.insert(1, 'mes', pd.to_datetime(df_extrato['data']).dt.month)

    return df_extrato