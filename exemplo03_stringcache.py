import pandas as pd 
import polars as pl                   #pip install scypi
from datetime import datetime
import os


ENDERECO_DADOS = './../DADOS/CSVS/NovoBolsaFamilia/'

try:
    print('Obtendo os dados...')
    inicio = datetime.now()

    df_bolsa_familia = None

    lista_arquivos = []

    lista_dir_arquivos = os.listdir(ENDERECO_DADOS)

    for arquivo in lista_dir_arquivos:
        if arquivo.endswith('csv'):
            lista_arquivos.append(arquivo)

    print(lista_arquivos)

    for arquivo in lista_arquivos:
        print(f'processando o mês {arquivo}')
        df = pl.read_csv(ENDERECO_DADOS + arquivo, separator=';', encoding='iso-8859-1')

        if df_bolsa_familia is None:
            df_bolsa_familia = df
        else:
            df_bolsa_familia = pl.concat([df_bolsa_familia, df])

        print(df)
    
    del df
    
    print('Meses concatenados com sucesso!')

except Exception as e:
    print(f'Erro ao obter os dados: {e}')


#Salvando em arquivo parquet
try:
    df_bolsa_familia = df_bolsa_familia.with_columns(
        pl.col('VALOR PARCELA').str.replace(',','.').cast(pl.Float64)

    )

    print('Iniciado gravação de arquivo parquet...')
    df_bolsa_familia.write_parquet('./../DADOS/Parquet/NovoBolsaFamilia/bolsa_familia.parquet')

    fim = datetime.now()
    print(f' Tempo de execução, {fim - inicio}')

except Exception as e:
    print(f'Erro ao processar os dados {e} ')
    #import polars as pl 
import pandas as pd 
from datetime import datetime
#pip install fastparquet p/ rodar no pandas

#Dados parquet
try:
    inicio = datetime.now()
    print('Lendo arquivo parquet')
    df_bolsa_familia = pd.read_parquet('bolsa_familia.parquet')

    print(df_bolsa_familia.head())

    fim = datetime.now()
    print(f' Tempo de execução, {fim - inicio}')

except Exception as e:
    print(f'Erro ao obter dados parquet')

    import polars as pl
from datetime import datetime


ENDERECO_DADOS = './../DADOS/Parquet/NovoBolsaFamilia/'

try: 
    print('Iniciando o processamento Lazy()')
    inicio = datetime.now()

    with pl.StringCache():


        lazy_plan = (
        pl.scan_parquet(ENDERECO_DADOS + 'bolsa_familia.parquet').select(['NOME MUNICÍPIO', 'VALOR PARCELA']).with_columns(pl.col('NOME MUNICÍPIO').cast(pl.Categorical))
        .filter(pl.col('VALOR PARCELA') > 2000.0)
        .group_by('NOME MUNICÍPIO').agg(pl.col('VALOR PARCELA').sum().alias('SOMA_VALOR_PARCELA')).sort('SOMA_VALOR_PARCELA', descending=True).limit(100)



        )

    #print(lazy_plan)

    df_bolsa_familia = lazy_plan.collect()
    print(df_bolsa_familia)

    fim = datetime.now()
    print(f'Tempo de execução {fim - inicio}')

except Exception as e:
    print('Erro ao processar os dados', e)

