# LENDO ARQUIVO PARQUET
import polars as pl
# import numpy as np
from datetime import datetime
# from scipy.stats import kurtosis, skew
# import matplotlib.pyplot as plt

# ENDERECO_DADOS = r'../bronze/'
# ENDERECO_DADOS = r'./AULA-13/PARQUET/'
ENDERECO_DADOS = './../DADOS/ParQuer/'

try:
    print('\nIniciando leitura do arquivo parquet...')
    inicio = datetime.now()  # Pega o tempo inicial

    # Gera o plano de execução para leitura do arquivo parquet
    df_plano_execucao = pl.scan_parquet(ENDERECO_DADOS + 'bolsa_familia.parquet')  # Polars - leitura direta
    
    # Executa o plano de execução para obter o DataFrame
    df_bolsa_familia = df_plano_execucao.collect()
  
    # PRÉPROCESSAMENTO - TRANSFORMAÇÃO
    # delimitando as colunas para exibir: NOME MUNICÍPIO, VALOR PARCELA
    # print('\nIniciando processamento dos dados do DataFrame...')    
    df_bolsa_familia = df_bolsa_familia[['NOME MUNICÍPIO', 'VALOR PARCELA']]

    # PROCESSAMENTO - TRANSFORMAÇÃO
    # POLARS
    df_bolsa_familia = (
        df_bolsa_familia.group_by('NOME MUNICÍPIO')
        .agg(pl.col('VALOR PARCELA')
        .sum())
        )

    # PROCESSAMENTO - TRANSFORMAÇÃO (Pensar no pré-processamento como o esforço necessário para deixar os dados "utilizáveis" e "limpos".)
    # POLARS 
    df_bolsa_familia = df_bolsa_familia.sort(by='VALOR PARCELA', descending=True)
    print(df_bolsa_familia.head(10))

    fim = datetime.now()  # Pega o tempo final
    print(f'Tempo de execução para leitura do parquet: {fim - inicio}')

except Exception as e:
    print("Erro ao Converter Valor da Parcela: ", e)