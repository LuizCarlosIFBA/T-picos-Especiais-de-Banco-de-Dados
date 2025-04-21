import pandas as pd
import glob
import os

# Caminho onde estão os arquivos CSV
caminho_pasta = '/home/luiz/Downloads/CSV/'
padrao_arquivos = os.path.join(caminho_pasta, '*.csv')

# Lista todos os arquivos CSV na pasta
lista_arquivos = glob.glob(padrao_arquivos)

# Mostra arquivos encontrados
print(f'📁 {len(lista_arquivos)} arquivos encontrados:')
for arq in lista_arquivos:
    print(f' - {arq}')

# Lista para armazenar os DataFrames
todos_dfs = []

# Tenta ler cada arquivo com tratamento de erro
for arquivo in lista_arquivos:
    print(f'📄 Lendo: {arquivo}')
    try:
        df = pd.read_csv(arquivo, sep=',', quotechar='"', encoding='latin1', dtype=str)
        print(f'✅ Lido com sucesso: {len(df)} linhas')
        todos_dfs.append(df)
    except Exception as e:
        print(f'❌ Erro ao ler {arquivo}: {e}')

# Verifica se algum DataFrame foi lido com sucesso
if not todos_dfs:
    print("⚠️ Nenhum arquivo foi carregado. Verifique os arquivos e tente novamente.")
    exit()

# Junta todos os DataFrames
df_completo = pd.concat(todos_dfs, ignore_index=True)

# Conversão e ordenação por ano e mês
try:
    df_completo['ANO_CMPT'] = df_completo['ANO_CMPT'].astype(int)
    df_completo['MES_CMPT'] = df_completo['MES_CMPT'].astype(int)
    df_completo = df_completo.sort_values(by=['ANO_CMPT', 'MES_CMPT'], ascending=[True, True])
except Exception as e:
    print(f'⚠️ Erro ao ordenar por ANO_CMPT e MES_CMPT: {e}')

# Salva o resultado final
saida = '/home/luiz/Downloads/aih_completo.csv'
df_completo.to_csv(saida, index=False, encoding='utf-8')
print(f'✅ Arquivos unidos e ordenados com sucesso! Salvo em: {saida}')

