import pandas as pd
from sqlalchemy import create_engine

# === CONFIGURAÇÕES ===
usuario = 'root'
senha = ''
host = 'localhost'
porta = '3306'
banco = 'sergipe'
tabela = 'fato_sergipe'
csv_path = '/home/luiz/Documentos/TEBD/CSV/tabelaFatoSE.csv'

# === CONECTA NO BANCO ===
engine = create_engine(f'mysql+pymysql://{usuario}:{senha}@{host}:{porta}/{banco}')

# === IMPORTAÇÃO EM CHUNKS COM TODAS AS COLUNAS COMO TEXTO ===
chunksize = 100000
primeiro_chunk = True

for chunk in pd.read_csv(csv_path, chunksize=chunksize, dtype=str):
    if primeiro_chunk:
        chunk.to_sql(name=tabela, con=engine, if_exists='replace', index=False)
        primeiro_chunk = False
    else:
        chunk.to_sql(name=tabela, con=engine, if_exists='append', index=False)
    print('Chunk importado com sucesso.')

print("Importação finalizada com sucesso!")

