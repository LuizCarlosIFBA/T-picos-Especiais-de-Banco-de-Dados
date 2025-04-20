import os
import pandas as pd
from dbfread import DBF

# Caminho onde estão os arquivos .dbf
caminho_pasta = '/home/luiz/Documentos/TEBD/DBF/2019'

# Lista todos os arquivos .dbf na pasta
arquivos_dbf = [f for f in os.listdir(caminho_pasta) if f.endswith('.dbf')]

# Verifica se há arquivos .dbf
if not arquivos_dbf:
    print("❌ Nenhum arquivo .dbf encontrado.")
    exit()

# Processa cada arquivo .dbf
for arquivo_dbf in arquivos_dbf:
    caminho_dbf = os.path.join(caminho_pasta, arquivo_dbf)
    
    print(f'⚙️  Convertendo {arquivo_dbf} para CSV...')
    
    try:
        # Lê o arquivo DBF
        tabela = DBF(caminho_dbf, encoding='latin1')
        df = pd.DataFrame(iter(tabela))

        # Caminho de saída para o CSV
        nome_csv = arquivo_dbf.replace('.dbf', '.csv')
        caminho_csv = os.path.join(caminho_pasta, nome_csv)

        # Exporta o DataFrame para um arquivo CSV
        df.to_csv(caminho_csv, index=False, encoding='utf-8')

        print(f'✅ {nome_csv} criado com sucesso!')
    except Exception as e:
        print(f"❌ Erro ao ler o arquivo {arquivo_dbf}: {e}")

print('🚀 Conversão concluída!')

