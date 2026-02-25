import pandas as pd
import zipfile
import os

def descompactar_extrair_dados():
    arquivo_zip = r'dados/microdados_enem_2024.zip'
    
    with zipfile.ZipFile(arquivo_zip, 'r') as zip_ref:
        
        # Lista todos os arquivos dentro do zip
        lista_arquivos = zip_ref.namelist()
        
        # Procura o arquivo dentro da pasta DADOS
        arquivo_desejado = None
        for nome in lista_arquivos:
            if 'DADOS/' in nome and 'RESULTADOS_2024' in nome and nome.endswith('.csv'):
                arquivo_desejado = nome
                break
        
        if arquivo_desejado is None:
            raise FileNotFoundError("Arquivo RESULTADOS_2024 não encontrado dentro do ZIP.")
        
        # Extrai somente o arquivo desejado
        zip_ref.extract(arquivo_desejado, 'dados')
        
        caminho_extraido = os.path.join('dados', arquivo_desejado)
        
        # Lê o CSV
        df = pd.read_csv(caminho_extraido, sep=';', encoding='latin1')
        
        return df

resultados = descompactar_extrair_dados()
print(resultados.head())