from pyspark.sql import SparkSession
import os
import zipfile

def extrair_todos_zips(pasta='dados'):
    
    for arquivo in os.listdir(pasta):
        
        if arquivo.endswith('.zip'):
            
            caminho_zip = os.path.join(pasta, arquivo)
            nome_pasta = arquivo.replace('.zip', '')
            pasta_destino = os.path.join(pasta, nome_pasta)
            
            if not os.path.exists(pasta_destino):
                os.makedirs(pasta_destino)
                print(f'📁 Criando pasta: {pasta_destino}')
            
            print(f'📦 Extraindo {arquivo}...')
            
            with zipfile.ZipFile(caminho_zip, 'r') as zip_ref:
                zip_ref.extractall(pasta_destino)
            
            print(f'✅ Extraído: {arquivo}\n')


if __name__ == "__main__":
    
    # Inicializa Spark
    spark = SparkSession.builder \
        .appName("ExtrairMicrodadosENEM") \
        .getOrCreate()
    
    extrair_todos_zips()
    
    spark.stop()