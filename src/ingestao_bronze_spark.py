from pyspark.sql import SparkSession
import os

def criar_camada_bronze():
    # Inicializa a sessão do Spark configurada para otimização de Parquet
    spark = SparkSession.builder \
        .appName("IngestaoBronzeENEM") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .getOrCreate()
    
    pasta_dados = '/app/dados'
    pasta_bronze = '/app/data_lake/bronze/enem'
    
    print("🚀 Iniciando ingestão do Data Lake (Camada Bronze)...")
    
    # Itera sobre todas as pastas extraídas (ex: microdados_enem_2020)
    for diretorio in os.listdir(pasta_dados):
        caminho_dir = os.path.join(pasta_dados, diretorio)
        
        # Ignora arquivos soltos tipo .zip
        if os.path.isdir(caminho_dir):
            ano = diretorio.split('_')[-1]
            nome_arquivo_csv = f"MICRODADOS_ENEM_{ano}.csv"
            caminho_csv = os.path.join(caminho_dir, 'DADOS', nome_arquivo_csv)
            
            # Se não achar o arquivo minúsculo, testa maiúsculo genérico (por conta de case sensitive no linux)
            if not os.path.exists(caminho_csv):
                 caminho_csv = os.path.join(caminho_dir, 'DADOS', f"MICRODADOS_ENEM_{ano}.CSV")
            
            if os.path.exists(caminho_csv):
                print(f'⏳ Processando ano {ano} a partir de: {caminho_csv}')
                
                # O formato do Inep/ENEM: CSV separado por ponto-e-vírgula e charset iso-8859-1 (latin1)
                df = spark.read.csv(
                    caminho_csv, 
                    header=True, 
                    sep=';', 
                    encoding='latin1'
                )
                
                # Destino: Particionando a pasta da camada bronze por ano
                pasta_saida = os.path.join(pasta_bronze, f"ano={ano}")
                
                print(f'💾 Convertendo para Parquet otimizado em: {pasta_saida}')
                
                # A conversão para parquet salva > 80% do espaço e aumenta a velocidade de leitura assustadoramente
                df.write.mode('overwrite').parquet(pasta_saida)
                
                print(f'✅ Ano {ano} salvo no Data Lake com sucesso!\n')
            else:
                print(f'⚠️ Arquivo CSV não encontrado na pasta {caminho_dir}/DADOS. Pulando...\n')

    print("🏁 Ingestão da Camada Bronze finalizada!")
    spark.stop()

if __name__ == "__main__":
    criar_camada_bronze()
