from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
import os

def processamento_camada_prata():
    # Inicializa Spark configurado para otimização em memória e gravação Parquet
    spark = SparkSession.builder \
        .appName("ProcessamentoPrataENEM") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .getOrCreate()
        
    pasta_bronze = '/app/data_lake/bronze/enem'
    pasta_prata = '/app/data_lake/prata/enem'
    
    print("✨ Iniciando Processamento e Limpeza (Camada Prata)...")
    
    anos_disponiveis = [d for d in os.listdir(pasta_bronze) if d.startswith('ano=')]
    
    if not anos_disponiveis:
         print("⚠️ Nenhuma pasta particionada encontrada na camada Bronze. Execute a extração primeiro.")
         spark.stop()
         return

    for diretorio_ano in anos_disponiveis:
        caminho_leitura = os.path.join(pasta_bronze, diretorio_ano)
        # Extrai o número do ano da string, ex "ano=2021" -> "2021"
        ano = diretorio_ano.split('=')[1] 
        caminho_escrita = os.path.join(pasta_prata, f"ano={ano}")
        
        print(f"🔍 Tratando dados do ano: {ano}...")
        
        # 1. Lê a base particionada inteira com extrema velocidade da Camada Bronze
        df = spark.read.parquet(caminho_leitura)
        
        # 2. Transformações da Camada Prata (Exemplos Práticos):
        
        # Selecionar apenas as colunas mais importantes para análise (Otimiza armazenamento final)
        # Notas, Informações do Candidato, e Redação
        colunas_analise = [
            "NU_INSCRICAO", "TP_FAIXA_ETARIA", "TP_SEXO", "TP_ESTADO_CIVIL", 
            "TP_COR_RACA", "TP_ESCOLA", "IN_TREINEIRO", "SG_UF_PROVA", 
            "NU_NOTA_CN", "NU_NOTA_CH", "NU_NOTA_LC", "NU_NOTA_MT", "NU_NOTA_REDACAO"
        ]
        
        # Filtra pelas colunas que existem no dataframe histórico do ano respectivo
        colunas_presentes = [c for c in colunas_analise if c in df.columns]
        df_prata = df.select(*colunas_presentes)
        
        # Preencher notas em branco (quando o aluno falta) com ZERO
        notas = ["NU_NOTA_CN", "NU_NOTA_CH", "NU_NOTA_LC", "NU_NOTA_MT", "NU_NOTA_REDACAO"]
        for nota in notas:
            if nota in df_prata.columns:
                df_prata = df_prata.withColumn(nota, when(col(nota).isNull(), 0.0).otherwise(col(nota)))
                
        print(f"💾 Salvando tabela higienizada em: {caminho_escrita}")
        
        # 3. Grava os dados tratados e otimizados na pasta da Prata
        df_prata.write.mode("overwrite").parquet(caminho_escrita)
        
        print(f"✅ Tratamento do ano {ano} concluído com sucesso e movido para a Prata!\n")

    print("🏁 Camada Prata Inteiramente Processada!")
    spark.stop()

if __name__ == "__main__":
    processamento_camada_prata()
