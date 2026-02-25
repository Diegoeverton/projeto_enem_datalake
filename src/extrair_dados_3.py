import os
import zipfile

def extrair_todos_zips(pasta='dados'):
    
    # percorre todos os arquivos da pasta
    for arquivo in os.listdir(pasta):
        
        if arquivo.endswith('.zip'):
            
            caminho_zip = os.path.join(pasta, arquivo)
            
            # nome da pasta de destino (remove .zip)
            nome_pasta = arquivo.replace('.zip', '')
            pasta_destino = os.path.join(pasta, nome_pasta)
            
            # cria pasta se não existir
            if not os.path.exists(pasta_destino):
                os.makedirs(pasta_destino)
                print(f'📁 Criando pasta: {pasta_destino}')
            else:
                print(f'⚠️ Pasta já existe: {pasta_destino}')
            
            print(f'📦 Extraindo {arquivo}...')
            
            with zipfile.ZipFile(caminho_zip, 'r') as zip_ref:
                zip_ref.extractall(pasta_destino)
            
            print(f'✅ Extraído: {arquivo}\n')


if __name__ == "__main__":
    extrair_todos_zips()