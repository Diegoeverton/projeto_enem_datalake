import pandas as pd
import zipfile
import os

def carregar_microdados_enem(pasta='dados'):
    
    lista_dfs = []
    
    # percorre todos os arquivos da pasta
    for arquivo_zip in os.listdir(pasta):
        
        if arquivo_zip.endswith('.zip'):
            
            caminho_zip = os.path.join(pasta, arquivo_zip)
            
            # extrai o ano do nome do arquivo
            ano = arquivo_zip.split('_')[-1].replace('.zip', '')
            
            with zipfile.ZipFile(caminho_zip, 'r') as zip_ref:
                
                for nome in zip_ref.namelist():
                    
                    if 'DADOS/' in nome and 'RESULTADOS' in nome and nome.endswith('.csv'):
                        
                        print(f'Carregando ano {ano}...')
                        
                        with zip_ref.open(nome) as arquivo:
                            
                            df = pd.read_csv(
                                arquivo,
                                sep=';',
                                encoding='latin1',
                                low_memory=False
                            )
                            
                            df['ANO'] = int(ano)
                            
                            lista_dfs.append(df)
                            
                        break
    
    # concatena todos
    df_final = pd.concat(lista_dfs, ignore_index=True)
    
    return df_final


# executando
enem = carregar_microdados_enem()
print(enem.head())
print(enem['ANO'].value_counts())