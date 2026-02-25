# Documentação da Infraestrutura Docker: Apache Spark e Python

Esta documentação detalha a configuração do ambiente conteinerizado responsável pela extração e processamento dos microdados do ENEM. A infraestrutura foi projetada visando estabilidade, fácil manutenção e compatibilidade, utilizando o sistema operacional **Ubuntu Linux** como base.

## 1. O Arquivo `Dockerfile`

O arquivo `Dockerfile` na raiz do projeto é a **receita** utilizada para construir a imagem customizada para o nosso container. O objetivo é ter um ambiente leve, onde possamos rodar o Python e o motor interno do Apache Spark em completa sincronia.

Foi decidido o uso do **Ubuntu 22.04 LTS** como imagem base no lugar de imagens pré-construídas do Spark (como as da Bitnami) para que tenhamos total controle sobre as versões sendo instaladas e evitemos conflitos de bibliotecas Python.

### Estrutura do Dockerfile:
* **`FROM ubuntu:22.04`**: Partimos de um sistema operacional completamente limpo.
* **`ENV DEBIAN_FRONTEND=noninteractive`**: Impede que o instalador do Linux pule telas perguntando sobre fuso horário (ex. biblioteca `tzdata`), o que travaria o processo de *build* do Docker.
* **`RUN apt-get update && apt-get install -y openjdk-17-jre-headless python3 python3-pip`**: 
  * O Spark requer a linguagem **Java (JRE/JDK)** para rodar seu motor principal por baixo dos panos na JVM. Instalamos a versão robusta 17.
  * O **Python 3** e o interpretador de pacotes **pip** foram instalados nativamente.
* **Variáveis de Ambiente (`ENV JAVA_HOME=...`)**: Dizem ao sistema operacional e ao Spark exatamente onde encontrar os arquivos bases do Java.
* **`RUN pip3 install pyspark==3.5.1`**: Instala globalmente o PySpark. Ao fazer isso desta forma no Ubuntu, o pacote já nos fornece o script `spark-submit`, dispensando a necessidade de baixar e configurar o Apache Spark manualmente da fonte.
* **`COPY`  e `CMD`**: Transportam as libs e scripts da nossa máquina para dentro da pasta padrão `/app`, e o comando final aciona a execução do processo `src/extrair_dados_spark.py`.

## 2. O Arquivo `docker-compose.yml`

Enquanto o `Dockerfile` constrói o *"sistema"*, o `docker-compose.yml` funciona como um **orquestrador**, gerenciando como este sistema deve ser inicializado, interagir e se conectar à nossa máquina hospedeira.

### Estrutura do Compose:
* **`spark`**: Serviço único criado nesta infraestrutura provisória.
* **`build: .`**: Em vez de baixar uma imagem pronta da internet, esta linha informa ao Docker que ele deve olhar para a nossa própria pasta (onde o Dockerfile se encontra) e seguir aquela receita do zero. 
* **`volumes`**:
  * `- ./dados:/app/dados`
  * `- ./data_lake:/app/data_lake`
  * **Explicação**: Esta é a parte mais importante. Isolamos o nosso banco de microdados em nossa máquina local. Ao invés de copiarmos GigaBytes de arquivos *.zip* do ENEM para *dentro* da imagem do Docker toda vez que formos rodar, nós mapeamos (espelhamos) como "Volumes". Isso garante que:
    1. Os dados da pasta local `dados` consigam ser lidos em tempo real pelo container em `/app/dados`.
    2. Logo, qualquer arquivo descompactado lá pelo script Python automaticamente surgirá na sua máquina original fora do contêiner. O mesmo vale futuramente para o processamento feito em `/app/data_lake`.

## 3. Como Inicializar e Usar a Solução

A execução ocorre primariamente usando apenas os comandos padrão do ecossistema do Docker.

### O Comando Principal:
```bash
docker-compose up --build
```

**Como funciona a esteira:**
1. A bandeira `--build` obriga a infraestrutura a ler novamente as alterações no seu `Dockerfile` ou no `requirements.txt` a depender do caso, subindo o container do zero com as modificações mais recentes.
2. O passo de dependências do Ubuntu processa o Java e o PySpark de forma limpa.
3. O comando listado no final do arquivo Docker file (`spark-submit src/extrair_dados_spark.py`) inicia a pipeline.
4. O container lerá o volume espelhado contendo os arquivos `zip` do ENEM.
5. Ele finaliza em alguns minutos e a execução é encerrada, mas a pasta `dados` local agora estará populada com os CSVs extraídos sem que você precisasse instalar Java ou Apache Spark nativos em sua própria máquina Windows Host local.

### Como entrar dentro do Contêiner (Modo Interativo)

Por padrão, quando o projeto roda o `docker-compose up`, o contêiner liga, executa a jornada de script e desliga sozinho ao final (já que a tarefa dele acabou).

Se você deseja "entrar" dentro do Ubuntu do contêiner para explorar pastas locais (acessar o terminal bash), testar o `pyspark` interativamente via shell ou debugar código manualmente, você deve subir uma sessão interativa limpa através do comando:

```bash
docker-compose run --rm spark bash
```

**Como funciona esse comando:**
* `run`: Cria uma nova imagem estática do nosso serviço e a mantém viva.
* `--rm`: Regra importante que diz "quando eu digitar '*exit*' e sair do terminal do contêiner, destrua a instância para não ficar pesando no Docker".
* `spark`: Nome do serviço definido no *docker-compose.yml*.
* `bash`: Informa ao Docker que, invés de rodar o comando fixo do `CMD` (que era rodar o script python de extração), ele deve apenas ligar a tela padrão do terminal Ubuntu do contêiner para nós.
