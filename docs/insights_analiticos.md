# Guia Analítico: Oportunidades de Insights do ENEM 📊

Este documento sugere perguntas de negócio exploratórias, painéis focais e análises preditivas que agora podem ser facilmente construídas por Analistas de Dados em nossa esteira do Jupyter Notebook conectada à **Camada Prata**.

Como nossos dados estão limpos e tipados, a equipe não precisará mais se preocupar em tratar os `NaNs` ou em lidar com tabelas exaustivas.

## 📈 Eixo 1: Desempenho e Desigualdade Sócio-Geográfica
Nesta vertente, foca-se na macro-visão da prova através das disparidades pelo Brasil:

* **Disparidade por Rede de Ensino (`TP_ESCOLA` vs Notas):** Comparar métricas de tendência central (Médias, Medianas) entre candidatos da rede Pública x Privada, provando quantitativamente hiatos em exatas (`NU_NOTA_MT`) vs escrita (`NU_NOTA_REDACAO`).
* **Calor Geográfico (`SG_UF_PROVA` vs Notas):** Qual estado desponta com a maior média no Brasil? Agrupar (*Group By*) por Unidade Federativa para demonstrar possíveis assimetrias entre a região Sudeste e outras regiões, traçando correlações com o Produto Interno Bruto (PIB).
* **Viés Racial no Desempenho (`TP_COR_RACA` vs Notas):** Distribuições de histograma para analisar a homogeneidade das notas considerando a política de cores do Censo (Branca, Preta, Parda, Amarela, Indígena), importante para políticas públicas e distribuição de bolsas Prouni e cotas SISU.

## 🧠 Eixo 2: Comportamento Diferenciado (Treineiros e Identificação)
Analítica focada na micro-visão sociológica:

* **Análise de Ansiedade e Precocidade (`IN_TREINEIRO`):** Como os chamados "treineiros" performam comparados com a população oficial que depende das notas para aprovação? Eles possuem uma curva mais "leve" no primeiro dia de prova (Humanas/Redação)?
* **Assimetria por Gênero (`TP_SEXO`):** A sociedade carrega o estereótipo de "homens formam mais em Exatas e mulheres em Humanas/Saúde". Há realmente um pico da densidade na *Nota de Matemática* para os homens neste conjunto completo da base? 
* **Abstenções Absolutas:** Quais recortes demográficos registram as maiores taxas de abstenção (Zero nas notas do dia 2)? Quantificarmos essa massa de candidatos zerados pode gerar estudos do impacto do dia de chuva e logística para a acessibilidade das provas.

## 🤖 Eixo 3: Modelagem Preditiva (Machine Learning)
Se o time quiser escalar do Business Intelligence tradicional para IA:

- **Classificador de Excelentes (Nota 1000 da Redação):** Seria possível prever com exatidão a probabilidade de um aluno ser "Nota 1000" na redação (*Target Binário*) levando em conta toda o *Background* de perfil de Cor/Raça, Idade (`TP_FAIXA_ETARIA`) e UF que o nosso dataset já possui? (Para uso com *Random Forest*).
- **Clusterização de Performance:** Aplicar um agrupamento (ex: Algoritmo K-Means) sobre os eixos das 4 provas objetivas para enxergar quais são as "personas acadêmicas" naturais originadas pelo INEP sem influência das categorias fixas demográficas.

> Todos esses insights podem, e devem, ser visualizados graficamente com o uso das bibliotecas `matplotlib` ou `seaborn` durante a apresentação/estudo usando a API de importação do Pandas via `read_parquet`.
