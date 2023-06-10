# Introdução
Este repositório irá apresentar a construção de uma pipeline de engenharia de dados, usando como exemplo a API do twitter. Com ela, iremos extrair dados de tweets sobre algum assunto que esteja bombando no momento.

Abaixo tem um checklist do básico que precisa ser feito:

[x] Criação da classe TwitterAPI para a extração de dados do twitter.

[ ] Ajuste da classe TwitterAPI, para a criação de um dataframe e salvando em alguma pasta raw(localmente) usando pyspark.

[ ] Criação da classe (validation) de Processamento dos dados salvos na raw e criando uma proxima camada, chamada trusted.

[ ] Essa etapa deveria ter vindo primeiro, usar o terraform para criar a IaC.

[ ] Essa etapa deveria ter vindo primeiro, criar uma classe para se connectar a AWS e criar buckets.

[ ] Deve surgir mais etapas
