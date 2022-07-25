![Debussy Concert](banner_debussy.png "Debussy Concert")
# Debussy Concert
Abstraction layers for Apache Airflow with musical theme. Depends on debussy_framework

Mount examples folder on airflow dags folder


# Overview

[Pt-BR]

O framework tem por filosofia persistir* o dado depois de cada transformação.

Com essa filosofia em mente, o framework prioriza o ELT (Extract - Load - Transform) em vez do ETL.

Os pipelines de ingestão de dados devem implementar uma etapa de extrair a informação da fonte e persistir na camada Raw Vault em formato parquet sempre que o dado for carregado (duplicando o dado quando necessário), sem fazer transformações no conteúdo, podendo ser necessário fazer alguns ajustes para que o dado seja salvo em formato parquet. Depois o arquivo será carregado na camada raw removendo os processamentos duplicados (ainda pode ter dado duplicado!)

O pipeline de ETL Reverso por sua vez deve extrair do lakehouse o dado levar para o dataset Reverse ETL onde todo o dado deve ser persistido após as transformações necessárias. Após isso o dado deve ser levado para a camada de storage no formato mais próximo possível do que será enviado ao destino

O pipeline de Transformação (ainda a ser desenvolvido) será feito utilizando o software `dbt` (Data Build Tool)

*O dado persistido pode ser descartado depois de algum tempo, por exemplo depois de 6 meses pode ir para um storage mais barato e depois de 2 anos ser removido de alguma determinada camada