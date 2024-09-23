# Carlton Data Master

[![Documentation Status](https://readthedocs.org/projects/carlton/badge/?version=latest)](https://carlton.readthedocs.io/pt-br/latest/?badge=latest)
[![CI](https://github.com/lealdouglas/carlton/actions/workflows/pipeline.yaml/badge.svg)](https://github.com/lealdouglas/carlton/actions/workflows/pipeline.yaml)
[![PyPI version](https://badge.fury.io/py/carlton.svg)](https://badge.fury.io/py/carlton)
[![codecov](https://codecov.io/gh/lealdouglas/carlton/graph/badge.svg?token=4RXRDPQDV4)](https://codecov.io/gh/lealdouglas/carlton)

# Carlton Framework

## Introdução

Bem-vindo à [documentação da Carlton](https://carlton.readthedocs.io/pt-br/latest/), uma poderosa biblioteca de ingestão de dados na cloud. Carlton foi projetada para simplificar o processo de captura e transformação de dados armazenados em um data lake, seguindo os padrões de ingestão da arquitetura de medalhão da Databricks.

Esse framework foi elaborado para o projeto [Data Master Douglas Leal](https://carlton.readthedocs.io/pt-br/latest/03_projeto/), onde Carlton é utilizado como [biblioteca padrão de utilizacão](https://carlton.readthedocs.io/pt-br/latest/03_projeto/#33-ideacao-do-projeto).

## Funcionalidades Principais

A arquitetura de medalhão organiza os dados em três camadas: **Bronze**, **Silver**, e **Gold**. Cada uma dessas camadas desempenha um papel crucial na preparação dos dados para consumo final. Atualmente, Carlton fornece cobertura as principais funcionalidade:

- **Conformidade com Padrões:** Todos os dados ingeridos e transformados seguem os padrões de ingestão e qualidade da arquitetura de medalhão da Databricks.
- **Qualidade nos dados:** Gerando logs de dados necessários para tomada de decisão.

## Casos de Uso

Carlton é ideal para:

- Equipes de dados que precisam automatizar o processo de ingestão em ambientes de nuvem.
- Organizações que utilizam a arquitetura de medalhão da Databricks para gerenciar seus dados.

## Começando

Para começar a utilizar Carlton, siga as instruções detalhadas na seção [início rápido](https://carlton.readthedocs.io/pt-br/latest/02_comecando/).

### 1. Pré-requisitos

Atualmente, Carlton foi desenvolvido para ser utilizado em um ambiente cloud, que executa em python. Sua fonte principal de consumo dos dados, quando ingestão, é o adls e/ou eventhub.

### 1. Início rápido

Baixe carlton do [pypi](https://pypi.org/project/carlton/)

```bash
pip install carlton==2.0.0
```

Execute um script de ingestão com seguinte comando:

```bash
poetry run carlton 'ingest' "-function" "ingest" "-storage_name_src" "stadrisk" "-container_src" "ctrdriskraw" "-file_resource" "adls" "-type_run" "batch" "-storage_name_tgt" "stadrisk" "-container_tgt" "dtmaster-catalog" "-schema_name" "bronze" "-table_name" "account" "-file_extension" "csv" "-path_src" "account" "-file_header" "true" "-file_delimiter" ","
```

## Referências

- [Poetry Documentation](https://python-poetry.org/docs/)
- [Data Contract](https://datacontract.com/)
- [Medallion Architecture](https://www.databricks.com/br/glossary/medallion-architecture)
