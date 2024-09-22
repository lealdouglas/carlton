# Carlton Data Master

[![Documentation Status](https://readthedocs.org/projects/carlton/badge/?version=latest)](https://carlton.readthedocs.io/pt-br/latest/?badge=latest)
[![CI](https://github.com/lealdouglas/carlton/actions/workflows/pipeline.yaml/badge.svg)](https://github.com/lealdouglas/carlton/actions/workflows/pipeline.yaml)
[![PyPI version](https://badge.fury.io/py/carlton.svg)](https://badge.fury.io/py/carlton)
[![codecov](https://codecov.io/gh/lealdouglas/carlton/graph/badge.svg?token=4RXRDPQDV4)](https://codecov.io/gh/lealdouglas/carlton)

## Introdução

Bem-vindo à documentação da Carlton, uma poderosa biblioteca de ingestão de dados na cloud. Carlton foi projetada para simplificar o processo de captura e transformação de dados armazenados em um data lake, seguindo os padrões de ingestão da arquitetura de medalhão da Databricks.

A arquitetura de medalhão organiza os dados em três camadas: **Bronze**, **Silver**, e **Gold**. Cada uma dessas camadas desempenha um papel crucial na preparação dos dados para consumo final, e Carlton automatiza as etapas necessárias para mover os dados entre essas camadas de forma eficiente.

## Funcionalidades Principais

- **Captura de Dados:** Carlton permite capturar dados de diversas fontes e armazená-los na camada Bronze do data lake.
- **Transformação de Dados:** Após a captura, os dados são transformados e movidos para as camadas Silver e Gold, garantindo que estejam prontos para consumo.
- **Conformidade com Padrões:** Todos os dados ingeridos e transformados seguem os padrões de ingestão e qualidade da arquitetura de medalhão da Databricks.
- **Interface Simplificada:** Carlton oferece uma interface fácil de usar para configuração e execução de pipelines de ingestão de dados.

## Casos de Uso

Carlton é ideal para:

- Equipes de dados que precisam automatizar o processo de ingestão em ambientes de nuvem.
- Organizações que utilizam a arquitetura de medalhão da Databricks para gerenciar seus dados.
- Aplicações que exigem dados prontos para análise, ciência de dados ou aprendizado de máquina.

## Começando

Para começar a utilizar Carlton, siga as instruções detalhadas na seção [Instalação](docs/installation.md).

## Contribuindo

Interessado em contribuir? Confira as diretrizes na seção [Contribuindo](docs/contributing.md) para saber como ajudar no desenvolvimento da Carlton.

## Documentação Completa

Para informações detalhadas sobre a configuração, uso e exemplos, explore a documentação completa nas seções subsequentes.

## Referências

- [Documentação da Databricks](https://docs.databricks.com/)
- [Arquitetura de Medalhão](https://databricks.com/solutions/data-lakehouse)
- [Guia de Contribuição do GitHub](https://docs.github.com/pt/github/collaborating-with-issues-and-pull-requests)
