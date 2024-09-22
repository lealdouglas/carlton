# Usando Carlton

## 1. Pré-requisitos

Atualmente, Carlton foi desenvolvido para ser utilizado em um ambiente cloud, que executa em python. Sua fonte principal de consumo dos dados, quando ingestão, é o adls e/ou eventhub.

## 1. Início rápido

Baixe carlton do [pypi](https://pypi.org/project/carlton/)

```bash
pip install carlton==2.0.0
```

Execute um script de ingestão com seguinte comando:

```bash
poetry run carlton 'ingest' "-function" "ingest" "-storage_name_src" "stadrisk" "-container_src" "ctrdriskraw" "-file_resource" "adls" "-type_run" "batch" "-storage_name_tgt" "stadrisk" "-container_tgt" "dtmaster-catalog" "-schema_name" "bronze" "-table_name" "account" "-file_extension" "csv" "-path_src" "account" "-file_header" "true" "-file_delimiter" ","
```
