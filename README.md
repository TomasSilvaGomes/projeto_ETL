# Projeto Prático de ETL & Visualização de Dados


![Logótipo Twitch](tw.jpg)

## Descrição

Este projeto tem como objetivo recolher dados dos streamers mais populares da plataforma Twitch, guardar os dados em ficheiros CSV, processá-los para limpeza e formatação, e posteriormente inserir essas informações numa base de dados SQL Servere obter visualizações apartir do Looker Studio.

O script está configurado para executar automaticamente num ciclo contínuo, realizando a recolha e atualização dos dados de dois em dois dias.

## Funcionalidades

- Autenticação via OAuth Client Credentials para acesso à API da Twitch.
- Recolha dos streams mais populares em tempo real, obtendo informações detalhadas dos utilizadores.
- Armazenamento dos dados brutos em CSV (`streamers.csv`).
- Limpeza e formatação dos dados, com conversão de datas e idiomas, e criação de CSV formatado (`streamers_twitch.csv`).
- Inserção incremental dos dados formatados numa tabela do SQL Server, evitando duplicações.
- Execução automática de 2 em 2 dias, com tratamento básico de erros e registo no console.

## Tecnologias Utilizadas

- Python 3.8+
- Bibliotecas: `requests`, `pandas`, `pyodbc`, `time`
- Base de Dados: Microsoft SQL Server
- API Twitch Helix

## Pré-requisitos

- Conta Twitch Developer com `client_id` e `client_secret` válidos.
- SQL Server configurado e acessível.
- Driver ODBC para SQL Server instalado (exemplo: ODBC Driver 17 for SQL Server).
- Pacotes Python instalados:
  ```bash
  pip install requests pandas pyodbc time
