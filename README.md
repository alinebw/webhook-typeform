# Webhook Typeform - Integração com MySQL

Este repositório contém a implementação de uma integração entre o Typeform e um banco de dados MySQL hospedado na AWS. O projeto foi desenvolvido para automatizar o processo de coleta e armazenamento de dados de formulários Typeform, com foco em avaliações de clientes.

## Objetivo do Projeto

Automatizar a integração de formulários Typeform com um banco de dados relacional MySQL, garantindo maior consistência e confiabilidade na coleta de respostas.

## Estrutura do Projeto
### Principais Funcionalidades
1. **Recepção de Dados:** Implementação de um webhook para receber respostas do Typeform e enviá-las automaticamente ao banco de dados
   
2. **Armazenamento e Processamento:** Uso de stored procedures para inserir, processar e calcular médias de avaliações (CSAT e NPS) no banco de dados
   
3. **Automação e Monitoramento:** Implementação de AWS Lambda para processar o webhook do Typeform

### Tecnologias Utilizadas
Python: Linguagem principal para a lógica do webhook e integração com o MySQL
PyMySQL: Biblioteca para a conexão entre Python e o banco de dados MySQL
Typeform: Ferramenta de formulários online para coleta de respostas
MySQL: Banco de dados relacional hospedado na AWS
AWS Lambda: Função serverless para processamento do webhook

## Configuração e Instalação
### Pré-requisitos
- AWS CLI configurado com as credenciais corretas para acessar o banco de dados e a função Lambda
  
- MySQL Workbench para gerenciamento e manutenção do banco de dados
  
- Acesso ao Typeform para configurar o webhook de integração

### Passo a Passo
1. **Configuração do Webhook no Typeform**:
- Acesse o painel do Typeform e crie um webhook direcionado para a URL da função Lambda configurada na AWS
- Certifique-se de que o webhook envia os dados no formato JSON necessário para o processamento

2. **Configuração do Banco de Dados:**
- Importe as tabelas usando o script SQL localizado em /scripts/database_setup.sql
- Execute as stored procedures para cálculos de CSAT e NPS, conforme necessário

3. **Implementação da Lambda:**

- Configure a função para processar os dados recebidos do Typeform:
     - Configurações gerais: memória, tempo limite
     - Permissões
     - URL da função:  métodos, origem, cabeçalhos, modo de invocação
     - Variáveis de ambiente
     - VPC
