import os
import json
import hmac
import hashlib
import base64
import logging
from datetime import datetime

from database import (
    get_db_connection,
    insert_checklist,
    insert_avaliacao,
    insert_entregavel,
    ensure_pergunta_exists,
    insert_resposta,
    associate_pergunta_entregavel,
    log_processamento,
    log_event
)

# Configuração do logger
logging.basicConfig(level=logging.INFO)

# Variáveis de ambiente
SECRET_TOKEN = os.getenv('SECRET_TOKEN')

# Função para validar a assinatura do Typeform
def verify_signature(received_signature, payload):
    """Verifica a assinatura recebida no cabeçalho"""
    digest = hmac.new(SECRET_TOKEN.encode('utf-8'), payload, hashlib.sha256).digest()
    computed_signature = base64.b64encode(digest).decode()
    return hmac.compare_digest(received_signature, computed_signature)

# Função para obter cabeçalhos de forma case-insensitive
def get_header(headers, header_name):  
    for k, v in headers.items():
        if k.lower() == header_name.lower():
            return v
    return None

# Função principal da Lambda
def lambda_handler(event, context):
    connection = None
    id_entregavel = None
    
    # Obter headers e body
    headers = event.get("headers", {})
    typeform_signature = get_header(headers, 'Typeform-Signature') or ''
    body = event.get("body", "")
    is_base64_encoded = event.get("isBase64Encoded", False)

    # Decodificar o corpo se estiver em base64
    if is_base64_encoded:
        body_bytes = base64.b64decode(body)
    else:
        body_bytes = body.encode('utf-8')

    # Validar assinatura
    if not typeform_signature:
        return {
            "statusCode": 403,
            "body": json.dumps({"error": "Assinatura ausente"})
        }

    try:
        sha_name, signature = typeform_signature.split("=", 1)
        if sha_name != "sha256":
            return {
                "statusCode": 501,
                "body": json.dumps({"error": "Operação não suportada. Apenas sha256 é permitido"})
            }
    except ValueError:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "Cabeçalho Typeform-Signature inválido"})
        }

    # Validar assinatura com payload bruto
    if not verify_signature(signature, body_bytes):
        return {
            "statusCode": 401,
            "body": json.dumps({"error": "Assinatura inválida"})
        }

    # Processar payload
    try:
        data = json.loads(body_bytes.decode('utf-8'))
        logging.info(f"Payload recebido: {data}")
    except json.JSONDecodeError:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "Payload inválido"})
        }

    event_id = data.get("event_id")
    form_response = data.get("form_response")

    if not event_id or not form_response:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "Campos ausentes: event_id, form_response"})
        }

    # Inserir dados no banco de dados
    try:
        connection = get_db_connection()

        # Extrair dados necessários do form_response
        id_entregavel = event_id
        data_recebimento = form_response.get('submitted_at')
        variables = form_response.get('variables', [])
        id_checklist = None
        id_avaliacao = form_response.get('form_id')
        answers = form_response.get('answers', [])
        definition = form_response.get('definition', [])

        # Iterar pela lista de variáveis para encontrar a chave "checklist"
        for var in variables:
            if var.get("key") == "checklist":
                id_checklist = var.get("text")
                break

        # Validar e processar datas

        if data_recebimento:
            data_recebimento = datetime.strptime(
                data_recebimento, '%Y-%m-%dT%H:%M:%SZ'
            ).strftime('%Y-%m-%d %H:%M:%S')

        # Conversão de tipos
        if id_checklist is not None:
            id_checklist = str(id_checklist)

        if id_avaliacao is not None:
            id_avaliacao = str(id_avaliacao)
            logging.info(f"id_avaliacao recebido como string: {id_avaliacao}")

        # Inserir checklist
        if id_checklist:
            insert_checklist(connection, id_checklist)

        # Inserir avaliação
        if id_avaliacao:
            insert_avaliacao(connection, id_avaliacao, id_checklist)

        # Inserir entregável
        insert_entregavel(
            connection,
            id_entregavel=id_entregavel,
            id_avaliacao=id_avaliacao,
            data_recebimento=data_recebimento,
            nome_respondente=None,
            comentario_obrigatorio=None,
            comentario_opcional=None,
            id_checklist=id_checklist
        )
       
        # Processar as perguntas do payload
        fields = form_response.get('definition', {}).get('fields', [])
        field_id_to_title = {}
        for idx, field in enumerate(fields, start=1):
            id_pergunta = field.get('id')
            if not id_pergunta:
                continue  # Ignorar se não houver id da pergunta

            title = field.get('title', '').lower()
            field_id_to_title[id_pergunta] = title

            texto_pergunta = field.get('title')
            tipo_pergunta = field.get('type')
            ref = field.get('ref')
            ordem = idx  # Usar o índice do loop como ordem

            # Garantir que pergunta existe na tabela
            ensure_pergunta_exists(
                    connection, id_pergunta, id_avaliacao, {
                        'title': texto_pergunta,
                        'type': tipo_pergunta,
                        'ref': ref,
                        'ordem': ordem
                    }
                )
            
             # Associar pergunta ao entregável
            associate_pergunta_entregavel(connection, id_pergunta, id_entregavel)

        # Processar respostas
        for answer in answers:
            field = answer.get('field', {})
            id_pergunta = field.get('id')
            ref = field.get('ref')
            tipo_pergunta = field.get('type')
            tipo_resposta = answer.get('type')
            valor_resposta = None
            texto_resposta = None
            
            # Obter o título da pergunta
            title = field_id_to_title.get(id_pergunta, '').lower()
           

            # Extrair resposta com base no tipo
            if tipo_resposta == 'text':
                texto_resposta = answer.get('text')
            elif tipo_resposta == 'choice':
                choice = answer.get('choice', {})
                texto_resposta = choice.get('label')
            elif tipo_resposta == 'number':
                valor_resposta = answer.get('number')
            elif tipo_resposta == 'boolean':
                valor_resposta = int(answer.get('boolean'))
            else:
                texto_resposta = str(answer.get(tipo_resposta))

            logging.info(f"Processando resposta: id_pergunta={id_pergunta}, texto_pergunta={texto_pergunta}, valor_resposta={valor_resposta}, texto_resposta={texto_resposta}")

            # Inserir resposta
            insert_resposta(
                connection,
                id_entregavel=id_entregavel,
                id_pergunta=id_pergunta,
                id_avaliacao=id_avaliacao,
                valor_resposta=valor_resposta,
                texto_resposta=texto_resposta,
                tipo_resposta=tipo_resposta,
                ref=ref
            )

        # Registrar processamento bem-sucedido
        log_processamento(
            connection,
            id_entregavel,
            'RECEBIDO',
            'Dados recebidos e inseridos com sucesso.'
        )

        connection.commit()

        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Dados processados e inseridos no banco"})
        }

    except Exception as e:
        try:
            if 'connection' in locals() and connection:
                connection.rollback()
        except NameError:
            pass
        log_event(f"Erro ao processar o evento {event_id}: {e}", logging.ERROR)
        log_processamento(connection, id_entregavel, 'Erro', str(e))
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }
    finally:
        if connection:
            connection.close()