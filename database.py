import os
import pymysql
import logging
from pymysql.cursors import DictCursor

def log_event(mensagem, nivel=logging.INFO):
    logging.log(nivel, mensagem)

# Configuração de conexão com o banco de dados
DB_HOST = os.getenv('DB_HOST')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')
DB_PORT = int(os.getenv('DB_PORT', 3306))

# Check variáveis de ambiente
logging.info(f"DB_HOST: {DB_HOST}")
logging.info(f"DB_USER: {DB_USER}")
logging.info(f"DB_NAME: {DB_NAME}")
logging.info(f"DB_PORT: {DB_PORT}")

def get_db_connection():
    """Estabelece conexão com o banco de dados"""
    try:
        connection = pymysql.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
            port=DB_PORT,
            charset='utf8mb4',
            cursorclass=DictCursor,
            autocommit=False
        )
        log_event("Conexão com o banco de dados estabelecida com sucesso.")
        return connection
    except Exception as e:
        log_event(f"Erro ao conectar ao banco de dados: {e}", logging.ERROR)
        raise

def validate_data(field_name, value, expected_type, max_length=None):
    """
    Valida dados antes da inserção no banco
    """
    logging.info(f"Validando {field_name}: valor={value}, tipo={type(value)}")
    if value is not None and not isinstance(value, expected_type):
        raise ValueError(f"{field_name} deve ser do tipo {expected_type.__name__}.")
    if max_length and isinstance(value, str) and len(value) > max_length:
        raise ValueError(f"{field_name} excede o tamanho máximo de {max_length} caracteres.")

def insert_checklist(connection, id_checklist):
    """
    Insere um checklist no banco de dados, se ainda não existir.
    """
    try:
        with connection.cursor() as cursor:
            sql_check = "SELECT 1 FROM checklists WHERE id_checklist = %s"
            cursor.execute(sql_check, (id_checklist,))
            exists = cursor.fetchone()

            if not exists:
                sql_insert = """
                    INSERT INTO checklists (id_checklist)
                    VALUES (%s)
                """
                # Validação de dados
                validate_data("id_checklist", id_checklist, str, max_length=45)

                cursor.execute(sql_insert, (id_checklist,))
                log_event(f"Checklist {id_checklist} inserido com sucesso.")
            else:
                log_event(f"Checklist {id_checklist} já existe. Nenhuma inserção realizada.")

    except Exception as e:
        log_event(f"Erro ao inserir checklist {id_checklist}: {e}", logging.ERROR)
        raise

def insert_avaliacao(connection, id_avaliacao, id_checklist):
    """
    Insere uma avaliação no banco de dados, se ainda não existir.
    """
    try:
        with connection.cursor() as cursor:
            sql_check = "SELECT 1 FROM avaliacoes WHERE id_avaliacao = %s"
            cursor.execute(sql_check, (id_avaliacao,))
            exists = cursor.fetchone()

            if not exists:
                sql_insert = """
                    INSERT INTO avaliacoes (id_avaliacao, id_checklist, status)
                    VALUES (%s, %s, %s)
                """
                # Validação de dados
                validate_data("id_avaliacao", id_avaliacao, str, max_length=45)
                validate_data("id_checklist", id_checklist, str, max_length=45)

                status = 'Em andamento'  # Definir o status como "Em andamento" para identificar forms que chegam de avaliação realmente em andamento

                cursor.execute(sql_insert, (
                    id_avaliacao, id_checklist, status
                ))
                log_event(f"Avaliação {id_avaliacao} inserida com sucesso.")
            else:
                log_event(f"Avaliação {id_avaliacao} já existe. Nenhuma inserção realizada.")

    except Exception as e:
        log_event(f"Erro ao inserir avaliação {id_avaliacao}: {e}", logging.ERROR)
        raise

def insert_entregavel(connection, id_entregavel, id_avaliacao, data_recebimento, nome_respondente, comentario_obrigatorio, comentario_opcional, id_checklist):
    """
    Insere um entregável no banco de dados, se ainda não existir.
    """
    try:
        with connection.cursor() as cursor:
            sql_check = "SELECT 1 FROM entregaveis WHERE id_entregavel = %s"
            cursor.execute(sql_check, (id_entregavel,))
            exists = cursor.fetchone()

            if not exists:
                sql_insert = """
                    INSERT INTO entregaveis (id_entregavel, id_avaliacao, data_recebimento, nome_respondente, comentario_obrigatorio, comentario_opcional, id_checklist)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """
                # Validação de dados
                validate_data("id_entregavel", id_entregavel, str, 255)
                validate_data("id_avaliacao", id_avaliacao, str, max_length=45)

                cursor.execute(sql_insert, (
                    id_entregavel, id_avaliacao, data_recebimento,
                    nome_respondente, comentario_obrigatorio, comentario_opcional, id_checklist
                ))
                log_event(f"Entregável {id_entregavel} inserido com sucesso.")
            else:
                log_event(f"Entregável {id_entregavel} já existe. Nenhuma inserção realizada.")
        # Não é necessário commit aqui; o gerenciamento da transação é feito pelo contexto 'with' na conexão
    except Exception as e:
        log_event(f"Erro ao inserir entregável {id_entregavel}: {e}", logging.ERROR)
        raise

def ensure_pergunta_exists(connection, id_pergunta, id_avaliacao, field):
    """
    Garante que uma pergunta exista no banco de dados.
    """
    try:
        with connection.cursor() as cursor:
            sql_check = "SELECT 1 FROM perguntas WHERE id_pergunta = %s AND id_avaliacao = %s"
            cursor.execute(sql_check, (id_pergunta, id_avaliacao))
            exists = cursor.fetchone()

            if not exists:
                sql_insert = """
                    INSERT INTO perguntas (id_pergunta, id_avaliacao, texto_pergunta, tipo_pergunta, ordem, ref)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """
                texto_pergunta = field.get('title')
                tipo_pergunta = field.get('type')
                ordem = field.get('ordem', 0)
                ref = field.get('ref')

            # Validação e ajuste de texto_pergunta
                if not texto_pergunta:  # Trata valores nulos ou vazios
                    texto_pergunta = "Pergunta não especificada"  # Valor padrão

                # Validação de dados
                validate_data("id_pergunta", id_pergunta, str, max_length=50)

                cursor.execute(sql_insert, (
                    id_pergunta, id_avaliacao, texto_pergunta,
                    tipo_pergunta, ordem, ref
                ))
                log_event(f"Pergunta {id_pergunta} inserida com sucesso.")
            else:
                log_event(f"Pergunta {id_pergunta} já existe. Nenhuma inserção realizada.")

    except Exception as e:
        log_event(f"Erro ao verificar/inserir pergunta {id_pergunta}: {e}", logging.ERROR)
        raise

def insert_resposta(connection, id_entregavel, id_pergunta, id_avaliacao, valor_resposta, texto_resposta, tipo_resposta, ref):
    """
    Insere uma resposta no banco de dados.
    """
    try:
        with connection.cursor() as cursor:
            sql = """
                INSERT INTO respostas (id_entregavel, id_pergunta, id_avaliacao, valor_resposta, texto_resposta, tipo_resposta, ref)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(sql, (
                id_entregavel, id_pergunta, id_avaliacao,
                valor_resposta, texto_resposta, tipo_resposta, ref
            ))
            log_event(f"Resposta para a pergunta {id_pergunta} inserida com sucesso.")
        # O commit será feito após todas as operações, não aqui
    except Exception as e:
        log_event(f"Erro ao inserir resposta para a pergunta {id_pergunta}: {e}", logging.ERROR)
        raise

def associate_pergunta_entregavel(connection, id_pergunta, id_entregavel):
    """
    Associa uma pergunta a um entregável.
    """
    try:
        with connection.cursor() as cursor:
            sql_check = "SELECT 1 FROM perguntas_entregaveis WHERE id_pergunta = %s AND id_entregavel = %s"
            cursor.execute(sql_check, (id_pergunta, id_entregavel))
            exists = cursor.fetchone()

            if not exists:
                sql_insert = """
                    INSERT INTO perguntas_entregaveis (id_pergunta, id_entregavel)
                    VALUES (%s, %s)
                """
                cursor.execute(sql_insert, (id_pergunta, id_entregavel))
                log_event(f"Pergunta {id_pergunta} associada ao entregável {id_entregavel} com sucesso.")
            else:
                log_event(f"Associação entre pergunta {id_pergunta} e entregável {id_entregavel} já existe.")
    except Exception as e:
        log_event(f"Erro ao associar pergunta {id_pergunta} ao entregável {id_entregavel}: {e}", logging.ERROR)
        raise

def log_processamento(connection, id_entregavel, status, mensagem):
    """
    Registra o processamento no banco de dados.
    """
    try:
        with connection.cursor() as cursor:
            sql = """
                INSERT INTO logs_processamento (id_entregavel, data_processamento, status, mensagem)
                VALUES (%s, NOW(), %s, %s)
            """
            cursor.execute(sql, (id_entregavel, status, mensagem))
            log_event(f"Log registrado para entregável {id_entregavel}: {status} - {mensagem}")
    except Exception as e:
        log_event(f"Erro ao registrar log para entregável {id_entregavel}: {e}", logging.ERROR)
        raise