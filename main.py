import logging
import azure.functions as func
import json
import os
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient
from azure.cosmos import CosmosClient

load_dotenv()

STORAGE_CONNECTION_STRING = os.getenv("STORAGE_CONNECTION_STRING")
COSMOSDB_CONNECTION_STRING = os.getenv("COSMOSDB_CONNECTION_STRING")
DATABASE_NAME = os.getenv("DATABASE_NAME", "MeuDatabase")
CONTAINER_NAME = os.getenv("CONTAINER_NAME", "MeuContainer")
BLOB_CONTAINER_NAME = os.getenv("BLOB_CONTAINER_NAME", "meus-arquivos")

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Recebendo requisição para salvar arquivo no Storage Account.")
    try:
        file = req.files['file']
        blob_service_client = BlobServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)
        blob_client = blob_service_client.get_blob_client(container=BLOB_CONTAINER_NAME, blob=file.filename)
        blob_client.upload_blob(file.stream.read())
        return func.HttpResponse(f"Arquivo {file.filename} salvo com sucesso!", status_code=200)
    except Exception as e:
        logging.error(str(e))
        return func.HttpResponse(f"Erro ao salvar arquivo: {str(e)}", status_code=500)

def salvar_no_cosmos(req: func.HttpRequest) -> func.HttpResponse:
    try:
        data = req.get_json()
        client = CosmosClient.from_connection_string(COSMOSDB_CONNECTION_STRING)
        database = client.get_database_client(DATABASE_NAME)
        container = database.get_container_client(CONTAINER_NAME)
        container.create_item(data)
        return func.HttpResponse("Registro salvo com sucesso!", status_code=200)
    except Exception as e:
        logging.error(str(e))
        return func.HttpResponse(f"Erro ao salvar no CosmosDB: {str(e)}", status_code=500)

def filtrar_cosmos(req: func.HttpRequest) -> func.HttpResponse:
    try:
        filtro = req.params.get('filtro')
        client = CosmosClient.from_connection_string(COSMOSDB_CONNECTION_STRING)
        database = client.get_database_client(DATABASE_NAME)
        container = database.get_container_client(CONTAINER_NAME)
        query = f"SELECT * FROM c WHERE c.campo = '{filtro}'"
        items = list(container.query_items(query=query, enable_cross_partition_query=True))
        return func.HttpResponse(json.dumps(items, indent=2), mimetype="application/json", status_code=200)
    except Exception as e:
        logging.error(str(e))
        return func.HttpResponse(f"Erro ao filtrar registros: {str(e)}", status_code=500)

def listar_cosmos(req: func.HttpRequest) -> func.HttpResponse:
    try:
        client = CosmosClient.from_connection_string(COSMOSDB_CONNECTION_STRING)
        database = client.get_database_client(DATABASE_NAME)
        container = database.get_container_client(CONTAINER_NAME)
        query = "SELECT * FROM c"
        items = list(container.query_items(query=query, enable_cross_partition_query=True))
        return func.HttpResponse(json.dumps(items, indent=2), mimetype="application/json", status_code=200)
    except Exception as e:
        logging.error(str(e))
        return func.HttpResponse(f"Erro ao listar registros: {str(e)}", status_code=500)
