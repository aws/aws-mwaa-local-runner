from elasticsearch import helpers, Elasticsearch
import os
from dotenv import load_dotenv
from dataclasses import dataclass
import logging

load_dotenv()


def bulk_upsert(es_client, actions):
    helpers.bulk(es_client, actions)


def es_client():
    host = os.getenv("es_host")
    id = os.getenv("es_id")
    key = os.getenv("es_key")
    es = Elasticsearch(
        [host],
        api_key=(id, key),
        request_timeout=60,
        max_retries=2,
        retry_on_timeout=True,
    )
    return es


def get_latest_connections_after(timestamp, es_client):
    query = {"query": {"range": {"timestamp": {"gte": timestamp}}}}
    results = es_client.search(index="latest_connection_logs", body=query)
    hits = results["hits"]["hits"]
    # 1000 connections are returned here
    return hits


def fetch_latest_meters_by_ids(es_client, meter_ids):
    query = {"query": {"bool": {"must": [{"terms": {"dataKey.meter.id": meter_ids}}]}}}
    results = es_client.search(index="latest_meter_logs", body=query)
    hits = results["hits"]["hits"]
    hits_by_meter_id = {}
    for hit in hits:
        meter_id = hit["_source"]["dataKey"]["meter"]["id"]
        hits_by_meter_id[meter_id] = hit
    return hits


def chunk_connections(connections, chunk_size):
    for i in range(0, len(connections), chunk_size):
        yield connections[i : i + chunk_size]


def enrich_meter_with_connection(latest_meter_documents, connection):
    try:
        meter_id = connection["_source"]["dataKey"]["meter"]["id"]
        meter_document = latest_meter_documents[meter_id]
        meter_document["_source"]["enriched"]["verificationStatus"] = connection[
            "_source"
        ]["connection"]["verificationStatus"]
    except KeyError:
        logging.info(f"Could not find meter for connection with meter id {meter_id}")
        meter_document = None
    return meter_document


def enrich_connections_onto_meters(connections, latest_meter_documents):
    latest_meter_documents = {
        meter["_source"]["dataKey"]["meter"]["id"]: meter
        for meter in latest_meter_documents
    }
    logging.info(f"Enriching {connections} connections ")
    logging.info(f"Enriching {latest_meter_documents} meters ")
    updated_meter_documents = []
    for connection in connections:
        enriched_meter = enrich_meter_with_connection(
            latest_meter_documents, connection
        )
        if enriched_meter:
            updated_meter_documents.append(enriched_meter)
    actions = [
        {
            "_id": meter_document["_id"],
            "_index": "latest_meter_logs",
            "_op_type": "update",
            "doc": meter_document["_source"],
            "doc_as_upsert": True,
        }
        for meter_document in updated_meter_documents
    ]
    return actions
