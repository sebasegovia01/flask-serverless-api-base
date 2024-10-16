# controllers/msg_broker_controller.py
from typing import Tuple
from flask import Response, request, jsonify
from config.config import Config
from services.pubsub_creator import PubSubCreatorService
from models.pubsub_models import *

gcp_credentials = Config.get_gcp_credentials()
if gcp_credentials is None:
    raise ValueError("GCP credentials not found in configuration")

pubsub_credentials = PubSubCredentials(
    project_id=gcp_credentials.get('project_id'),
    credentials=gcp_credentials
)
pubsub_creator_service = PubSubCreatorService(pubsub_credentials)

def create_pubsub_topic() -> Tuple[Response, int]:
    try:
        topic_create = TopicCreate(**request.json)
    except ValueError as e:
        return jsonify({"status": "error", "message": str(e)}), 400

    result = pubsub_creator_service.create_pubsub_topic(topic_create)
    return jsonify(result.model_dump(exclude_none=True)), 201 if result.status == "success" else 500


def delete_pubsub_topic() -> Tuple[Response, int]:
    try:
        topic_delete = TopicDelete(**request.json)
    except ValueError as e:
        return jsonify({"status": "error", "message": str(e)}), 400

    result = pubsub_creator_service.delete_pubsub_topic(topic_delete)
    return jsonify(result.model_dump(exclude_none=True)), 200 if result.status == "success" else 500


def delete_pubsub_subscription() -> Tuple[Response, int]:
    try:
        subscription_delete = SubscriptionDelete(**request.json)
    except ValueError as e:
        return jsonify({"status": "error", "message": str(e)}), 400

    result = pubsub_creator_service.delete_pubsub_subscription(subscription_delete)
    return jsonify(result.model_dump(exclude_none=True)), 200 if result.status == "success" else 500


def list_pubsub_topics() -> Tuple[Response, int]:
    result = pubsub_creator_service.list_pubsub_topics()
    return jsonify(result.model_dump(exclude_none=True)), 200 if result.status == "success" else 500


def get_pubsub_topic(topic_name: str) -> Tuple[Response, int]:
    result = pubsub_creator_service.get_pubsub_topic(topic_name)
    return jsonify(result.model_dump(exclude_none=True)), 200 if result.status == "success" else 500


def update_pubsub_topic() -> Tuple[Response, int]:
    try:
        topic_update = TopicUpdate(**request.json)
    except ValueError as e:
        return jsonify({"status": "error", "message": str(e)}), 400

    result = pubsub_creator_service.update_pubsub_topic(topic_update)
    return jsonify(result.model_dump(exclude_none=True)), 200 if result.status == "success" else 500
