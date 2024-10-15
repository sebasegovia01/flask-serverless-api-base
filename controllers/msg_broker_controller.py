# app/controllers/msg_broker_controller.py
from typing import Tuple
from flask import Response, request, jsonify
from services.pubsub_creator import PubSubCreatorService

pubsub_creator_service = PubSubCreatorService()

def create_pubsub_topic() -> Tuple[Response, int]:
    data = request.json
    if not data or "project_id" not in data or "topic_name" not in data:
        return jsonify({"status": "error", "message": "Invalid payload"}), 400

    project_id = data["project_id"]
    topic_name = data["topic_name"]
    labels = data.get("labels", {})

    result = pubsub_creator_service.create_pubsub_topic(project_id, topic_name, labels)
    return jsonify(result), 201 if result["status"] == "success" else 500

def delete_pubsub_topic() -> Tuple[Response, int]:
    data = request.json
    if "topic_name" not in data:
        return jsonify({"status": "error", "message": "Invalid payload, missing topic name"}), 400

    topic_name = data["topic_name"]

    result = pubsub_creator_service.delete_pubsub_topic(topic_name)
    return jsonify(result), 200 if result["status"] == "success" else 500

def list_pubsub_topics() -> Tuple[Response, int]:

    result = pubsub_creator_service.list_pubsub_topics()
    return jsonify(result), 200 if result["status"] == "success" else 500


def update_pubsub_topic() -> Tuple[Response, int]:
    data = request.json
    if not data or "topic_name" not in data:
        return jsonify({"status": "error", "message": "Invalid payload: topic_name is required"}), 400

    topic_name = data["topic_name"]
    labels = data.get("labels")
    message_retention_duration = data.get("message_retention_duration")
    add_subscription = data.get("add_subscription")

    result = pubsub_creator_service.update_pubsub_topic(
        topic_name=topic_name, 
        labels=labels, 
        message_retention_duration=message_retention_duration, 
        add_subscription=add_subscription
    )

    if result["status"] == "error":
        if "not initialized" in result["message"].lower():
            return jsonify({"status": "error", "message": "PubSub service is not available"}), 503
        return jsonify(result), 400

    return jsonify(result), 200