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
    if not data or "topic_name" not in data:
        return jsonify({"status": "error", "message": "Invalid payload, missing topic name"}), 400

    topic_name = data["topic_name"]
    delete_subscriptions = data.get("delete_subscriptions")

    result = pubsub_creator_service.delete_pubsub_topic(topic_name, delete_subscriptions)
    return jsonify(result), 200 if result["status"] == "success" else 500

def delete_pubsub_subscription() -> Tuple[Response, int]:
    data = request.json
    if not data or "subscription_name" not in data:
        return jsonify({"status": "error", "message": "Invalid payload, missing subscription name"}), 400

    subscription_name = data["subscription_name"]

    result = pubsub_creator_service.delete_pubsub_subscription(subscription_name)
    return jsonify(result), 200 if result["status"] == "success" else 500

def list_pubsub_topics() -> Tuple[Response, int]:
    result = pubsub_creator_service.list_pubsub_topics()

    if result["status"] == "success":
        # Formatear la respuesta para una mejor legibilidad
        formatted_topics = []
        for topic in result["topics"]:
            formatted_topic = {
                "name": topic["name"].split("/")[
                    -1
                ],  # Extraer solo el nombre del tópico
                "full_name": topic["name"],
                "labels": topic["labels"],
                "subscriptions": [
                    {
                        "name": sub["name"].split("/")[
                            -1
                        ],  # Extraer solo el nombre de la suscripción
                        "full_name": sub["name"],
                        "push_config": sub["push_config"],
                        "ack_deadline_seconds": sub["ack_deadline_seconds"],
                        "message_retention_duration": f"{sub['message_retention_duration']} seconds",
                        "labels": sub["labels"],
                    }
                    for sub in topic["subscriptions"]
                ],
            }
            formatted_topics.append(formatted_topic)

        return jsonify({"status": "success", "topics": formatted_topics}), 200
    else:
        return jsonify(result), 500


def get_pubsub_topic(topic_name: str) -> Tuple[Response, int]:
    result = pubsub_creator_service.get_pubsub_topic(topic_name)

    if result["status"] == "success":
        topic = result["topic"]
        formatted_topic = {
            "name": topic["name"].split("/")[-1],  # Extraer solo el nombre del tópico
            "full_name": topic["name"],
            "labels": topic["labels"],
            "message_storage_policy": topic["message_storage_policy"],
            "kms_key_name": topic["kms_key_name"],
            "schema_settings": topic["schema_settings"],
            "satisfies_pzs": topic["satisfies_pzs"],
            "message_retention_duration": f"{topic['message_retention_duration']} seconds",
            "subscriptions": [
                {
                    "name": sub["name"].split("/")[
                        -1
                    ],  # Extraer solo el nombre de la suscripción
                    "full_name": sub["name"],
                    "push_config": sub["push_config"],
                    "ack_deadline_seconds": sub["ack_deadline_seconds"],
                    "message_retention_duration": f"{sub['message_retention_duration']} seconds",
                    "labels": sub["labels"],
                }
                for sub in topic["subscriptions"]
            ],
        }

        return jsonify({"status": "success", "topic": formatted_topic}), 200
    else:
        return jsonify(result), 404 if "not found" in result["message"].lower() else 500


def update_pubsub_topic() -> Tuple[Response, int]:
    data = request.json
    if not data or "topic_name" not in data:
        return jsonify({"status": "error", "message": "Invalid payload"}), 400

    topic_name = data.get("topic_name")
    labels = data.get("labels")
    message_retention_duration = data.get("message_retention_duration")
    add_subscription = data.get("add_subscription")
    update_subscription = data.get("update_subscription")

    # Validar y procesar la configuración de suscripción
    if add_subscription:
        if not isinstance(add_subscription, dict):
            return jsonify({"status": "error", "message": "Invalid add_subscription format"}), 400
        if "name" not in add_subscription:
            return jsonify({"status": "error", "message": "Subscription name is required"}), 400

    if update_subscription:
        if not isinstance(update_subscription, dict):
            return jsonify({"status": "error", "message": "Invalid update_subscription format"}), 400
        if "name" not in update_subscription:
            return jsonify({"status": "error", "message": "Subscription name is required for update"}), 400

    result = pubsub_creator_service.update_pubsub_topic(
        topic_name=topic_name,
        labels=labels,
        message_retention_duration=message_retention_duration,
        add_subscription=add_subscription,
        update_subscription=update_subscription,
    )

    if result["status"] == "error":
        if "not initialized" in result["message"].lower():
            return jsonify({"status": "error", "message": "PubSub service is not available"}), 503
        return jsonify(result), 400

    return jsonify(result), 200