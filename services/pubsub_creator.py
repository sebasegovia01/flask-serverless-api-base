# app/services/pubsub_service.py
import logging
import re
import types
from typing import Any, Optional
from google.cloud import pubsub_v1
from google.api_core import retry
from google.oauth2 import service_account
from google.protobuf import field_mask_pb2, duration_pb2
from config.config import Config


class PubSubCreatorService:
    def __init__(self) -> None:
        logging.info("Initializing PubSubCreator service...")
        try:
            credentials = service_account.Credentials.from_service_account_info(
                Config.get_gcp_credentials()
            )
            self.publisher = pubsub_v1.PublisherClient(credentials=credentials)
            self.subscriber = pubsub_v1.SubscriberClient(credentials=credentials)
            self.project_id = credentials.project_id
            print("PubSubCreator service initialized successfully.")
        except Exception as e:
            error_message = f"Failed to initialize PubSubCreator service: {str(e)}"
            print(error_message)
            logging.error(error_message)
            self.publisher = None

    @retry.Retry()
    def create_pubsub_topic(
        self, project_id, topic_name, labels=None
    ) -> dict[str, str]:
        topic_path = self.publisher.topic_path(project_id, topic_name)

        try:
            topic = self.publisher.create_topic(
                request={
                    "name": topic_path,
                    "labels": labels or {},
                }
            )
            return {
                "status": "success",
                "message": f"Topic created: {topic.name}",
                "topic_path": topic.name,
            }
        except Exception as e:
            return {"status": "error", "message": f"Failed to create topic: {str(e)}"}

    def delete_pubsub_topic(self, topic_name) -> dict[str, str]:
        topic_path = self.publisher.topic_path(self.project_id, topic_name)

        try:
            self.publisher.delete_topic(request={"topic": topic_path})
            return {"status": "success", "message": f"Topic deleted: {topic_path}"}
        except Exception as e:
            return {"status": "error", "message": f"Failed to delete topic: {str(e)}"}

    def list_pubsub_topics(self) -> dict[str, Any] | dict[str, str]:
        project_path = f"projects/{self.project_id}"

        try:
            topics = list(self.publisher.list_topics(request={"project": project_path}))
            return {"status": "success", "topics": [topic.name for topic in topics]}
        except Exception as e:
            return {"status": "error", "message": f"Failed to list topics: {str(e)}"}

    def update_pubsub_topic(
        self,
        topic_name: str,
        labels: Optional[dict] = None,
        message_retention_duration: Optional[str] = None,
        add_subscription: Optional[dict] = None,
    ) -> dict[str, Any]:
        if not self.publisher or not self.subscriber:
            return {"status": "error", "message": "PubSub service not initialized"}

        topic_path = self.publisher.topic_path(self.project_id, topic_name)

        try:
            # Crear un nuevo objeto Topic para la actualización
            update_topic = pubsub_v1.types.Topic()
            update_topic.name = topic_path

            # Crear una máscara de campo para los campos que se actualizarán
            update_mask = field_mask_pb2.FieldMask()

            if labels is not None:
                update_topic.labels.update(labels)
                update_mask.paths.append("labels")

            if message_retention_duration is not None:
                duration = self._parse_duration(message_retention_duration)
                if duration:
                    update_topic.message_retention_duration = duration
                    update_mask.paths.append("message_retention_duration")
                else:
                    return {"status": "error", "message": "Invalid message_retention_duration format"}

            # Actualizar el tópico solo si hay cambios
            if update_mask.paths:
                updated_topic = self.publisher.update_topic(
                    request={
                        "topic": update_topic,
                        "update_mask": update_mask,
                    }
                )
                result = {
                    "status": "success",
                    "message": f"Topic updated: {updated_topic.name}",
                    "topic_path": updated_topic.name,
                }
            else:
                result = {
                    "status": "success",
                    "message": "No updates were necessary",
                    "topic_path": topic_path,
                }

            # Añadir suscripción si se solicita
            if add_subscription:
                subscription_result = self._add_subscription(topic_path, add_subscription)
                if subscription_result["status"] == "success":
                    result["subscription"] = subscription_result["subscription"]
                else:
                    result["subscription_error"] = subscription_result["message"]

            return result

        except Exception as e:
            return {"status": "error", "message": f"Failed to update topic: {str(e)}"}

    def _parse_duration(self, duration_str: str) -> Optional[duration_pb2.Duration]:
        """Parse a duration string into a Duration object."""
        match = re.match(r'^(\d+)(s|m|h)$', duration_str)
        if not match:
            return None
        
        value, unit = match.groups()
        seconds = int(value) * {'s': 1, 'm': 60, 'h': 3600}[unit]
        return duration_pb2.Duration(seconds=seconds)
    def _add_subscription(
        self, topic_path: str, subscription_config: dict
    ) -> dict[str, Any]:
        """Add a subscription to a topic."""
        try:
            subscription_name = subscription_config.get("name")
            push_endpoint = subscription_config.get("push_endpoint")

            if not subscription_name:
                return {"status": "error", "message": "Subscription name is required"}

            subscription_path = self.subscriber.subscription_path(
                self.project_id, subscription_name
            )

            push_config = (
                pubsub_v1.types.PushConfig(push_endpoint=push_endpoint) if push_endpoint else None
            )

            subscription = self.subscriber.create_subscription(
                request={
                    "name": subscription_path,
                    "topic": topic_path,
                    "push_config": push_config,
                }
            )

            return {
                "status": "success",
                "subscription": {
                    "name": subscription.name,
                    "topic": subscription.topic,
                },
            }
        except Exception as e:
            return {
                "status": "error",
                "message": f"Failed to add subscription: {str(e)}",
            }
