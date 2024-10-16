# app/services/pubsub_service.py
import logging
import re
from typing import Any, Dict, List, Optional
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

    def list_pubsub_topics(self) -> Dict[str, Any]:
        if not self.publisher or not self.subscriber:
            return {"status": "error", "message": "PubSub service not initialized"}

        project_path = f"projects/{self.project_id}"

        try:
            topics_with_subscriptions = []
            topics = list(self.publisher.list_topics(request={"project": project_path}))

            for topic in topics:
                topic_dict = {
                    "name": topic.name,
                    "labels": dict(topic.labels),
                    "subscriptions": self._list_topic_subscriptions(topic.name),
                }
                topics_with_subscriptions.append(topic_dict)

            return {"status": "success", "topics": topics_with_subscriptions}
        except Exception as e:
            return {"status": "error", "message": f"Failed to list topics: {str(e)}"}

    def get_pubsub_topic(self, topic_name: str) -> Dict[str, Any]:
        if not self.publisher or not self.subscriber:
            return {"status": "error", "message": "PubSub service not initialized"}

        topic_path = self.publisher.topic_path(self.project_id, topic_name)

        try:
            topic = self.publisher.get_topic(request={"topic": topic_path})

            topic_dict = {
                "name": topic.name,
                "labels": dict(topic.labels),
                "message_storage_policy": {
                    "allowed_persistence_regions": list(
                        topic.message_storage_policy.allowed_persistence_regions
                    )
                },
                "kms_key_name": topic.kms_key_name,
                "schema_settings": (
                    {
                        "schema": topic.schema_settings.schema,
                        "encoding": topic.schema_settings.encoding,
                    }
                    if topic.schema_settings
                    else None
                ),
                "satisfies_pzs": topic.satisfies_pzs,
                "message_retention_duration": topic.message_retention_duration.seconds,
                "subscriptions": self._list_topic_subscriptions(topic.name),
            }

            return {"status": "success", "topic": topic_dict}
        except Exception as e:
            return {"status": "error", "message": f"Failed to get topic: {str(e)}"}

    def _list_topic_subscriptions(self, topic_name: str) -> List[Dict[str, Any]]:
        try:
            subscriptions = self.publisher.list_topic_subscriptions(
                request={"topic": topic_name}
            )
            subscription_details = []
            for subscription_path in subscriptions:
                sub = self.subscriber.get_subscription(
                    request={"subscription": subscription_path}
                )
                subscription_details.append(
                    {
                        "name": sub.name,
                        "push_config": (
                            {"push_endpoint": sub.push_config.push_endpoint}
                            if sub.push_config.push_endpoint
                            else None
                        ),
                        "ack_deadline_seconds": sub.ack_deadline_seconds,
                        "message_retention_duration": sub.message_retention_duration.seconds,
                        "labels": dict(sub.labels),
                    }
                )
            return subscription_details
        except Exception as e:
            logging.error(
                f"Failed to list subscriptions for topic {topic_name}: {str(e)}"
            )
            return []

    def update_pubsub_topic(
        self,
        topic_name: str,
        labels: Optional[dict] = None,
        message_retention_duration: Optional[str] = None,
        add_subscription: Optional[dict] = None,
        update_subscription: Optional[dict] = None,
    ) -> dict[str, Any]:
        if not self.publisher or not self.subscriber:
            return {"status": "error", "message": "PubSub service not initialized"}

        topic_path = self.publisher.topic_path(self.project_id, topic_name)

        try:
            update_topic = pubsub_v1.types.Topic()
            update_topic.name = topic_path
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

            if update_mask.paths:
                updated_topic = self.publisher.update_topic(
                    request={"topic": update_topic, "update_mask": update_mask,}
                )
                result = {
                    "status": "success",
                    "message": f"Topic updated: {updated_topic.name}",
                    "topic_path": updated_topic.name,
                }
            else:
                result = {
                    "status": "success",
                    "message": "No updates were necessary for the topic",
                    "topic_path": topic_path,
                }

            if add_subscription:
                subscription_result = self._add_subscription(topic_path, add_subscription)
                if subscription_result["status"] == "success":
                    result["added_subscription"] = subscription_result["subscription"]
                else:
                    result["add_subscription_error"] = subscription_result["message"]

            if update_subscription:
                update_result = self._update_subscription(topic_path, update_subscription)
                if update_result["status"] == "success":
                    result["updated_subscription"] = update_result["subscription"]
                else:
                    result["update_subscription_error"] = update_result["message"]

            return result

        except Exception as e:
            return {"status": "error", "message": f"Failed to update topic: {str(e)}"}

    def _update_subscription(self, topic_path: str, subscription_config: dict) -> dict[str, Any]:
        try:
            subscription_name = subscription_config.get("name")
            if not subscription_name:
                return {"status": "error", "message": "Subscription name is required"}

            subscription_path = self.subscriber.subscription_path(self.project_id, subscription_name)
            
            # Crear máscara de actualización y objeto de suscripción
            update_mask = field_mask_pb2.FieldMask()
            subscription = pubsub_v1.types.Subscription()
            subscription.name = subscription_path

            if "ack_deadline_seconds" in subscription_config:
                subscription.ack_deadline_seconds = subscription_config["ack_deadline_seconds"]
                update_mask.paths.append("ack_deadline_seconds")

            if "retain_acked_messages" in subscription_config:
                subscription.retain_acked_messages = subscription_config["retain_acked_messages"]
                update_mask.paths.append("retain_acked_messages")

            if "message_retention_duration" in subscription_config:
                duration = self._parse_duration(subscription_config["message_retention_duration"])
                if duration:
                    subscription.message_retention_duration = duration
                    update_mask.paths.append("message_retention_duration")

            if "labels" in subscription_config:
                subscription.labels.update(subscription_config["labels"])
                update_mask.paths.append("labels")

            if "push_endpoint" in subscription_config:
                subscription.push_config = pubsub_v1.types.PushConfig(
                    push_endpoint=subscription_config["push_endpoint"]
                )
                update_mask.paths.append("push_config")

            # Actualizar la suscripción
            updated_subscription = self.subscriber.update_subscription(
                request={"subscription": subscription, "update_mask": update_mask}
            )

            return {
                "status": "success",
                "subscription": {
                    "name": updated_subscription.name,
                    "topic": updated_subscription.topic,
                    "push_config": updated_subscription.push_config.push_endpoint if updated_subscription.push_config else None,
                    "ack_deadline_seconds": updated_subscription.ack_deadline_seconds,
                    "retain_acked_messages": updated_subscription.retain_acked_messages,
                    "message_retention_duration": updated_subscription.message_retention_duration.seconds,
                    "labels": dict(updated_subscription.labels),
                },
            }
        except Exception as e:
            return {"status": "error", "message": f"Failed to update subscription: {str(e)}"}

    def _parse_duration(self, duration_str: str) -> Optional[duration_pb2.Duration]:
        """Parse a duration string into a Duration object."""
        match = re.match(r"^(\d+)(s|m|h)$", duration_str)
        if not match:
            return None

        value, unit = match.groups()
        seconds = int(value) * {"s": 1, "m": 60, "h": 3600}[unit]
        return duration_pb2.Duration(seconds=seconds)

    def _add_subscription(self, topic_path: str, subscription_config: dict) -> dict[str, Any]:
        try:
            subscription_name = subscription_config.get("name")
            if not subscription_name:
                return {"status": "error", "message": "Subscription name is required"}

            subscription_path = self.subscriber.subscription_path(self.project_id, subscription_name)
            
            # Configuración de suscripción
            subscription_settings = {
                "name": subscription_path,
                "topic": topic_path,
                "ack_deadline_seconds": subscription_config.get("ack_deadline_seconds", 10),
                "retain_acked_messages": subscription_config.get("retain_acked_messages", False),
                "message_retention_duration": self._parse_duration(subscription_config.get("message_retention_duration", "7d")),
                "labels": subscription_config.get("labels", {}),
            }

            # Configuración de Push si se proporciona
            push_endpoint = subscription_config.get("push_endpoint")
            if push_endpoint:
                subscription_settings["push_config"] = pubsub_v1.types.PushConfig(push_endpoint=push_endpoint)

            # Crear la suscripción
            subscription = self.subscriber.create_subscription(request=subscription_settings)

            return {
                "status": "success",
                "subscription": {
                    "name": subscription.name,
                    "topic": subscription.topic,
                    "push_config": subscription.push_config.push_endpoint if subscription.push_config else None,
                    "ack_deadline_seconds": subscription.ack_deadline_seconds,
                    "retain_acked_messages": subscription.retain_acked_messages,
                    "message_retention_duration": subscription.message_retention_duration.seconds,
                    "labels": dict(subscription.labels),
                },
            }
        except Exception as e:
            return {"status": "error", "message": f"Failed to add subscription: {str(e)}"}