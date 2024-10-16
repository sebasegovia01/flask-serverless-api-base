# services/pubsub_service.py
import logging
import re
from typing import Any, List, Optional
from google.cloud import pubsub_v1
from google.api_core import retry, exceptions
from google.oauth2 import service_account
from google.protobuf import field_mask_pb2, duration_pb2
from models.pubsub_models import *


class PubSubCreatorService:
    def __init__(self, credentials: PubSubCredentials) -> None:
        logging.info("Initializing PubSubCreator service...")
        try:
            self.credentials = service_account.Credentials.from_service_account_info(
                credentials.credentials
            )
            self.publisher = pubsub_v1.PublisherClient(credentials=self.credentials)
            self.subscriber = pubsub_v1.SubscriberClient(credentials=self.credentials)
            self.project_id = credentials.project_id
            print("PubSubCreator service initialized successfully.")
        except Exception as e:
            error_message = f"Failed to initialize PubSubCreator service: {str(e)}"
            logging.error(error_message)
            self.publisher = None
            self.subscriber = None
            self.project_id = None

    @retry.Retry()
    def create_pubsub_topic(self, topic_create: TopicCreate) -> TopicCreateResponse:
        topic_path = self.publisher.topic_path(
            topic_create.project_id, topic_create.topic_name
        )

        try:
            topic = self.publisher.create_topic(
                request={
                    "name": topic_path,
                    "labels": topic_create.labels or {},
                }
            )
            return TopicCreateResponse(
                status="success",
                message=f"Topic created: {topic.name}",
                topic_path=topic.name,
            )
        except Exception as e:
            error_message = f"Failed to create topic: {str(e)}"
            logging.error(error_message)
            return TopicCreateResponse(
                status="error", message=error_message, topic_path=""
            )

    def delete_pubsub_topic(self, topic_delete: TopicDelete) -> TopicDeleteResponse:
        if not self.publisher or not self.subscriber:
            return TopicDeleteResponse(
                status="error", message="PubSub service not initialized"
            )

        topic_path = self.publisher.topic_path(self.project_id, topic_delete.topic_name)
        deleted_subscriptions = []

        # Primero, verificar si el tópico existe
        try:
            self.publisher.get_topic(request={"topic": topic_path})
        except exceptions.NotFound:
            return TopicDeleteResponse(
                status="error",
                message=f"Topic not found: {topic_path}",
                deleted_subscriptions=None,
            )

        if topic_delete.delete_subscriptions:
            for subscription_name in topic_delete.delete_subscriptions:
                try:
                    subscription_path = self.subscriber.subscription_path(
                        self.project_id, subscription_name
                    )
                    self.subscriber.delete_subscription(
                        request={"subscription": subscription_path}
                    )
                    deleted_subscriptions.append(
                        SubscriptionDeleteResult(
                            name=subscription_name,
                            status="success",
                            message=f"Subscription deleted: {subscription_name}",
                        )
                    )
                except exceptions.NotFound:
                    deleted_subscriptions.append(
                        SubscriptionDeleteResult(
                            name=subscription_name,
                            status="error",
                            message=f"Subscription not found: {subscription_name}",
                        )
                    )
                except Exception as e:
                    deleted_subscriptions.append(
                        SubscriptionDeleteResult(
                            name=subscription_name,
                            status="error",
                            message=f"Failed to delete subscription: {str(e)}",
                        )
                    )

        try:
            self.publisher.delete_topic(request={"topic": topic_path})
            return TopicDeleteResponse(
                status="success",
                message=f"Topic deleted: {topic_path}",
                deleted_subscriptions=(
                    deleted_subscriptions if deleted_subscriptions else None
                ),
            )
        except Exception as e:
            error_message = f"Failed to delete topic: {str(e)}"
            logging.error(error_message)
            return TopicDeleteResponse(
                status="error",
                message=error_message,
                deleted_subscriptions=(
                    deleted_subscriptions if deleted_subscriptions else None
                ),
            )

    def delete_pubsub_subscription(
        self, subscription_delete: SubscriptionDelete
    ) -> PubSubBaseResponse:
        if not self.subscriber:
            return PubSubBaseResponse(
                status="error", message="PubSub service not initialized"
            )

        subscription_path = self.subscriber.subscription_path(
            self.project_id, subscription_delete.subscription_name
        )

        try:
            self.subscriber.delete_subscription(
                request={"subscription": subscription_path}
            )
            return PubSubBaseResponse(
                status="success", message=f"Subscription deleted: {subscription_path}"
            )
        except exceptions.NotFound:
            return PubSubBaseResponse(
                status="error", message=f"Subscription not found: {subscription_path}"
            )
        except Exception as e:
            error_message = f"Failed to delete subscription: {str(e)}"
            logging.error(error_message)
            return PubSubBaseResponse(status="error", message=error_message)

    def list_pubsub_topics(self) -> PubSubListResponse:
        if not self.publisher or not self.subscriber:
            return PubSubListResponse(
                status="error", message="PubSub service not initialized"
            )

        project_path = f"projects/{self.project_id}"

        try:
            topics_with_subscriptions = []
            topics = list(self.publisher.list_topics(request={"project": project_path}))

            for topic in topics:
                topic_dict = TopicList(
                    name=topic.name,
                    labels=dict(topic.labels),
                    subscriptions=self._list_topic_subscriptions(topic.name),
                )
                topics_with_subscriptions.append(topic_dict)

            return PubSubListResponse(
                status="success", topics=topics_with_subscriptions
            )
        except Exception as e:
            error_message = f"Failed to list topics: {str(e)}"
            logging.error(error_message)
            return PubSubListResponse(status="error", message=error_message)

    def get_pubsub_topic(self, topic_name: str) -> PubSubTopicResponse:
        if not self.publisher or not self.subscriber:
            return PubSubTopicResponse(
                status="error", message="PubSub service not initialized"
            )

        topic_path = self.publisher.topic_path(self.project_id, topic_name)

        try:
            topic = self.publisher.get_topic(request={"topic": topic_path})

            topic_detail = TopicDetail(
                name=topic.name,
                labels=dict(topic.labels),
                message_storage_policy={
                    "allowed_persistence_regions": list(
                        topic.message_storage_policy.allowed_persistence_regions
                    )
                },
                kms_key_name=topic.kms_key_name,
                schema_settings=(
                    {
                        "schema": topic.schema_settings.schema,
                        "encoding": topic.schema_settings.encoding,
                    }
                    if topic.schema_settings
                    else None
                ),
                satisfies_pzs=topic.satisfies_pzs,
                message_retention_duration=topic.message_retention_duration.seconds,
                subscriptions=self._list_topic_subscriptions(topic.name),
            )

            return PubSubTopicResponse(status="success", topic=topic_detail)
        except Exception as e:
            error_message = f"Failed to get topic: {str(e)}"
            logging.error(error_message)
            return PubSubTopicResponse(status="error", message=error_message)

    def _list_topic_subscriptions(self, topic_name: str) -> List[Subscription]:
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
                    Subscription(
                        name=sub.name,
                        full_name=sub.name,
                        push_config=(
                            {"push_endpoint": sub.push_config.push_endpoint}
                            if sub.push_config.push_endpoint
                            else None
                        ),
                        ack_deadline_seconds=sub.ack_deadline_seconds,
                        message_retention_duration=str(
                            sub.message_retention_duration.seconds
                        ),
                        labels=dict(sub.labels),
                    )
                )
            return subscription_details
        except Exception as e:
            error_message = (
                f"Failed to list subscriptions for topic {topic_name}: {str(e)}"
            )
            logging.error(error_message)
            return []

    def update_pubsub_topic(self, topic_update: TopicUpdate) -> TopicUpdateResponse:
        if not self.publisher or not self.subscriber:
            return TopicUpdateResponse(
                status="error", message="PubSub service not initialized", topic_path=""
            )

        topic_path = self.publisher.topic_path(self.project_id, topic_update.topic_name)

        try:
            update_mask = field_mask_pb2.FieldMask()
            topic = pubsub_v1.types.Topic()
            topic.name = topic_path

            if topic_update.labels is not None:
                topic.labels.update(topic_update.labels)
                update_mask.paths.append("labels")

            if topic_update.message_retention_duration is not None:
                duration = self._parse_duration(topic_update.message_retention_duration)
                if duration:
                    topic.message_retention_duration = duration
                    update_mask.paths.append("message_retention_duration")
                else:
                    return TopicUpdateResponse(
                        status="error",
                        message="Invalid message_retention_duration format",
                        topic_path=topic_path,
                    )

            result = TopicUpdateResponse(
                status="success", message="", topic_path=topic_path
            )

            if update_mask.paths:
                updated_topic = self.publisher.update_topic(
                    request={"topic": topic, "update_mask": update_mask}
                )
                result.message = f"Topic updated: {updated_topic.name}"
            else:
                result.message = "No updates were necessary for the topic"

            if topic_update.add_subscription:
                subscription_result = self._add_subscription(
                    topic_path, topic_update.add_subscription
                )
                if subscription_result.status == "success":
                    result.added_subscription = subscription_result.dict()
                else:
                    result.add_subscription_error = subscription_result.message

            if topic_update.update_subscription:
                update_result = self._update_subscription(
                    topic_path, topic_update.update_subscription
                )
                if update_result.status == "success":
                    result.updated_subscription = update_result.dict()
                else:
                    result.update_subscription_error = update_result.message

            return result

        except Exception as e:
            error_message = f"Failed to update topic: {str(e)}"
            logging.error(error_message)
            return TopicUpdateResponse(
                status="error", message=error_message, topic_path=topic_path
            )

    def _update_subscription(
        self, topic_path: str, subscription_config: SubscriptionConfig
    ) -> PubSubResponse:
        try:
            subscription_path = self.subscriber.subscription_path(
                self.project_id, subscription_config.name
            )

            update_mask = field_mask_pb2.FieldMask()
            subscription = pubsub_v1.types.Subscription()
            subscription.name = subscription_path

            if subscription_config.ack_deadline_seconds is not None:
                subscription.ack_deadline_seconds = (
                    subscription_config.ack_deadline_seconds
                )
                update_mask.paths.append("ack_deadline_seconds")

            if subscription_config.retain_acked_messages is not None:
                subscription.retain_acked_messages = (
                    subscription_config.retain_acked_messages
                )
                update_mask.paths.append("retain_acked_messages")

            if subscription_config.message_retention_duration:
                duration = self._parse_duration(
                    subscription_config.message_retention_duration
                )
                if duration:
                    subscription.message_retention_duration = duration
                    update_mask.paths.append("message_retention_duration")

            if subscription_config.labels:
                subscription.labels.update(subscription_config.labels)
                update_mask.paths.append("labels")

            if subscription_config.push_endpoint:
                subscription.push_config = pubsub_v1.types.PushConfig(
                    push_endpoint=subscription_config.push_endpoint
                )
                update_mask.paths.append("push_config")

            updated_subscription = self.subscriber.update_subscription(
                request={"subscription": subscription, "update_mask": update_mask}
            )

            return PubSubResponse(
                status="success",
                message=f"Subscription updated: {updated_subscription.name}",
                subscription={
                    "name": updated_subscription.name,
                    "topic": updated_subscription.topic,
                    "push_config": (
                        updated_subscription.push_config.push_endpoint
                        if updated_subscription.push_config
                        else None
                    ),
                    "ack_deadline_seconds": updated_subscription.ack_deadline_seconds,
                    "retain_acked_messages": updated_subscription.retain_acked_messages,
                    "message_retention_duration": str(
                        updated_subscription.message_retention_duration.seconds
                    ),
                    "labels": dict(updated_subscription.labels),
                },
            )
        except Exception as e:
            return PubSubResponse(
                status="error", message=f"Failed to update subscription: {str(e)}"
            )

    def _parse_duration(self, duration_str: str) -> Optional[duration_pb2.Duration]:
        """Parse a duration string into a Duration object."""
        match = re.match(r"^(\d+)(s|m|h)$", duration_str)
        if not match:
            return None

        value, unit = match.groups()
        seconds = int(value) * {"s": 1, "m": 60, "h": 3600}[unit]
        return duration_pb2.Duration(seconds=seconds)

    def _add_subscription(
        self, topic_path: str, subscription_config: SubscriptionConfig
    ) -> PubSubResponse:
        try:
            subscription_path = self.subscriber.subscription_path(
                self.project_id, subscription_config.name
            )

            subscription_settings = {
                "name": subscription_path,
                "topic": topic_path,
            }

            if subscription_config.ack_deadline_seconds is not None:
                subscription_settings["ack_deadline_seconds"] = (
                    subscription_config.ack_deadline_seconds
                )
            if subscription_config.retain_acked_messages is not None:
                subscription_settings["retain_acked_messages"] = (
                    subscription_config.retain_acked_messages
                )
            if subscription_config.message_retention_duration:
                subscription_settings["message_retention_duration"] = (
                    self._parse_duration(subscription_config.message_retention_duration)
                )
            if subscription_config.labels:
                subscription_settings["labels"] = subscription_config.labels
            if subscription_config.push_endpoint:
                subscription_settings["push_config"] = pubsub_v1.types.PushConfig(
                    push_endpoint=subscription_config.push_endpoint
                )

            subscription = self.subscriber.create_subscription(
                request=subscription_settings
            )

            return PubSubResponse(
                status="success",
                message=f"Subscription added: {subscription.name}",
                subscription={
                    "name": subscription.name,
                    "topic": subscription.topic,
                    "push_config": (
                        subscription.push_config.push_endpoint
                        if subscription.push_config
                        else None
                    ),
                    "ack_deadline_seconds": subscription.ack_deadline_seconds,
                    "retain_acked_messages": subscription.retain_acked_messages,
                    "message_retention_duration": str(
                        subscription.message_retention_duration.seconds
                    ),
                    "labels": dict(subscription.labels),
                },
            )
        except Exception as e:
            return PubSubResponse(
                status="error", message=f"Failed to add subscription: {str(e)}"
            )
