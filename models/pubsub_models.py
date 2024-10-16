# models/pubsub_models.py
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any


class PubSubCredentials(BaseModel):
    project_id: str
    credentials: Dict[str, Any]


class TopicCreate(BaseModel):
    project_id: str
    topic_name: str
    labels: Optional[Dict[str, str]] = Field(default_factory=dict)


class TopicDelete(BaseModel):
    topic_name: str
    delete_subscriptions: Optional[List[str]] = None


class SubscriptionDelete(BaseModel):
    subscription_name: str


class SubscriptionConfig(BaseModel):
    name: str
    push_endpoint: Optional[str] = None
    ack_deadline_seconds: Optional[int] = None
    retain_acked_messages: Optional[bool] = None
    message_retention_duration: Optional[str] = None
    labels: Optional[Dict[str, str]] = None


class TopicUpdate(BaseModel):
    topic_name: str
    labels: Optional[Dict[str, str]] = None
    message_retention_duration: Optional[str] = None
    add_subscription: Optional[SubscriptionConfig] = None
    update_subscription: Optional[SubscriptionConfig] = None

class Subscription(BaseModel):
    name: str
    full_name: str
    push_config: Optional[Dict[str, str]]
    ack_deadline_seconds: int
    message_retention_duration: str
    labels: Dict[str, str]


class Topic(BaseModel):
    name: str
    full_name: str
    labels: Dict[str, str]
    message_storage_policy: Dict[str, List[str]]
    kms_key_name: Optional[str]
    schema_settings: Optional[Dict[str, str]]
    satisfies_pzs: bool
    message_retention_duration: str
    subscriptions: List[Subscription]

class PubSubBaseResponse(BaseModel):
    status: str
    message: str

class TopicCreateResponse(PubSubBaseResponse):
    topic_path: str

class SubscriptionDeleteResult(BaseModel):
    name: str
    status: str
    message: str

class TopicDeleteResponse(PubSubBaseResponse):
    deleted_subscriptions: Optional[List[SubscriptionDeleteResult]] = None

class TopicListResponse(PubSubBaseResponse):
    topics: List[Topic]

class TopicGetResponse(PubSubBaseResponse):
    topic: Topic

class TopicUpdateResponse(PubSubBaseResponse):
    topic_path: str
    added_subscription: Optional[Dict[str, Any]] = None
    updated_subscription: Optional[Dict[str, Any]] = None
    add_subscription_error: Optional[str] = None
    update_subscription_error: Optional[str] = None

class PubSubResponse(BaseModel):
    status: str
    message: str
    topic_path: Optional[str] = None
    subscription: Optional[Dict[str, Any]] = None
    topics: Optional[List[Topic]] = None
    topic: Optional[Topic] = None
    deleted_subscriptions: Optional[List[Dict[str, str]]] = None
    added_subscription: Optional[Dict[str, Any]] = None
    updated_subscription: Optional[Dict[str, Any]] = None
    add_subscription_error: Optional[str] = None
    update_subscription_error: Optional[str] = None


class SubscriptionUpdate(BaseModel):
    name: str
    ack_deadline_seconds: Optional[int] = None
    retain_acked_messages: Optional[bool] = None
    message_retention_duration: Optional[str] = None
    labels: Optional[Dict[str, str]] = None
    push_endpoint: Optional[str] = None


class TopicDetail(BaseModel):
    name: str
    labels: Dict[str, str]
    message_storage_policy: Dict[str, List[str]]
    kms_key_name: Optional[str]
    schema_settings: Optional[Dict[str, str]]
    satisfies_pzs: bool
    message_retention_duration: int
    subscriptions: List[Subscription]


class TopicList(BaseModel):
    name: str
    labels: Dict[str, str]
    subscriptions: List[Subscription]


class PubSubListResponse(BaseModel):
    status: str
    topics: Optional[List[TopicList]] = None
    message: Optional[str] = None


class PubSubTopicResponse(BaseModel):
    status: str
    topic: Optional[TopicDetail] = None
    message: Optional[str] = None
