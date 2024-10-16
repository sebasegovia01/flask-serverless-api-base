# app/routes.py
from flask import Flask
from controllers import msg_broker_controller

def register_routes(app: Flask):
    #rutas para message broker creator
    app.add_url_rule('/pubsub/topics', 'create_pubsub_topic', 
                     msg_broker_controller.create_pubsub_topic, methods=['POST'])
    app.add_url_rule('/pubsub/topics', 'delete_pubsub_topic', 
                     msg_broker_controller.delete_pubsub_topic, methods=['DELETE'])
    app.add_url_rule('/pubsub/topics', 'list_pubsub_topics', 
                     msg_broker_controller.list_pubsub_topics, methods=['GET'])
    app.add_url_rule('/pubsub/topics/<topic_name>', 'get_pubsub_topic', 
                     msg_broker_controller.get_pubsub_topic, methods=['GET'])
    app.add_url_rule('/pubsub/topics', 'update_pubsub_topic', 
                     msg_broker_controller.update_pubsub_topic, methods=['PUT'])
    app.add_url_rule('/pubsub/subscriptions', 'delete_pubsub_subscription', 
                     msg_broker_controller.delete_pubsub_subscription, methods=['DELETE'])