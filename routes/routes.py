from flask import Flask
from controllers import hello_controller

def register_routes(app: Flask):
    app.add_url_rule('/', 'hello', hello_controller.hello_world, methods=['GET'])