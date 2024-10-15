from flask import Flask, jsonify, Request
import functions_framework
import serverless_wsgi

app = Flask(__name__)


# cloud function
@functions_framework.http
def hello_http(request: Request):
    return jsonify(message="Hello, World!")

# lambda call all flask routes defined
def handler(event, context):
    return serverless_wsgi.handle_request(app, event, context)

# generic & lambda
@app.route('/', methods=['GET'])
def hello_http_flask():
    return jsonify(message="Hello, World!")


if __name__ == '__main__':
    app.run(debug=True)