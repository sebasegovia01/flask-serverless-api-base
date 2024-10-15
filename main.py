from flask import Flask, Request
import functions_framework
import serverless_wsgi
from handlers.error_response import ErrorResponse
from routes.routes import register_routes

app = Flask(__name__)

# Register error handlers
ErrorResponse.register_error_handlers(app)

# Register all routes
register_routes(app)


# cloud function
@functions_framework.http
def hello_http(request: Request):
    return app(request.environ, lambda x, y: None)


# lambda / handle al request to reoutes
def handler(event, context):
    return serverless_wsgi.handle_request(app, event, context)


if __name__ == "__main__":
    app.run(debug=True)
