# handlers/error_response.py
from flask import jsonify

class ErrorResponse:
    @staticmethod
    def register_error_handlers(app):
        @app.errorhandler(404)
        def not_found(error):
            return jsonify({"status": "error", "message": "Not Found"}), 404

        @app.errorhandler(500)
        def server_error(error):
            return jsonify({"status": "error", "message": "Internal Server Error"}), 500

        # Puedes añadir más manejadores de errores aquí si lo necesitas