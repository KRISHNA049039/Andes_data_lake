"""Base error hierarchy for the Andes Data Lake."""

import uuid
from datetime import datetime


class AndesError(Exception):
    """Base exception for all Andes Data Lake errors."""

    code: str = "INTERNAL_ERROR"

    def __init__(self, message: str, details: dict | None = None):
        super().__init__(message)
        self.message = message
        self.details = details or {}
        self.correlation_id = str(uuid.uuid4())
        self.timestamp = datetime.utcnow().isoformat()

    def to_response(self) -> dict:
        return {
            "error": {
                "code": self.code,
                "message": self.message,
                "correlationId": self.correlation_id,
                "timestamp": self.timestamp,
                "details": self.details,
            }
        }


class AuthorizationDeniedError(AndesError):
    """Raised when an RBAC authorization check denies access."""

    code = "AUTHORIZATION_DENIED"
