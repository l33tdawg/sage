"""SAGE SDK exceptions."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import httpx


class SageError(Exception):
    """Base exception for all SAGE SDK errors."""


class SageAPIError(SageError):
    """Error returned by the SAGE API."""

    def __init__(
        self,
        status_code: int,
        detail: str,
        error_type: str | None = None,
        reason_code: str | None = None,
        remedy: str | None = None,
        retryable: bool | None = None,
    ) -> None:
        self.status_code = status_code
        self.detail = detail
        self.error_type = error_type
        self.reason_code = reason_code
        self.remedy = remedy
        self.retryable = retryable
        super().__init__(f"HTTP {status_code}: {detail}")

    @classmethod
    def from_response(cls, response: httpx.Response) -> SageAPIError:
        """Parse an RFC 7807 Problem Details response into the appropriate error."""
        status_code = response.status_code
        try:
            body = response.json()
            detail = body.get("detail", response.text)
            error_type = body.get("type")
            reason_code = body.get("reason_code")
            remedy = body.get("remedy")
            retryable = body.get("retryable")
        except Exception:
            detail = response.text
            error_type = None
            reason_code = None
            remedy = None
            retryable = None

        if error_type in {
            "https://sage.dev/errors/memory-tainted",
            "https://sage.dev/errors/memory-taint-pending",
        }:
            return SageTaintError(
                status_code=status_code,
                detail=detail,
                error_type=error_type,
                reason_code=reason_code,
                remedy=remedy,
                retryable=retryable,
            )
        if status_code == 401 or status_code == 403:
            return SageAuthError(
                status_code=status_code,
                detail=detail,
                error_type=error_type,
                reason_code=reason_code,
                remedy=remedy,
                retryable=retryable,
            )
        if status_code == 404:
            return SageNotFoundError(
                status_code=status_code,
                detail=detail,
                error_type=error_type,
                reason_code=reason_code,
                remedy=remedy,
                retryable=retryable,
            )
        if status_code == 422:
            return SageValidationError(
                status_code=status_code,
                detail=detail,
                error_type=error_type,
                reason_code=reason_code,
                remedy=remedy,
                retryable=retryable,
            )
        return cls(
            status_code=status_code,
            detail=detail,
            error_type=error_type,
            reason_code=reason_code,
            remedy=remedy,
            retryable=retryable,
        )


class SageAuthError(SageAPIError):
    """Authentication, signing, or authorization error (HTTP 401/403)."""

    def __init__(
        self,
        detail: str,
        status_code: int = 403,
        error_type: str | None = None,
        reason_code: str | None = None,
        remedy: str | None = None,
        retryable: bool | None = None,
    ) -> None:
        # Preserve the historical ``SageAuthError("message")`` constructor
        # while exposing the same structured RFC 7807 fields as SageAPIError.
        super().__init__(
            status_code=status_code,
            detail=detail,
            error_type=error_type,
            reason_code=reason_code,
            remedy=remedy,
            retryable=retryable,
        )


class SageNotFoundError(SageAPIError):
    """Resource not found (404)."""


class SageValidationError(SageAPIError):
    """Validation error (422)."""


class SageTaintError(SageAPIError):
    """Memory withheld by app-v30 governance taint coverage."""

    @property
    def taint_state(self) -> str:
        if self.error_type == "https://sage.dev/errors/memory-taint-pending":
            return "taint_pending"
        return "tainted"


# --- ABCI error code surface notes ---
#
# The exception hierarchy is intentionally flat — the SDK doesn't map every
# ABCI error code to a dedicated class. Useful ones to know when handling
# :class:`SageAPIError` from the v8.0 domain reassign endpoints:
#
#   Code 13 — generic permission denied (HTTP 403 / SageAuthError).
#   Code 50 — "shared domain not ownable": surfaced as HTTP 403 when an agent
#             attempts to own a shared domain (``open_to_shared=true``).
#
# App-v23 memory-write denials use RFC 7807 ``reason_code``, ``remedy``, and
# ``retryable`` extensions. Callers must branch on those structured fields,
# never on the human-readable detail string.
