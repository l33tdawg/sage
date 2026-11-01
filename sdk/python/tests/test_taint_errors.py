import httpx
import pytest

from sage_sdk.exceptions import SageAPIError, SageAuthError, SageTaintError


def test_tainted_memory_maps_to_typed_sdk_error():
    response = httpx.Response(
        451,
        json={
            "type": "https://sage.dev/errors/memory-tainted",
            "title": "Memory withheld by governance taint",
            "status": 451,
            "detail": "An active governance decision withholds this memory.",
        },
    )
    error = SageAPIError.from_response(response)
    assert isinstance(error, SageTaintError)
    assert error.status_code == 451
    assert error.taint_state == "tainted"


def test_pending_taint_maps_to_distinct_sdk_state():
    response = httpx.Response(
        451,
        json={
            "type": "https://sage.dev/errors/memory-taint-pending",
            "title": "Memory taint coverage is pending",
            "status": 451,
            "detail": "Governance taint coverage is still being determined.",
        },
    )
    error = SageAPIError.from_response(response)
    assert isinstance(error, SageTaintError)
    assert error.taint_state == "taint_pending"


@pytest.mark.parametrize("problem_type", [{}, [], None])
@pytest.mark.parametrize(
    "status_code,error_class", [(451, SageAPIError), (403, SageAuthError)]
)
def test_non_string_problem_type_preserves_status_mapping(
    problem_type, status_code, error_class
):
    response = httpx.Response(
        status_code,
        json={"type": problem_type, "detail": "request denied"},
    )

    error = SageAPIError.from_response(response)

    assert type(error) is error_class
    assert error.status_code == status_code
    assert error.detail == "request denied"
