import httpx

from sage_sdk.exceptions import SageTaintError


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
    error = SageTaintError.from_response(response)
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
    error = SageTaintError.from_response(response)
    assert isinstance(error, SageTaintError)
    assert error.taint_state == "taint_pending"
