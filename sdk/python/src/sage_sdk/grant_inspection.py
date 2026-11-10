"""Strict per-target direct-grant observations; never effective-access claims."""
from typing import Literal
import re

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


def inspection_agent_id(value: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{64}", value):
        raise ValueError("a canonical agent ID is required")
    return value


class DirectGrantObservation(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    domain: str = Field(min_length=1, max_length=1048576)
    grantee_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    granter_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    access_level: int = Field(ge=1, le=3)
    expires_at: int = Field(ge=0, le=2**63 - 1)


class DirectGrantsSnapshot(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    schema_version: Literal["sage.direct-grants-snapshot.v1"] = Field(alias="schema")
    caller_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    target_agent_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    chain_domain: str = Field(pattern=r"^[0-9a-f]{64}$")
    height: int = Field(gt=0, le=2**63 - 1)
    app_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    root_principal_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    root_generation: int = Field(gt=0, le=2**64 - 1)
    evaluated_at: int = Field(gt=0, le=2**63 - 1)
    coverage: Literal["direct-grants-for-target"]
    complete: Literal[True]
    grants: list[DirectGrantObservation] = Field(max_length=1024)

    @field_validator("complete", mode="before")
    @classmethod
    def complete_is_boolean_true(cls, value):
        if value is not True:
            raise ValueError("complete must be the boolean true")
        return value

    @model_validator(mode="after")
    def exact_target_and_live_rows(self):
        domains = set()
        for row in self.grants:
            if (row.grantee_id != self.target_agent_id or row.domain in domains
                    or row.expires_at and row.expires_at <= self.evaluated_at):
                raise ValueError("inconsistent direct-grant snapshot")
            domains.add(row.domain)
        return self


def inspection_response(response, caller: str, target: str) -> DirectGrantsSnapshot:
    if len(response.content) > 1048576:
        raise ValueError("grant snapshot exceeds response budget")
    result = DirectGrantsSnapshot.model_validate(response.json())
    if result.caller_id != caller or result.target_agent_id != target:
        raise ValueError("grant snapshot belongs to another request")
    return result


def _inspection_length(response):
    declared = response.headers.get("content-length")
    if declared is not None:
        if not declared.isascii() or not declared.isdecimal() or int(declared) > 1048576:
            raise ValueError("grant snapshot exceeds response budget")


def bounded_inspection_get(client, identity, target):
    """Consume at most the bounded decompressed body and always close its stream."""
    import httpx
    path = f"/v1/access/grants-snapshot/{target}"
    headers = identity.sign_request("GET", path, None)
    with client.stream("GET", path, headers=headers) as response:
        _inspection_length(response)
        body = bytearray()
        for chunk in response.iter_bytes(chunk_size=65536):
            if len(body) + len(chunk) > 1048576:
                raise ValueError("grant snapshot exceeds response budget")
            body.extend(chunk)
        return httpx.Response(response.status_code, content=bytes(body),
                              headers={"content-type": response.headers.get("content-type", "application/json")},
                              request=response.request)


async def bounded_async_inspection_get(client, identity, target):
    import httpx
    path = f"/v1/access/grants-snapshot/{target}"
    headers = identity.sign_request("GET", path, None)
    async with client.stream("GET", path, headers=headers) as response:
        _inspection_length(response)
        body = bytearray()
        async for chunk in response.aiter_bytes(chunk_size=65536):
            if len(body) + len(chunk) > 1048576:
                raise ValueError("grant snapshot exceeds response budget")
            body.extend(chunk)
        return httpx.Response(response.status_code, content=bytes(body),
                              headers={"content-type": response.headers.get("content-type", "application/json")},
                              request=response.request)
