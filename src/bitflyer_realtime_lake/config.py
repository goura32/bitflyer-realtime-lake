from __future__ import annotations

import os
import socket
from pathlib import Path
from typing import Any
from uuid import uuid4

import yaml
from dotenv import dotenv_values
from pydantic import BaseModel, ConfigDict, Field, field_validator


class BitflyerConfig(BaseModel):
    endpoint: str = "wss://ws.lightstream.bitflyer.com/json-rpc"
    product_codes: list[str] = Field(default_factory=lambda: ["BTC_JPY", "FX_BTC_JPY", "ETH_BTC"])
    channel_types: list[str] = Field(
        default_factory=lambda: ["executions", "ticker", "board_snapshot", "board"]
    )


class CollectorConfig(BaseModel):
    instance_id: str = "auto"
    reconnect_initial_seconds: int = 1
    reconnect_max_seconds: int = 30
    receive_timeout_seconds: int = 60


class SpoolConfig(BaseModel):
    path: str = "./spool/events.sqlite3"
    sqlite_synchronous: str = "NORMAL"

    @field_validator("sqlite_synchronous")
    @classmethod
    def validate_synchronous(cls, value: str) -> str:
        upper_value = value.upper()
        if upper_value not in {"OFF", "NORMAL", "FULL", "EXTRA"}:
            raise ValueError("sqlite_synchronous must be one of OFF, NORMAL, FULL, EXTRA")
        return upper_value


class ClickHouseConfig(BaseModel):
    host: str = "server.local"
    port: int = 8123
    database: str = "clickhouse"
    username: str = "clickhouse"
    password: str = ""
    batch_size: int = 1000
    flush_interval_seconds: float = 1.0
    retry_interval_seconds: float = 5.0


class AppConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    bitflyer: BitflyerConfig = Field(default_factory=BitflyerConfig)
    collector: CollectorConfig = Field(default_factory=CollectorConfig)
    spool: SpoolConfig = Field(default_factory=SpoolConfig)
    clickhouse: ClickHouseConfig = Field(default_factory=ClickHouseConfig)

    def resolved_instance_id(self) -> str:
        if self.collector.instance_id != "auto":
            return self.collector.instance_id
        return f"{socket.gethostname()}-{os.getpid()}-{uuid4().hex[:8]}"


def _load_yaml(path: Path | None) -> dict[str, Any]:
    if path is None:
        return {}
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    with path.open("r", encoding="utf-8") as fp:
        data = yaml.safe_load(fp) or {}
    if not isinstance(data, dict):
        raise ValueError("Config root must be a mapping")
    return data


def _load_env(path: Path | None) -> dict[str, str]:
    if path is not None and path.exists():
        return {k: v for k, v in dotenv_values(path).items() if v is not None}
    default_env = Path(".env")
    if default_env.exists():
        return {k: v for k, v in dotenv_values(default_env).items() if v is not None}
    return {}


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def _apply_env_overrides(data: dict[str, Any], env: dict[str, str]) -> dict[str, Any]:
    clickhouse_overrides: dict[str, Any] = {}
    mapping = {
        "CLICKHOUSE_HOST": ("host", str),
        "CLICKHOUSE_PORT": ("port", int),
        "CLICKHOUSE_DATABASE": ("database", str),
        "CLICKHOUSE_USERNAME": ("username", str),
        "CLICKHOUSE_PASSWORD": ("password", str),
    }
    for env_key, (field_name, caster) in mapping.items():
        if env_key in env:
            clickhouse_overrides[field_name] = caster(env[env_key])

    if clickhouse_overrides:
        current_clickhouse = data.get("clickhouse", {})
        data["clickhouse"] = _deep_merge(current_clickhouse, clickhouse_overrides)
    return data


def load_config(config_path: Path | None = None, env_path: Path | None = None) -> AppConfig:
    yaml_data = _load_yaml(config_path)
    env_data = _load_env(env_path)
    merged = _apply_env_overrides(yaml_data, env_data)
    return AppConfig.model_validate(merged)


def resolve_workdir_path(workdir: Path, path_value: str) -> Path:
    path = Path(path_value)
    if path.is_absolute():
        return path
    return (workdir / path).resolve()
