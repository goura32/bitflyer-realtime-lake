from pathlib import Path

from bitflyer_realtime_lake.config import load_config, resolve_workdir_path


def test_load_config_from_yaml_and_env(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        """
clickhouse:
  host: "yaml-host"
  password: ""
""".strip()
        + "\n",
        encoding="utf-8",
    )
    env_path = tmp_path / ".env"
    env_path.write_text(
        "\n".join(
            [
                "CLICKHOUSE_HOST=env-host",
                "CLICKHOUSE_PORT=9000",
                "CLICKHOUSE_PASSWORD=secret",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    config = load_config(config_path=config_path, env_path=env_path)

    assert config.clickhouse.host == "env-host"
    assert config.clickhouse.port == 9000
    assert config.clickhouse.password == "secret"


def test_resolve_workdir_path(tmp_path: Path) -> None:
    resolved = resolve_workdir_path(tmp_path, "./spool/events.sqlite3")
    assert resolved == (tmp_path / "spool" / "events.sqlite3").resolve()
