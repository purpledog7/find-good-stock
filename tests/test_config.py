import os
from pathlib import Path

from config import load_local_env


def test_load_local_env_reads_values_without_overriding_existing_env(tmp_path, monkeypatch):
    env_path = tmp_path / ".env"
    env_path.write_text(
        "\n".join(
                [
                    "# comment",
                    "LOCAL_ENV_TEST_ID=local-id",
                    "export LOCAL_ENV_TEST_PW=local-password",
                    'LOCAL_ENV_TEST_SECRET="quoted-secret"',
                    "LOCAL_ENV_EXISTING_KEY=from-file",
                ]
            ),
            encoding="utf-8",
        )
    monkeypatch.delenv("LOCAL_ENV_TEST_ID", raising=False)
    monkeypatch.delenv("LOCAL_ENV_TEST_PW", raising=False)
    monkeypatch.delenv("LOCAL_ENV_TEST_SECRET", raising=False)
    monkeypatch.setenv("LOCAL_ENV_EXISTING_KEY", "from-env")

    load_local_env(Path(env_path))

    assert os.getenv("LOCAL_ENV_TEST_ID") == "local-id"
    assert os.getenv("LOCAL_ENV_TEST_PW") == "local-password"
    assert os.getenv("LOCAL_ENV_TEST_SECRET") == "quoted-secret"
    assert os.getenv("LOCAL_ENV_EXISTING_KEY") == "from-env"
