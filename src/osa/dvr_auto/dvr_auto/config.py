from pathlib import Path

import yaml


def _merge(a: dict, b: dict) -> dict:
    out = dict(a)
    for k, v in b.items():
        out[k] = _merge(out[k], v) if isinstance(v, dict) and isinstance(out.get(k), dict) else v
    return out


class Config:
    def __init__(self, raw: dict, profile: str):
        self.raw = raw
        self.profile = profile

    @classmethod
    def load(cls, path, profile=None):
        data = yaml.safe_load(Path(path).read_text())
        profiles = data.pop("profiles", {})
        name = profile or data.get("profile")
        if name:
            if name not in profiles:
                raise SystemExit(f"Profile '{name}' not in config (have: {list(profiles)})")
            data = _merge(data, profiles[name])
        cfg = cls(data, name)
        for k in ("r0v_output_root", "pixmask_dir", "workdir"):
            if not data["paths"].get(k):
                raise SystemExit(f"paths.{k} is not set (profile '{name}')")
        return cfg

    def path(self, key: str) -> Path:
        return Path(self.raw["paths"][key])

    @property
    def dl1_version(self) -> str:
        return self.raw["paths"]["dl1_version"]

    def job_preamble(self) -> str:
        env = self.raw["env"]
        lines = ["#!/bin/bash",
                 f"source {env['conda_sh']}",
                 f"conda activate {env['lstchain_env']}",
                 *env.get("extra_lines", [])]
        return "\n".join(lines) + "\n"
