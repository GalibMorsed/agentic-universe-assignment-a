"""Workflow lead schemas and the disposition catalogue (read from assignment-a-data/schemas)."""
from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from .normalize import normalize_header


@dataclass(frozen=True)
class Field:
    name: str
    type: str
    required: bool
    pii: bool
    reference: bool
    aliases: tuple[str, ...]      # normalised, includes the field name itself
    values: tuple[str, ...]       # enum values, lower-case
    default: Optional[str]


@dataclass(frozen=True)
class Workflow:
    workflow_id: str
    fields: tuple[Field, ...]     # schema order matters for rejection reasons

    @property
    def phone_field(self) -> Field:
        return next(f for f in self.fields if f.type == "phone")

    @property
    def reference_field(self) -> Field:
        return next(f for f in self.fields if f.reference)


class Catalog:
    def __init__(self, workflows: dict[str, Workflow],
                 dispositions: dict[tuple[str, str], tuple[bool, bool]]):
        self.workflows = workflows
        self._disp = dispositions

    def disposition(self, workflow_id: str, code: Optional[str]) -> Optional[tuple[bool, bool]]:
        """(is_terminal, is_success) or None when the code is not in the catalogue."""
        return self._disp.get((workflow_id, code))

    def rejection_reasons(self) -> list[str]:
        reasons = {"invalid_phone"}
        for wf in self.workflows.values():
            reasons.update(f"missing_required:{f.name}" for f in wf.fields if f.required)
        return sorted(reasons)


def load_catalog(schema_dir: Path) -> Catalog:
    workflows: dict[str, Workflow] = {}
    for p in sorted(Path(schema_dir).glob("*.json")):
        raw = json.loads(p.read_text(encoding="utf-8"))
        fields = []
        for f in raw["fields"]:
            aliases = {normalize_header(f["name"])}
            aliases.update(normalize_header(a) for a in f.get("aliases", []))
            fields.append(Field(
                name=f["name"], type=f["type"], required=bool(f.get("required")),
                pii=bool(f.get("pii")), reference=bool(f.get("reference")),
                aliases=tuple(sorted(aliases)),
                values=tuple(v.lower() for v in f.get("values", [])),
                default=f.get("default")))
        wf = Workflow(raw["workflow_id"], tuple(fields))
        if sum(f.type == "phone" for f in wf.fields) != 1 or not any(f.reference for f in wf.fields):
            raise ValueError(f"{p.name}: need exactly one phone field and a reference field")
        workflows[wf.workflow_id] = wf
    disp: dict[tuple[str, str], tuple[bool, bool]] = {}
    with open(Path(schema_dir) / "dispositions.csv", newline="", encoding="utf-8") as fh:
        for r in csv.DictReader(fh):
            disp[(r["workflow_id"], r["code"])] = (
                r["is_terminal"].strip().lower() == "true",
                r["is_success"].strip().lower() == "true")
    return Catalog(workflows, disp)
