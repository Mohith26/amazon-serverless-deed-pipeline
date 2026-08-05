"""Seeded SYNTHETIC county-deed record generator.

IMPORTANT: This project uses 100% synthetic data. No real county website is
scraped and no real property records are used. Records are generated
deterministically from a ``(county, date)`` seed, so any day can be reproduced
byte-for-byte — which is what makes the idempotency and zero-dropped
reconciliation audits meaningful.
"""

from __future__ import annotations

import hashlib
import random
from dataclasses import dataclass
from datetime import date

# Synthetic stand-ins for public TX county real-property indexes (2+ sources).
COUNTIES = ("HARRIS", "DALLAS")

DOC_TYPES = (
    "WARRANTY DEED",
    "DEED OF TRUST",
    "RELEASE OF LIEN",
    "FORECLOSURE NOTICE",
    "MECHANICS LIEN",
    "QUITCLAIM DEED",
    "SUBSTITUTE TRUSTEE DEED",
    "ASSIGNMENT OF DEED OF TRUST",
)

_FIRST = (
    "JAMES", "MARY", "JOHN", "PATRICIA", "ROBERT", "JENNIFER", "MICHAEL", "LINDA",
    "WILLIAM", "ELIZABETH", "DAVID", "BARBARA", "RICHARD", "SUSAN", "JOSEPH", "JESSICA",
)
_LAST = (
    "SMITH", "JOHNSON", "WILLIAMS", "BROWN", "JONES", "GARCIA", "MILLER", "DAVIS",
    "RODRIGUEZ", "MARTINEZ", "HERNANDEZ", "LOPEZ", "GONZALEZ", "WILSON", "ANDERSON", "THOMAS",
)
_ORG = ("LP", "LLC", "TRUST", "HOLDINGS INC", "CAPITAL PARTNERS", "PROPERTIES LLC")
_STREETS = ("MAIN", "OAK", "ELM", "CEDAR", "PINE", "MAPLE", "WASHINGTON", "LAKE", "HILL", "RIVER")


@dataclass(frozen=True)
class DeedRecord:
    county: str
    filing_date: str
    doc_id: str
    doc_type: str
    grantor: str
    grantee: str
    legal_desc: str


def _seed(county: str, day: str) -> int:
    return int(hashlib.sha256(f"{county}|{day}".encode()).hexdigest(), 16) % (2**32)


def _name(rng: random.Random) -> str:
    if rng.random() < 0.25:
        return f"{rng.choice(_LAST)} {rng.choice(_ORG)}"
    return f"{rng.choice(_FIRST)} {rng.choice(_LAST)}"


def generate_day(county: str, day, count: int) -> list[DeedRecord]:
    """Deterministically generate ``count`` deed records for ``county`` on ``day``.

    The same (county, day, count) always yields identical records, so replaying a
    day cannot silently change its contents.
    """
    ds = day.isoformat() if isinstance(day, date) else str(day)
    rng = random.Random(_seed(county, ds))
    records: list[DeedRecord] = []
    for i in range(count):
        doc_id = f"{county[:3]}-{ds.replace('-', '')}-{i:05d}"
        records.append(
            DeedRecord(
                county=county,
                filing_date=ds,
                doc_id=doc_id,
                doc_type=rng.choice(DOC_TYPES),
                grantor=_name(rng),
                grantee=_name(rng),
                legal_desc=(
                    f"LOT {rng.randint(1, 40)} BLK {rng.randint(1, 20)} "
                    f"{rng.randint(100, 9999)} {rng.choice(_STREETS)} ST"
                ),
            )
        )
    return records
