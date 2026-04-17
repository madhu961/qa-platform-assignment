import re
from datetime import datetime
from typing import List


CORP_SUFFIXES = {
    "inc", "inc.", "corp", "corp.", "corporation",
    "ltd", "ltd.", "limited", "llc", "co", "co."
}

ADDRESS_REPLACEMENTS = {
    "street": "st",
    "road": "rd",
    "avenue": "ave",
    "boulevard": "blvd",
    "drive": "dr"
}


def get_utc_ts() -> str:
    return datetime.utcnow().isoformat()


def clean_corporate_name(name: str) -> str:
    if not name:
        return ""
    text = name.lower().strip()
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    tokens = [t for t in text.split() if t not in CORP_SUFFIXES]
    return " ".join(tokens)


def normalize_address(address: str) -> str:
    if not address:
        return ""
    text = address.lower().strip()
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    tokens = []
    for token in text.split():
        tokens.append(ADDRESS_REPLACEMENTS.get(token, token))
    return " ".join(tokens)


def parse_list_field(raw: str) -> List[str]:
    if not raw:
        return []
    parts = [x.strip() for x in raw.split("|")]
    return [p for p in parts if p]
