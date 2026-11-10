#!/usr/bin/env python3
"""Enforce the domain import boundary with a self-testing probe."""
import json
import subprocess
import sys

MODULE = "github.com/marxms/go-whale/"
RULES = {
    "internal/market": set(),
    "internal/trade": {"internal/market"},
    "internal/candle": {"internal/market", "internal/trade"},
    "internal/derivatives": {"internal/market"},
}


def violations(entries):
    found = []
    for pkg, imports in entries.items():
        rel = pkg.removeprefix(MODULE)
        allowed = RULES.get(rel)
        if allowed is None:
            continue
        for imp in imports:
            if not imp.startswith(MODULE):
                continue
            target = imp.removeprefix(MODULE)
            if target == rel:
                continue
            if target not in allowed:
                found.append((rel, target))
    return found


def self_test():
    checks = [
        ({MODULE + "internal/candle": {MODULE + "internal/adapters/outbound/redis"}}, True),
        ({MODULE + "internal/candle": {MODULE + "internal/trade"}}, False),
        ({MODULE + "internal/market": {MODULE + "internal/candle"}}, True),
        ({MODULE + "internal/derivatives": {"fmt"}}, False),
        ({MODULE + "internal/derivatives": {MODULE + "internal/market"}}, False),
        ({MODULE + "internal/derivatives": {MODULE + "internal/candle"}}, True),
    ]
    for entries, want_violation in checks:
        got = bool(violations(entries))
        if got != want_violation:
            print(f"boundary self-test failed: {entries} -> {got}", file=sys.stderr)
            return False
    return True


def load_entries():
    proc = subprocess.run(
        ["go", "list", "-json", "./internal/..."],
        check=True, capture_output=True, text=True,
    )
    entries = {}
    decoder = json.JSONDecoder()
    text = proc.stdout.strip()
    while text:
        obj, end = decoder.raw_decode(text)
        entries[obj["ImportPath"]] = set(obj.get("Imports") or [])
        text = text[end:].lstrip()
    return entries


def main():
    if not self_test():
        return 2
    found = violations(load_entries())
    for pkg, imp in found:
        print(f"{pkg}: forbidden import {imp}", file=sys.stderr)
    if found:
        return 1
    print(f"import boundaries: {len(RULES)} core packages checked, 0 violations")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
