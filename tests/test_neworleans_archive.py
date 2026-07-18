import json
import os
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from sources import neworleans_archive


class _FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


class _FakeSession:
    def __init__(self, rows):
        self.rows = rows

    def get(self, _url, *, params, timeout):
        del timeout
        if params.get("$select") == "count(*) as count":
            return _FakeResponse([{"count": str(len(self.rows))}])
        return _FakeResponse(list(self.rows))


class NewOrleansArchiveTests(unittest.TestCase):
    def test_raw_mirror_appends_changed_payload_version_without_losing_original(self):
        first_rows = [
            {
                "nopd_item": "A0000126",
                "typetext": "SIMPLE BURGLARY",
                "timecreate": "2026-01-01T00:01:00",
                "selfinitiated": "N",
                "block_address": "010XX Example St",
                "location": {"type": "Point", "coordinates": [-90.1, 29.9]},
            },
            {
                "nopd_item": "A0000226",
                "typetext": "AREA CHECK",
                "timecreate": "2026-01-01T00:02:00",
                "selfinitiated": "Y",
                "block_address": "020XX Example St",
                "location": {"type": "Point", "coordinates": [-90.2, 29.8]},
            },
        ]
        changed_rows = [dict(row) for row in first_rows]
        changed_rows[0] = {
            **changed_rows[0],
            "typetext": "AGGRAVATED BURGLARY",
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = os.path.join(temp_dir, "neworleans_calls_2026.db")
            with patch.object(
                neworleans_archive.neworleans,
                "_session",
                return_value=_FakeSession(first_rows),
            ):
                first = neworleans_archive.mirror_year(
                    2026,
                    db_path=db_path,
                    user_agent="test",
                    page_size=100,
                )

            with patch.object(
                neworleans_archive.neworleans,
                "_session",
                return_value=_FakeSession(changed_rows),
            ):
                second = neworleans_archive.mirror_year(
                    2026,
                    db_path=db_path,
                    user_agent="test",
                    page_size=100,
                )

            self.assertEqual("complete", first["status"])
            self.assertEqual(2, first["current_calls"])
            self.assertEqual(2, first["call_versions"])
            self.assertEqual(3, second["call_versions"])
            self.assertEqual(1, second["changed_calls"])

            conn = sqlite3.connect(db_path)
            payloads = [
                json.loads(row[0])
                for row in conn.execute(
                    "SELECT raw_json FROM call_versions WHERE nopd_item = ? ORDER BY id",
                    ("A0000126",),
                ).fetchall()
            ]
            current_type = conn.execute(
                """
                SELECT versions.type_text
                FROM current_calls current
                JOIN call_versions versions ON versions.id = current.version_id
                WHERE current.nopd_item = ?
                """,
                ("A0000126",),
            ).fetchone()[0]
            conn.close()

            self.assertEqual("SIMPLE BURGLARY", payloads[0]["typetext"])
            self.assertEqual("AGGRAVATED BURGLARY", payloads[1]["typetext"])
            self.assertEqual("AGGRAVATED BURGLARY", current_type)


if __name__ == "__main__":
    unittest.main()
