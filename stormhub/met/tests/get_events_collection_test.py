"""Tests for get_events_collection catalog lookup."""

import unittest
from types import SimpleNamespace

from stormhub.met.storm_catalog import get_events_collection


def _catalog(collection_ids, catalog_id="mycat"):
    """Minimal stand-in exposing only what get_events_collection touches."""
    collections = [SimpleNamespace(id=cid) for cid in collection_ids]
    return SimpleNamespace(id=catalog_id, get_all_collections=lambda: collections)


class GetEventsCollectionTest(unittest.TestCase):
    def test_finds_events_collection_when_not_first(self):
        # A catalog carrying another child collection before the events one must
        # still resolve the events collection (previously raised on the first miss).
        catalog = _catalog(["normalized-precip", "72hr-events"])
        self.assertEqual(get_events_collection(catalog).id, "72hr-events")

    def test_finds_events_collection_when_only_one(self):
        catalog = _catalog(["72hr-events"])
        self.assertEqual(get_events_collection(catalog).id, "72hr-events")

    def test_raises_when_no_events_collection(self):
        catalog = _catalog(["normalized-precip", "some-other"])
        with self.assertRaises(ValueError):
            get_events_collection(catalog)

    def test_raises_when_catalog_has_no_collections(self):
        # Empty catalog previously fell through the loop and returned None,
        # deferring the failure to an opaque AttributeError in the caller.
        catalog = _catalog([])
        with self.assertRaises(ValueError):
            get_events_collection(catalog)


if __name__ == "__main__":
    unittest.main()
