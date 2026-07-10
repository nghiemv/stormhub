"""Tests for the AORC source selector."""

import os
import unittest
from unittest import mock

from stormhub.met.zarr_to_dss import aorc_storage_options

_MIRROR_ENV = {
    "AORC_S3_KEY": "AK",
    "AORC_S3_SECRET": "SK",
    "AORC_S3_ENDPOINT": "https://s3.hecdev.net",
}


class AorcStorageOptionsTest(unittest.TestCase):
    def test_anonymous_when_no_credentials(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            opts = aorc_storage_options()
        self.assertTrue(opts["anon"])
        self.assertNotIn("key", opts)

    def test_mirror_credentials_when_key_set(self):
        with mock.patch.dict(os.environ, _MIRROR_ENV, clear=True):
            opts = aorc_storage_options()
        self.assertFalse(opts["anon"])
        self.assertEqual(opts["key"], "AK")
        self.assertEqual(opts["secret"], "SK")
        self.assertEqual(opts["endpoint_url"], "https://s3.hecdev.net")
        self.assertNotIn("client_kwargs", opts)

    def test_region_is_optional(self):
        with mock.patch.dict(os.environ, {**_MIRROR_ENV, "AORC_S3_REGION": "us-east-1"}, clear=True):
            opts = aorc_storage_options()
        self.assertEqual(opts["client_kwargs"], {"region_name": "us-east-1"})

    def test_secret_required_with_key(self):
        with mock.patch.dict(os.environ, {"AORC_S3_KEY": "AK"}, clear=True):
            with self.assertRaises(KeyError):
                aorc_storage_options()


if __name__ == "__main__":
    unittest.main()
