"""Tests for downloading and verifying SAFE products against a fake S3."""

import hashlib
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from botocore.exceptions import ClientError

from safe_download import (STATUS_DOWNLOADED, STATUS_FAILED, STATUS_SKIPPED,
                           download_product, read_manifest_checksums)
from stac_search import Scene

PREFIX = "Sentinel-1/SAR/SLC/2021/05/29/S1A_TEST.SAFE/"
MEASUREMENT = b"measurement data"
ANNOTATION = b"<annotation/>"


def md5(data):
    return hashlib.md5(data).hexdigest()


def manifest_for(files):
    """Build a manifest.safe that lists an MD5 for each given file."""
    entries = "".join(
        f'<dataObject><byteStream size="{len(data)}">'
        f'<fileLocation locatorType="URL" href="./{path}"/>'
        f'<checksum checksumName="MD5">{md5(data)}</checksum>'
        f"</byteStream></dataObject>"
        for path, data in files.items()
    )
    return (f'<xfdu:XFDU xmlns:xfdu="urn:ccsds:schema:xfdu:1">'
            f"<dataObjectSection>{entries}</dataObjectSection></xfdu:XFDU>").encode()


class FakePaginator:
    def __init__(self, objects):
        self.objects = objects

    def paginate(self, Bucket, Prefix):
        yield {"Contents": [o for o in self.objects if o["Key"].startswith(Prefix)]}


class FakeS3:
    """
    Serves objects from memory. `corrupt_once` lists keys whose first
    download returns wrong bytes of the right size (simulating corruption).
    """

    def __init__(self, files, corrupt_once=()):
        self.files = {PREFIX + path: data for path, data in files.items()}
        self.corrupt_once = set(PREFIX + path for path in corrupt_once)
        self.downloaded_keys = []

    def get_paginator(self, name):
        objects = [{"Key": PREFIX, "Size": 0}]  # directory marker, must be ignored
        objects += [{"Key": key, "Size": len(data)} for key, data in self.files.items()]
        return FakePaginator(objects)

    def download_file(self, bucket, key, filename, Config=None):
        self.downloaded_keys.append(key)
        data = self.files[key]
        if key in self.corrupt_once:
            self.corrupt_once.remove(key)
            data = b"x" * len(data)
        Path(filename).write_bytes(data)


def remote_files():
    files = {"measurement/a.tiff": MEASUREMENT, "annotation/a.xml": ANNOTATION}
    files["manifest.safe"] = manifest_for(files)
    return files


SCENE = Scene("S1A_TEST.SAFE", "2021-05-29T16:50:25Z", "eodata", PREFIX)


class DownloadProductTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.target = Path(self.tmp.name) / "S1A_TEST.SAFE"
        # Do not really wait between retry rounds in tests.
        sleep_patcher = mock.patch("safe_download.time.sleep")
        self.sleep = sleep_patcher.start()
        self.addCleanup(sleep_patcher.stop)

    def tearDown(self):
        self.tmp.cleanup()

    def download(self, s3, max_retries=3):
        return download_product(s3, SCENE, self.target, max_threads=2, threads_per_file=1,
                                max_retries=max_retries)

    def test_fresh_download_is_complete_and_manifest_comes_first(self):
        s3 = FakeS3(remote_files())
        result = self.download(s3)

        self.assertEqual(result.status, STATUS_DOWNLOADED)
        self.assertEqual((self.target / "measurement/a.tiff").read_bytes(), MEASUREMENT)
        self.assertEqual(s3.downloaded_keys[0], PREFIX + "manifest.safe")

    def test_complete_product_is_skipped(self):
        self.download(FakeS3(remote_files()))
        s3 = FakeS3(remote_files())

        result = self.download(s3)

        self.assertEqual(result.status, STATUS_SKIPPED)
        self.assertEqual(s3.downloaded_keys, [])

    def test_only_missing_files_are_downloaded(self):
        self.download(FakeS3(remote_files()))
        (self.target / "measurement/a.tiff").unlink()
        s3 = FakeS3(remote_files())

        self.download(s3)

        self.assertEqual(s3.downloaded_keys, [PREFIX + "measurement/a.tiff"])

    def test_corrupted_file_is_detected_by_md5_and_downloaded_again(self):
        s3 = FakeS3(remote_files(), corrupt_once=["measurement/a.tiff"])

        result = self.download(s3)

        self.assertEqual(result.status, STATUS_DOWNLOADED)
        self.assertEqual((self.target / "measurement/a.tiff").read_bytes(), MEASUREMENT)
        self.assertEqual(s3.downloaded_keys.count(PREFIX + "measurement/a.tiff"), 2)

    def test_gives_up_after_max_retries(self):
        s3 = FakeS3(remote_files(), corrupt_once=["measurement/a.tiff"])

        result = self.download(s3, max_retries=1)

        self.assertEqual(result.status, STATUS_FAILED)
        self.assertFalse((self.target / "measurement/a.tiff").exists())

    def test_waits_before_retry_round(self):
        s3 = FakeS3(remote_files(), corrupt_once=["measurement/a.tiff"])
        self.download(s3)
        self.sleep.assert_called_once_with(60)

    def test_listing_error_fails_product_instead_of_raising(self):
        s3 = FakeS3(remote_files())
        throttled = ClientError({"Error": {"Code": "429", "Message": "Too Many Requests"}}, "ListObjectsV2")
        s3.get_paginator = mock.Mock(side_effect=throttled)

        result = self.download(s3)

        self.assertEqual(result.status, STATUS_FAILED)
        self.assertIn("429", result.message)

    def test_missing_product_on_s3_fails(self):
        s3 = FakeS3({})
        self.assertEqual(self.download(s3).status, STATUS_FAILED)


class ManifestTests(unittest.TestCase):
    def test_checksums_are_read_with_normalised_paths(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "manifest.safe"
            path.write_bytes(manifest_for({"measurement/a.tiff": MEASUREMENT}))
            self.assertEqual(read_manifest_checksums(path), {"measurement/a.tiff": md5(MEASUREMENT)})

    def test_missing_manifest_gives_no_checksums(self):
        self.assertEqual(read_manifest_checksums(Path("/does/not/exist")), {})


if __name__ == "__main__":
    unittest.main()
