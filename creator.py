#!/usr/bin/env python3
import os
import sys
import tempfile
import uuid
from sh import kolibri, zip as zip_files
from envcontext import EnvironmentContext as EnvContext
from shutil import copyfile
import hashlib
import urllib.request
import configparser
import json
import oss2

DATA_ROOT = os.environ.get("KOLIBRI_ZIP_DATA_ROOT", "/tmp")

CACHE_KOLIBRI_HOME = os.environ.get(
    "CACHE_KOLIBRI_HOME", os.path.join(DATA_ROOT, "kolibrihomecache")
)
CACHE_CONTENT_PARENT_DIR = os.environ.get(
    "CACHE_CONTENT_PARENT_DIR", os.path.join(DATA_ROOT, "contentcache")
)
CACHE_CONTENT_DIR = os.path.join(CACHE_CONTENT_PARENT_DIR, "content")
TEMP_ZIP_CONTENTS_ROOT = os.environ.get(
    "TEMP_ZIP_CONTENTS_ROOT", os.path.join(DATA_ROOT, "zipcontents")
)
TEMP_KOLIBRI_HOME_ROOT = os.environ.get(
    "TEMP_KOLIBRI_HOME_ROOT", os.path.join(DATA_ROOT, "kolibrihomes")
)
ZIP_ROOT = os.environ.get("ZIP_ROOT", os.path.join(DATA_ROOT, "zips"))
TEMP_FILE_DOWNLOAD_DIR = os.environ.get(
    "TEMP_FILE_DOWNLOAD_DIR", os.path.join(DATA_ROOT, "tempfiles")
)


def ensure_dir(path):
    if not os.path.exists(path):
        os.makedirs(path)


ensure_dir(TEMP_ZIP_CONTENTS_ROOT)
ensure_dir(TEMP_KOLIBRI_HOME_ROOT)
ensure_dir(ZIP_ROOT)
ensure_dir(TEMP_FILE_DOWNLOAD_DIR)


class KolibriContentImporter(object):
    def __init__(self, channels, source="https://studio.learningequality.org/"):
        self.channels = channels
        self.source = source

        if self.source.startswith("http"):
            self.method = "network"
            self.extra_arguments = ["--baseurl", self.source]
        else:
            self.method = "disk"
            self.extra_arguments = [self.source]

    def import_all_channels(self):
        for channel_id in self.channels:
            self.import_specific_channel(channel_id)

    def import_specific_channel(self, channel_id):

        assert channel_id in self.channels

        print("Importing content from channel {}...".format(channel_id))

        include_node_ids = self.channels[channel_id].get("include_node_ids", None)
        exclude_node_ids = self.channels[channel_id].get("exclude_node_ids", None)

        try:

            # download the channel database
            kolibri(
                "manage",
                "importchannel",
                self.method,
                channel_id,
                *self.extra_arguments,
                _in="y\n"
            )

            # download the requested content for the channel
            args = ["manage", "importcontent"]
            if include_node_ids:
                args += ["--node_ids", ",".join(include_node_ids)]
            if exclude_node_ids:
                args += ["--exclude_node_ids", ",".join(exclude_node_ids)]
            args += [self.method, channel_id]
            kolibri(*(args + self.extra_arguments), _in="y\n")

        except Exception as e:
            error = e.stderr.decode()
            print("ERROR DOWNLOADING CHANNEL {}...".format(channel_id))
            print(error)


def upload_to_oss(path):
    config = configparser.ConfigParser()
    config.read(os.path.expanduser("~/.ossutilconfig"))
    OSS_ENDPOINT = config["Credentials"]["endpoint"]
    OSS_ACCESS_KEY_ID = config["Credentials"]["accessKeyID"]
    OSS_ACCESS_KEY_SECRET = config["Credentials"]["accessKeySecret"]
    OSS_BUCKET = "kolibri"

    oss_auth = oss2.Auth(OSS_ACCESS_KEY_ID, OSS_ACCESS_KEY_SECRET)
    bucket = oss2.Bucket(oss_auth, OSS_ENDPOINT, OSS_BUCKET)
    filename = os.path.split(path)[1]
    oss2.resumable_upload(bucket, filename, path)


def download_file(url):
    url_hash = hashlib.md5(url.encode()).hexdigest()
    target_path = os.path.join(TEMP_FILE_DOWNLOAD_DIR, url_hash)
    if not os.path.exists(target_path):
        print("Downloading file:", url)
        urllib.request.urlretrieve(url, target_path)
        print("Download complete!")
    else:
        print("File already exists, so no need to download:", url)
    return target_path


class KolibriZipCreator(object):
    def __init__(self, manifest_path):
        with open(manifest_path) as f:
            self.manifest = json.load(f)
        self.job_id = self.manifest["job_id"]

    def create(self):

        # Download all channel content (as needed) into content cache.
        with EnvContext(
            KOLIBRI_HOME=CACHE_KOLIBRI_HOME, KOLIBRI_CONTENT_DIR=CACHE_CONTENT_DIR
        ):
            print(
                "\nSTARTING: Download all channel content (as needed) into content cache."
            )
            KolibriContentImporter(self.manifest["channels"]).import_all_channels()
            print(
                "COMPLETED: Download all channel content (as needed) into content cache."
            )

        # Set up the paths for assembling content and building zip file.
        temp_kolibri_home = os.path.join(TEMP_KOLIBRI_HOME_ROOT, self.job_id)
        temp_zip_root_dir = os.path.join(TEMP_ZIP_CONTENTS_ROOT, self.job_id, "")
        temp_content_dir = os.path.join(temp_zip_root_dir, "KOLIBRI_DATA", "content")
        zip_file_path = (
            os.path.join(ZIP_ROOT, self.job_id) + "_to_unzip_onto_usb_key.zip"
        )

        # Import content across into temporary content directory.
        with EnvContext(
            KOLIBRI_HOME=temp_kolibri_home, KOLIBRI_CONTENT_DIR=temp_content_dir
        ):
            print("\nSTARTING: Import content across into temporary content directory.")
            KolibriContentImporter(
                self.manifest["channels"], source=CACHE_CONTENT_PARENT_DIR
            ).import_all_channels()
            print("COMPLETED: Import content across into temporary content directory.")

        # Download and copy standard common files into root of path.
        print("\nSTARTING: Download and copy standard common files into root of path.")
        for file in self.manifest["other_files"]:
            source_path = download_file(file["source"])
            dest_path = os.path.join(temp_zip_root_dir, file["destination"])

            # ensure the folder we're copying into exists
            ensure_dir(os.path.dirname(dest_path))

            copyfile(source_path, dest_path)
        print("COMPLETED: Download and copy standard common files into root of path.")

        # Bundle up the content in the temporary directory into a zip file.
        print(
            "\nSTARTING: Bundle up the content in the temporary directory into a zip file."
        )
        zip_files(zip_file_path, "-r", ".", _cwd=temp_zip_root_dir)
        print(
            "COMPLETED: Bundle up the content in the temporary directory into a zip file."
        )

        # Push zip file to OSS.
        print("\nSTARTING: Push zip file to OSS.")
        upload_to_oss(zip_file_path)
        print("COMPLETED: Push zip file to OSS.")

        print(
            "Should now be available at:",
            "https://kolibri.oss-cn-shenzhen.aliyuncs.com/"
            + os.path.split(zip_file_path)[1],
        )


if __name__ == "__main__":
    if len(sys.argv) > 1:
        KolibriZipCreator(manifest_path=sys.argv[1]).create()
