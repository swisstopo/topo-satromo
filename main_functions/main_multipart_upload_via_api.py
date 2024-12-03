#!/usr/bin/env python3
"""
Script to upload large assets to STAC via the REST API

This script facilitates the upload of large asset files to a STAC (SpatioTemporal Asset Catalog) API
using multipart uploads. It supports various environments (localhost, dev, int, prod) and provides
options for customizing the upload process.

Key features:
- Multipart file upload to STAC API
- MD5 checksum generation for file parts
- Support for different environments
- Configurable part size
- Ability to force upload and abort existing uploads
- Verbose logging option

Usage:
    python script_name.py <env> <collection> <item> <asset> <filepath> [options]

    env: Environment to be used (choices: localhost, dev, int, prod)
    collection: Name of the asset's collection
    item: Name of the asset's item
    asset: Name of the asset to be uploaded
    filepath: Local path of the asset file to be uploaded
    [options]
        --part-size: Size of the file parts in MB (default: 250 MB)
        --username: Username for authentication
        --password: Password for authentication
        -v, --verbose: Increase output verbosity
        --force: Cancel any existing upload before starting a new one

Dependencies:
    requests, multihash

Author: Unknown (based on geoadmin/bgdi-scripts, orginal  https://github.com/geoadmin/bgdi-scripts/blob/master/system-utilities/sys-data/py-scripts/multipart_upload_via_api.py)

chnages to the orginal:
 - added def multipart_upload
 - added in def _create_multipart_upload(self) : "update_interval": 30

"""

import argparse
import hashlib
import json
import os
import sys
from base64 import b64encode
from datetime import datetime
from hashlib import md5

import multihash
import requests
# retry and timeout support
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry  # pylint: disable=import-error

# For large parts, it might help to increase the DEFAULT_TIMEOUT
DEFAULT_TIMEOUT = 60  # seconds
MAX_PARTS_NUMBER = 100
DEFAULT_PART_SIZE = 250  # MB


class TimeoutHTTPAdapter(HTTPAdapter):
    '''Session-wide timeout for requests'''

    def __init__(self, *args, **kwargs):
        self.timeout = DEFAULT_TIMEOUT
        if "timeout" in kwargs:
            self.timeout = kwargs["timeout"]
            del kwargs["timeout"]
        super().__init__(*args, **kwargs)

    def send(self, request, **kwargs):  # pylint: disable=arguments-differ
        timeout = kwargs.get("timeout")
        if timeout is None:
            kwargs["timeout"] = self.timeout
        return super().send(request, **kwargs)


class HttpError(requests.exceptions.HTTPError):

    def __init__(self, response, message=None):
        msg = f'Error {response.status_code} for url "{response.url}"'
        try:
            msg += f' and response "{json.dumps(response.json()["description"])}"'
        except (requests.exceptions.JSONDecodeError, KeyError):
            pass  # Ignore if response if not json or if "description" does not exist
        if message:
            msg += f': {message}'
        super().__init__(msg, response=response)


retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
adapter = TimeoutHTTPAdapter(max_retries=retries)

http = requests.Session()
http.mount("http://", adapter)
http.mount("https://", adapter)


def b64_md5(data):
    '''Return the Base64 encoded MD5 digest of the given data

    Args:
        data: string
            input data

    Returns:
        string - base64 encoded MD5 digest of data
    '''
    return b64encode(md5(data).digest()).decode('utf-8')


def get_args():
    '''Returns parsed CLI arguments'''
    parser = argparse.ArgumentParser(
        description="Utility script for uploading large asset files as a multipart upload " \
            "using the STAC API."
    )
    parser.add_argument(
        "env",
        choices=['localhost', 'dev', 'int', 'prod'],
        help="Environment to be used: localhost, DEV, INT or PROD",
        type=str.lower
    )
    parser.add_argument("collection", help="Name of the asset's collection")
    parser.add_argument("item", help="Name of the asset's item")
    parser.add_argument("asset", help="Name of the asset to be uploaded")
    parser.add_argument("filepath", help="Local path of the asset file to be uploaded")
    parser.add_argument(
        "--part-size",
        type=int,
        default=DEFAULT_PART_SIZE,
        dest="part_size_in_mb",
        help=
        f"Size of the file parts in MB [Integer, default: {DEFAULT_PART_SIZE} MB] " \
        f"(Number of parts must be smaller than {MAX_PARTS_NUMBER})"
    )
    parser.add_argument("--username", help="If username is provided as argument, " \
        "the potentially defined STAC_USER environment variable will be IGNORED",
        default=os.environ.get('STAC_USER'))
    parser.add_argument("--password", help="If password is provided as argument, " \
        "the potentially defined STAC_PASSWORD environment variable will be IGNORED",
        default=os.environ.get('STAC_PASSWORD'))
    parser.add_argument("-v", "--verbose", help="Increase output verbosity", action="store_true")
    parser.add_argument(
        '--force',
        help="If an upload is already in progress, cancel it before starting this one.",
        action="store_true"
    )

    args = parser.parse_args()

    return args


class StacMultipartUploader:

    def __init__(self):
        '''Read the command line arguments and set the corresponding instance variables'''
        args = get_args()

        if not os.path.isfile(args.filepath):
            self._log(f"Error. The file {args.filepath} doesn't exists")
            sys.exit(1)

        scheme = "https"
        hostname = "data.geo.admin.ch"

        if args.env == "localhost":
            hostname = "127.0.0.1:8000"
            scheme = "http"
        elif args.env == "dev":
            hostname = "sys-data.dev.bgdi.ch"
        elif args.env == "int":
            hostname = "sys-data.int.bgdi.ch"

        asset_path = f'collections/{args.collection}/items/{args.item}/assets/{args.asset}'
        asset_file_size = os.path.getsize(args.filepath)

        self.asset_file_name = args.filepath
        self.verbose = args.verbose
        self.force = args.force
        self.part_size = min(args.part_size_in_mb * 1024**2, asset_file_size)
        self.uploads_url = f"{scheme}://{hostname}/api/stac/v0.9/{asset_path}/uploads"
        self.credentials = (args.username, args.password)

        if asset_file_size / self.part_size > MAX_PARTS_NUMBER:
            self._log(f"Error: parts number must be smaller than {MAX_PARTS_NUMBER}. "\
                  "Increase `--part-size`",
                  verbose=self.verbose
            )
            sys.exit(1)

        # Generate hashes
        self.checksum_multihash, self.md5_parts = self._generate_hashes()

    def _log(self, message, verbose=False, request=None, response=None):
        '''Log messages with optional timestamp, request, and response details'''
        if verbose:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"[{timestamp}] {message}")

            # Log request details if available
            if request:
                print(f"Request: {request.method} {request.url}")
                if request.body:
                    # Avoid logging file attachments or large binary data
                    if isinstance(request.body, bytes):
                        print("Request Body: [binary data omitted]")
                    else:
                        try:
                            formatted_body = json.dumps(json.loads(request.body), indent=4)
                            print(f"Request Body (formatted JSON):\n{formatted_body}")
                        except (json.JSONDecodeError, TypeError):
                            print(f"Request Body (raw):\n{request.body}")

            # Log response details if available
            if response:
                print(f"Response Status: {response.status_code}")
                if response.text:
                    try:
                        formatted_response = json.dumps(response.json(), indent=4)
                        print(f"Response Body (formatted JSON):\n{formatted_response}")
                    except (json.JSONDecodeError, TypeError):
                        print(f"Response Body (raw):\n{response.text}")
        else:
            print(message)

        print()

    def _generate_hashes(self):
        '''Returns the hashes for the file parts to upload. Called by the constructor.'''
        sha256 = hashlib.sha256()
        md5_parts = []
        if self.verbose:
            self._log(
                f"Reading {self.asset_file_name} and calculating parts and md5sum:",
                verbose=self.verbose
            )
        with open(self.asset_file_name, 'rb') as file_descriptor:
            while True:
                data = file_descriptor.read(self.part_size)
                if data in (b'', ''):
                    break
                sha256.update(data)
                md5_parts.append({'part_number': len(md5_parts) + 1, 'md5': b64_md5(data)})
                if self.verbose:
                    print(len(md5_parts), end="..", flush=True)

        print()
        sha2_256 = multihash.encode(sha256.digest(), 'sha2-256')
        checksum_multihash = multihash.to_hex_string(sha2_256)
        return (checksum_multihash, md5_parts)

    def _abort_upload(self, upload_id):
        response = http.post(f"{self.uploads_url}/{upload_id}/abort", auth=self.credentials)
        self._log(
            "Abort response received.",
            verbose=self.verbose,
            request=response.request,
            response=response
        )
        if response.status_code != 200 or response.json().get('status') != 'aborted':
            raise HttpError(response, "Aborting unsuccessful!")
        self._log("Aborted.", self.verbose)

    def upload_file(self):
        '''Upload the file to STAC'''
        if self.force:
            self._abort_previous_upload()
        upload_id, upload_urls = self._create_multipart_upload()
        try:
            uploaded_part_etags = self._upload_parts(upload_urls)
            self._complete_upload(upload_id, uploaded_part_etags)
        except (KeyboardInterrupt, requests.exceptions.RequestException) as ex:
            self._log(f"{type(ex).__name__} was raised. Gracefully abort...", verbose=self.verbose)
            self._abort_upload(upload_id)
            raise ex

    def _abort_previous_upload(self):
        self._log("Abort an upload that was already in progress...", verbose=self.verbose)
        response = http.get(self.uploads_url, params={"status": "in-progress"})
        if response.status_code != 200:
            raise HttpError(response)
        uploads = response.json()['uploads']
        if len(uploads) > 0:
            self._abort_upload(uploads[0]['upload_id'])
        else:
            self._log("No previous upload found", verbose=self.verbose)

    def _create_multipart_upload(self):
        '''Create a multipart upload'''
        self._log("First POST request for creating the multipart upload...", verbose=self.verbose)
        payload = {
            "number_parts": len(self.md5_parts),
            "md5_parts": self.md5_parts,
            "checksum:multihash": self.checksum_multihash,
            "update_interval": 30
        }
        response = http.post(self.uploads_url, auth=self.credentials, json=payload)
        self._log(
            f"Response status code was: {response.status_code}",
            verbose=self.verbose,
            request=response.request,
            response=response
        )
        if response.status_code == 409 and 'Upload already in progress' in response.json(
        )["description"]:
            raise HttpError(
                response,
                "Another upload is already in progress. To force upload, use \"--force\"."
            )
        if response.status_code != 201:
            raise HttpError(response)
        return (response.json()['upload_id'], response.json()['urls'])

    def _upload_parts(self, upload_urls):
        '''Upload the parts using the presigned urls'''
        self._log("Uploading the parts...", verbose=self.verbose)
        parts = []
        number_of_parts = len(upload_urls)

        with open(self.asset_file_name, 'rb') as file_descriptor:
            for url in upload_urls:
                self._log(
                    f"Uploading part {url['part']} of {number_of_parts}", verbose=self.verbose
                )
                data = file_descriptor.read(self.part_size)
                retry = 3
                while retry:
                    response = http.put(
                        url['url'],
                        data=data,
                        headers={'Content-MD5': self.md5_parts[url['part'] - 1]["md5"]}
                    )
                    self._log(
                        f"Part {url['part']} upload complete.",
                        verbose=self.verbose,
                        request=response.request,
                        response=response
                    )
                    if response.status_code == 200:
                        parts.append({'etag': response.headers['ETag'], 'part_number': url['part']})
                        retry = 0
                    else:
                        retry -= 1
                        if retry <= 0:
                            raise HttpError(response, f'Failed to upload part {url["part"]}')

        return parts

    def _complete_upload(self, upload_id, parts):
        '''Complete the upload'''
        self._log("Checking for multipart upload completness...", verbose=self.verbose)
        payload = {'parts': parts}
        response = http.post(
            f"{self.uploads_url}/{upload_id}/complete", auth=self.credentials, json=payload
        )
        self._log(
            "Completed multipart upload.",
            verbose=self.verbose,
            request=response.request,
            response=response
        )
        if response.status_code != 200 or response.json().get('status') != 'completed':
            raise HttpError(response)
        self._log("Completed!", verbose=self.verbose)


def main():
    '''Upload large file to STAC using multi-part uploads'''
    try:
        StacMultipartUploader().upload_file()
    except KeyboardInterrupt as interrupt:
        print('\nInterrupted!')
        raise SystemExit(interrupt) from interrupt


def multipart_upload(env, collection, item, asset, filepath, username, password, force=True,verbose=False):
    import os
    os.environ['STAC_USER'] = username
    os.environ['STAC_PASSWORD'] = password

    sys.argv = [
        'main_multipart_upload_via_api.py',
        env,
        collection,
        item,
        asset,
        filepath,
        '--username', username,
        '--password', password
    ]
    if verbose:
        sys.argv.append('--verbose')

    if force:
        sys.argv.append('--force')

    try:
        StacMultipartUploader().upload_file()
        return True
    except Exception as e:
        print(f"Upload failed: {str(e)}")
        return False

