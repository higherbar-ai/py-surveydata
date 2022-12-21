#  Copyright (c) 2022 Orange Chair Labs LLC
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

"""Support for Google Cloud Storage survey data storage."""

from surveydata import StorageSystem
from google.oauth2 import service_account
from google.cloud import storage
from urllib.parse import quote_plus, unquote_plus
import json
from typing import BinaryIO


class GoogleCloudStorage(StorageSystem):
    """Google Cloud Storage survey data storage implementation."""

    # define constants
    SUBMISSION_KEY_SUFFIX = ".json"                     # suffix for submission keys
    ATTACHMENT_LOCATION_PREFIX = "gs:"                  # prefix for attachment location strings
    ATTACHMENT_CHUNK_SIZE = 262144                      # chunk size for streaming attachments

    def __init__(self, project_id: str, bucket_name: str, blob_name_prefix: str,
                 credentials: service_account.Credentials = None):
        """
        Initialize Google Cloud Storage for survey data.

        :param project_id: Google Cloud Storage project ID
        :type project_id: str
        :param bucket_name: Globally-unique Cloud Storage bucket name (must already exist)
        :type bucket_name: str
        :param blob_name_prefix: Prefix to use for all blob names (e.g., "Surveys/Form123/")
        :type blob_name_prefix: str
        :param credentials: Explicit service account credentials to use (e.g., loaded from
            service_account.Credentials.from_service_account_file())
        :type credentials: credentials.Credentials
        """

        # start a client session
        if credentials is None:
            # assume environment has project and credential details
            self.client = storage.Client(project=project_id)
        else:
            self.client = storage.Client(project=project_id, credentials=credentials)

        # go ahead and create the bucket object
        self.bucket = self.client.bucket(bucket_name)

        # save our project ID, bucket name, and blob name prefix
        self.project_id = project_id
        self.bucket_name = bucket_name
        self.blob_name_prefix = blob_name_prefix

        # call base class constructor as well
        super().__init__()

    def store_metadata(self, metadata_id: str, metadata: str):
        """
        Store metadata string in storage.

        :param metadata_id: Unique metadata ID (should begin and end with __ and not conflict with any submission ID)
        :type metadata_id: str
        :param metadata: Metadata string to store
        :type metadata: str
        """

        # convert string to byte array and store
        self.store_metadata_binary(metadata_id, metadata.encode('utf-8'))

    def get_metadata(self, metadata_id: str) -> str:
        """
        Get metadata string from storage.

        :param metadata_id: Unique metadata ID (should begin and end with __ and not conflict with any submission ID)
        :type metadata_id: str
        :return: Metadata string from storage, or empty string if no such metadata exists
        :rtype: str
        """

        # fetch bytes, decode, and return
        return self.get_metadata_binary(metadata_id).decode('utf-8')

    def store_metadata_binary(self, metadata_id: str, metadata: bytes):
        """
        Store metadata bytes in storage.

        :param metadata_id: Unique metadata ID (should begin and end with __ and not conflict with any submission ID)
        :type metadata_id: str
        :param metadata: Metadata bytes to store
        :type metadata: bytes
        """

        # check to confirm metadata ID seems valid
        if not metadata_id.startswith("__") or not metadata_id.endswith("__"):
            raise ValueError(f"Metadata IDs must begin and end with __. {metadata_id} doesn't qualify.")

        # store metadata
        blob = self.bucket.blob(self.blob_name_prefix + quote_plus(metadata_id, safe=""))
        blob.upload_from_string(metadata)

    def get_metadata_binary(self, metadata_id: str) -> bytes:
        """
        Get metadata bytes from storage.

        :param metadata_id: Unique metadata ID (should not conflict with any submission ID)
        :type metadata_id: str
        :return: Metadata bytes from storage, or empty bytes array if no such metadata exists
        :rtype: bytes
        """

        # try to fetch the metadata, returning an empty bytes array if it's not found
        blob = self.bucket.blob(self.blob_name_prefix + quote_plus(metadata_id, safe=""))
        if blob is None or not blob.exists():
            return bytes()

        # return the metadata as bytes
        return blob.download_as_bytes(raw_download=True)

    def list_submissions(self) -> list:
        """
        List all submissions currently in storage.

        :return: List of submission IDs
        :rtype: list
        """

        # spin through all .json files in the appropriate folder, to assemble our list of submissions
        submissions = []
        for blob in self.client.list_blobs(self.bucket_name, prefix=self.blob_name_prefix, delimiter="/"):
            # see if it's a .json file
            if blob.name.endswith(self.SUBMISSION_KEY_SUFFIX):
                # if so, strip, decode, and add to list
                submissions += [self.submission_id(blob.name)]

        # return all submissions found
        return submissions

    def query_submission(self, submission_id: str) -> bool:
        """
        Query whether specific submission exists in storage.

        :param submission_id: Unique submission ID
        :type submission_id: str
        :return: True if submission exists in storage; otherwise False
        :rtype: bool
        """

        blob = self.bucket.blob(self.submission_object_name(submission_id))
        return blob is not None and blob.exists()

    def store_submission(self, submission_id: str, submission_data: dict):
        """
        Store submission data in storage.

        :param submission_id: Unique submission ID
        :type submission_id: str
        :param submission_data: Submission data to store
        :type submission_data: dict
        """

        # store submission data as JSON file
        blob = self.bucket.blob(self.submission_object_name(submission_id))
        blob.upload_from_string(json.dumps(submission_data))

    def get_submission(self, submission_id: str) -> dict:
        """
        Get submission data from storage.

        :param submission_id: Unique submission ID
        :type submission_id: str
        :return: Submission data (or empty dictionary if submission not found)
        :rtype: dict
        """

        # try to fetch the submission, returning an empty dictionary if it's not found
        blob = self.bucket.blob(self.submission_object_name(submission_id))
        if blob is None or not blob.exists():
            return {}

        # return data from JSON, parsed as dict
        return json.loads(blob.download_as_bytes(raw_download=True).decode('utf-8'))

    def attachments_supported(self) -> bool:
        """
        Query whether storage system supports attachments.

        :return: True if attachments supported, otherwise False
        :rtype: bool
        """

        return True

    def list_attachments(self, submission_id: str = "") -> list:
        """
        List all attachments currently in storage.

        :param submission_id: Optional submission ID, to list only attachments for specific submission
        :type submission_id: str
        :return: List of attachments, each as dict with name, submission_id, and location_string
        :rtype: list
        """

        # count expected number of slashes to make sure we get attachments at correct directory level
        slashes_expected = self.blob_name_prefix.count("/")+1

        # determine the appropriate filter prefix
        if submission_id:
            prefix = self.attachment_object_name(submission_id, "")
        else:
            prefix = self.blob_name_prefix

        # spin through all blobs under the appropriate folder, to assemble our list of attachments
        attachments = []
        for blob in self.client.list_blobs(self.bucket_name, prefix=prefix):
            # see if it's a file at the correct folder level
            if blob.name.count("/") == slashes_expected:
                (subid, attname) = self.submission_id_and_attachment_name(blob.name)
                attachments += [{"name": attname, "submission_id": subid,
                                 "location_string": self.ATTACHMENT_LOCATION_PREFIX + blob.name}]

        # return all attachments found
        return attachments

    def query_attachment(self, attachment_location: str = "", submission_id: str = "",
                         attachment_name: str = "") -> bool:
        """
        Query whether specific submission attachment exists in storage.

        :param attachment_location: Attachment location string (as returned when attachment stored)
        :type attachment_location: str
        :param submission_id: Unique submission ID (in lieu of attachment_location)
        :type submission_id: str
        :param attachment_name: Attachment filename (in lieu of attachment_location)
        :type attachment_name: str
        :return: True if submission exists in storage; otherwise False
        :rtype: bool

        Must pass either attachment_location or both submission_id and attachment_name.
        """

        # parse and/or construct appropriate attachment object name
        attkey = self._attachment_key_from_params(attachment_location=attachment_location, submission_id=submission_id,
                                                  attachment_name=attachment_name)

        blob = self.bucket.blob(attkey)
        return blob is not None and blob.exists()

    def store_attachment(self, submission_id: str, attachment_name: str, attachment_data: BinaryIO) -> str:
        """
        Store submission attachment in storage.

        :param submission_id: Unique submission ID
        :type submission_id: str
        :param attachment_name: Attachment filename
        :type attachment_name: str
        :param attachment_data: File-type object containing the attachment data
        :type attachment_data: BinaryIO
        :return: Location string for stored attachment
        :rtype: str
        """

        # upload attachment and return location string (with a 256k chunk size for streaming)
        key = self.attachment_object_name(submission_id, attachment_name)
        with self.bucket.blob(key, chunk_size=262144).open(mode='wb') as f:
            while True:
                batch = attachment_data.read(self.ATTACHMENT_CHUNK_SIZE)
                if not batch:
                    break
                f.write(batch)
        return self.ATTACHMENT_LOCATION_PREFIX + key

    def get_attachment(self, attachment_location: str = "", submission_id: str = "",
                       attachment_name: str = "") -> BinaryIO:
        """
        Get submission attachment from storage.

        :param attachment_location: Attachment location string (as returned when attachment stored)
        :type attachment_location: str
        :param submission_id: Unique submission ID (in lieu of attachment_location)
        :type submission_id: str
        :param attachment_name: Attachment filename (in lieu of attachment_location)
        :type attachment_name: str
        :return: Attachment as file-like object (though, note: it doesn't support seeking)
        :rtype: BinaryIO

        Must pass either attachment_location or both submission_id and attachment_name.
        """

        # parse and/or construct appropriate attachment object name
        attkey = self._attachment_key_from_params(attachment_location=attachment_location, submission_id=submission_id,
                                                  attachment_name=attachment_name)

        # try to fetch the attachment, raising exception if it's not found
        blob = self.bucket.blob(attkey, chunk_size=self.ATTACHMENT_CHUNK_SIZE)
        if blob is None or not blob.exists():
            raise ValueError(f"Attachment '{attkey}' not found in Google Cloud Storage bucket '{self.bucket_name}'.")

        # return the attachment as a binary stream
        return blob.open(mode="rb", raw_download=True)

    def submission_object_name(self, submission_id: str) -> str:
        """
        Get submission object name for specific submission.

        :param submission_id: Unique submission ID
        :type submission_id: str
        :return: Object name for submission
        :rtype: str
        """

        # combine prefix with submission ID (URL-encoded, including /'s) to create .json path+file
        return self.blob_name_prefix + quote_plus(submission_id, safe="") + self.SUBMISSION_KEY_SUFFIX

    def submission_id(self, object_name: str) -> str:
        """
        Get submission ID from object name.

        :param object_name: Object name (e.g., from submission_object_name())
        :type object_name: str
        :return: Submission ID
        :rtype: str
        """

        # reverse everything submission_object_name() does to a submission ID
        return unquote_plus(object_name[len(self.blob_name_prefix):-len(self.SUBMISSION_KEY_SUFFIX)])

    def attachment_object_name(self, submission_id: str, attachment_name: str) -> str:
        """
        Get attachment object name for specific attachment.

        :param submission_id: Unique submission ID
        :type submission_id: str
        :param attachment_name: Attachment filename
        :type attachment_name: str
        :return: Object name for submission
        :rtype: str
        """

        # combine prefix with submission ID and attachment name (both URL-encoded, including /'s) to create path+file
        return self.blob_name_prefix + quote_plus(submission_id, safe="")\
            + "/" + quote_plus(attachment_name, safe="")

    def submission_id_and_attachment_name(self, object_name: str) -> (str, str):
        """
        Get submission ID and attachment name from object name.

        :param object_name: Object name (e.g., from submission_object_name())
        :type object_name: str
        :return: Submission ID and attachment name
        :rtype: (str, str)
        """

        # reverse everything attachment_object_name() does to a submission ID and attachment name
        stripped_and_split = object_name[len(self.blob_name_prefix):].split("/")
        return (unquote_plus(stripped_and_split[0]),
                unquote_plus(stripped_and_split[1]))

    def _attachment_key_from_params(self, attachment_location: str = "", submission_id: str = "",
                                    attachment_name: str = "") -> str:
        """
        Get attachment object key from parameters, throwing exceptions as appropriate.

        :param attachment_location: Attachment location string (as returned when attachment stored)
        :type attachment_location: str
        :param submission_id: Unique submission ID (in lieu of attachment_location)
        :type submission_id: str
        :param attachment_name: Attachment filename (in lieu of attachment_location)
        :type attachment_name: str
        :return: Attachment object name
        :rtype: str

        Must pass either attachment_location or both submission_id and attachment_name.
        """

        if not attachment_location:
            # confirm we have a submission ID and attachment name, since we don't have an attachment location
            if not submission_id or not attachment_name:
                raise ValueError(f"Must pass either attachment_location or both submission_id and attachment_name.")

            # construct object key from submission ID and attachment name
            return self.attachment_object_name(submission_id, attachment_name)
        else:
            # confirm attachment location looks legit; if not, raise exception
            if not attachment_location.startswith(self.ATTACHMENT_LOCATION_PREFIX):
                raise ValueError(f"Google Cloud Storage attachment locations must start with "
                                 f"{self.ATTACHMENT_LOCATION_PREFIX} prefix.")

            # extract object key from location string
            return attachment_location[len(self.ATTACHMENT_LOCATION_PREFIX):]
