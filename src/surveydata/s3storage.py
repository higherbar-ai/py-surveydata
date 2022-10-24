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

"""Support for AWS S3 survey data storage."""

from surveydata.storagesystem import StorageSystem
import boto3
import botocore.exceptions
from urllib.parse import quote_plus, unquote_plus
import json
from typing import BinaryIO


class S3Storage(StorageSystem):
    """AWS S3 survey data storage implementation."""

    # define constants
    SUBMISSION_KEY_SUFFIX = ".json"                     # suffix for submission keys
    ATTACHMENT_LOCATION_PREFIX = "s3:"                  # prefix for S3 attachment location strings

    def __init__(self, bucket_name: str, key_name_prefix: str, aws_access_key_id: str = None,
                 aws_secret_access_key: str = None, aws_session_token: str = None):
        """
        Initialize S3 storage for survey data.

        :param bucket_name: Globally-unique S3 bucket name (must already exist)
        :type bucket_name: str
        :param key_name_prefix: Prefix to use for all key names (e.g., "Surveys/Form123/")
        :type key_name_prefix: str
        :param aws_access_key_id: AWS access key ID; if None, will use local config file and/or environment vars
        :type aws_access_key_id: str
        :param aws_secret_access_key: AWS access key secret; if None, will use local config file and/or environment vars
        :type aws_secret_access_key: str
        :param aws_session_token: AWS session token to use, only if using temporary credentials
        :type aws_session_token: str
        """

        # start an AWS session, and use passed credentials (if specified)
        self.aws_session = boto3.Session(aws_access_key_id=aws_access_key_id,
                                         aws_secret_access_key=aws_secret_access_key,
                                         aws_session_token=aws_session_token)

        # open an S3 client and bucket
        self.s3 = self.aws_session.client('s3')
        self.s3bucket = self.aws_session.resource('s3').Bucket(bucket_name)

        # save our bucket name and key name prefix
        self.bucket_name = bucket_name
        self.key_name_prefix = key_name_prefix

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

        # check to confirm metadata ID seems valid
        if not metadata_id.startswith("__") or not metadata_id.endswith("__"):
            raise ValueError(f"Metadata IDs must begin and end with __. {metadata_id} doesn't qualify.")

        # store metadata
        self.s3.put_object(Bucket=self.bucket_name, Key=self.key_name_prefix + quote_plus(metadata_id, safe=""),
                           Body=metadata)

    def get_metadata(self, metadata_id: str) -> str:
        """
        Get metadata string from storage.

        :param metadata_id: Unique metadata ID (should begin and end with __ and not conflict with any submission ID)
        :type metadata_id: str
        :return: Metadata string from storage, or empty string if no such metadata exists
        :rtype: str
        """

        # try to fetch the metadata, returning an empty string if it's not found
        try:
            s3object = self.s3.get_object(Bucket=self.bucket_name,
                                          Key=self.key_name_prefix + quote_plus(metadata_id, safe=""))
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "NoSuchKey":
                return ""
            else:
                raise

        # return the metadata as a regular UTF-8 string
        return s3object.get('Body').read().decode('utf-8')

    def list_submissions(self) -> list:
        """
        List all submissions currently in storage.

        :return: List of submission IDs
        :rtype: list
        """

        # in order not to be fooled by JSON submission attachments, count our number of expected /'s
        slashes_expected = self.key_name_prefix.count("/")

        # spin through all .json files in the appropriate folder, to assemble our list of submissions
        submissions = []
        for obj in self.s3bucket.objects.filter(Prefix=self.key_name_prefix):
            # see if it's a .json file in the root folder
            if obj.key.endswith(self.SUBMISSION_KEY_SUFFIX) and obj.key.count("/") == slashes_expected:
                # if so, strip, decode, and add to list
                submissions += [self.submission_id(obj.key)]

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

        # issue HEAD request, which will fail if object doesn't exist
        try:
            self.s3.head_object(Bucket=self.bucket_name, Key=self.submission_object_name(submission_id))
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "NoSuchKey":
                return False
            else:
                raise

        # if there was no exception, that means the object exists
        return True

    def store_submission(self, submission_id: str, submission_data: dict):
        """
        Store submission data in storage.

        :param submission_id: Unique submission ID
        :type submission_id: str
        :param submission_data: Submission data to store
        :type submission_data: dict
        """

        # store submission data as JSON file
        self.s3.put_object(Bucket=self.bucket_name, Key=self.submission_object_name(submission_id),
                           Body=json.dumps(submission_data))

    def get_submission(self, submission_id: str) -> dict:
        """
        Get submission data from storage.

        :param submission_id: Unique submission ID
        :type submission_id: str
        :return: Submission data (or empty dictionary if submission not found)
        :rtype: dict
        """

        # try to fetch the submission, returning an empty dictionary if it's not found
        try:
            s3object = self.s3.get_object(Bucket=self.bucket_name, Key=self.submission_object_name(submission_id))
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "NoSuchKey":
                return {}
            else:
                raise

        # return data from JSON, parsed as dict
        return json.loads(s3object.get('Body').read().decode('utf-8'))

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
        slashes_expected = self.key_name_prefix.count("/")+1

        # determine the appropriate filter prefix
        if submission_id:
            prefix = self.attachment_object_name(submission_id, "")
        else:
            prefix = self.key_name_prefix

        # spin through all objects under the appropriate folder, to assemble our list of attachments
        attachments = []
        for obj in self.s3bucket.objects.filter(Prefix=prefix):
            # see if it's a file at the correct folder level
            if obj.key.count("/") == slashes_expected:
                (subid, attname) = self.submission_id_and_attachment_name(obj.key)
                attachments += [{"name": attname, "submission_id": subid,
                                 "location_string": self.ATTACHMENT_LOCATION_PREFIX + obj.key}]

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

        # issue HEAD request, which will fail if object doesn't exist
        try:
            self.s3.head_object(Bucket=self.bucket_name, Key=attkey)
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "NoSuchKey":
                return False
            else:
                raise

        # if there was no exception, that means the attachment exists
        return True

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

        key = self.attachment_object_name(submission_id, attachment_name)
        self.s3.upload_fileobj(attachment_data, self.bucket_name, key)
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

        # try to fetch the attachment, allowing exception if it's not found
        s3object = self.s3.get_object(Bucket=self.bucket_name, Key=attkey)

        # return the attachment as a binary stream
        return s3object.get('Body')

    def submission_object_name(self, submission_id: str) -> str:
        """
        Get submission object name for specific submission.

        :param submission_id: Unique submission ID
        :type submission_id: str
        :return: Object name for submission
        :rtype: str
        """

        # combine prefix with submission ID (URL-encoded, including /'s) to create .json path+file
        return self.key_name_prefix + quote_plus(submission_id, safe="") + self.SUBMISSION_KEY_SUFFIX

    def submission_id(self, object_name: str) -> str:
        """
        Get submission ID from object name.

        :param object_name: Object name (e.g., from submission_object_name())
        :type object_name: str
        :return: Submission ID
        :rtype: str
        """

        # reverse everything submission_object_name() does to a submission ID
        return unquote_plus(object_name[len(self.key_name_prefix):-len(self.SUBMISSION_KEY_SUFFIX)])

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
        return self.key_name_prefix + quote_plus(submission_id, safe="")\
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
        stripped_and_split = object_name[len(self.key_name_prefix):].split("/")
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
                raise ValueError(f"S3 attachment locations must start with {self.ATTACHMENT_LOCATION_PREFIX} prefix.")

            # extract object key from location string
            return attachment_location[len(self.ATTACHMENT_LOCATION_PREFIX):]
