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

"""Support for local file system survey data storage."""

from surveydata.storagesystem import StorageSystem
from urllib.parse import quote_plus, unquote_plus
import json
import os
import shutil
from typing import BinaryIO


class FileStorage(StorageSystem):
    """Local file system survey data storage implementation."""

    # define constants
    SUBMISSION_FILE_SUFFIX = ".json"                    # file suffix for submission keys
    ATTACHMENT_LOCATION_PREFIX = "file:"                # prefix for attachment location strings

    def __init__(self, submission_path: str):
        """
        Initialize local file system storage for survey data.

        :param submission_path: Globally-unique S3 bucket name (must already exist)
        :type submission_path: str
        """

        # create submission directory if it doesn't exist already
        if not os.path.exists(submission_path):
            os.makedirs(submission_path)

        # save local submission path
        self.submission_path = submission_path

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
        with open(os.path.join(self.submission_path, quote_plus(metadata_id, safe="")), "wt", encoding="utf-8") \
                as metadata_file:
            metadata_file.write(metadata)

    def get_metadata(self, metadata_id: str) -> str:
        """
        Get metadata string from storage.

        :param metadata_id: Unique metadata ID (should begin and end with __ and not conflict with any submission ID)
        :type metadata_id: str
        :return: Metadata string from storage, or empty string if no such metadata exists
        :rtype: str
        """

        metadata_path = os.path.join(self.submission_path, quote_plus(metadata_id, safe=""))
        if os.path.isfile(metadata_path):
            with open(metadata_path, "rt", encoding="utf-8") as metadata_file:
                return metadata_file.read()

        # if we didn't find the metadata, return an empty string
        return ""

    def list_submissions(self) -> list:
        """
        List all submissions currently in storage.

        :return: List of submission IDs
        :rtype: list
        """

        # spin through all .json files in the appropriate folder, to assemble our list of submissions
        submissions = []
        for filename in os.listdir(self.submission_path):
            # if it ends in .json, we'll assume it's a submission
            if filename.endswith(self.SUBMISSION_FILE_SUFFIX):
                # strip, decode, and add to list
                submissions += [self.submission_id(filename)]

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

        file_path = os.path.join(self.submission_path, self.submission_file_name(submission_id))
        return os.path.isfile(file_path)

    def store_submission(self, submission_id: str, submission_data: dict):
        """
        Store submission data in storage.

        :param submission_id: Unique submission ID
        :type submission_id: str
        :param submission_data: Submission data to store
        :type submission_data: dict
        """

        file_path = os.path.join(self.submission_path, self.submission_file_name(submission_id))
        with open(file_path, "wt", encoding="utf-8") as submission_file:
            submission_file.write(json.dumps(submission_data))

    def get_submission(self, submission_id: str) -> dict:
        """
        Get submission data from storage.

        :param submission_id: Unique submission ID
        :type submission_id: str
        :return: Submission data (or empty dictionary if submission not found)
        :rtype: dict
        """

        file_path = os.path.join(self.submission_path, self.submission_file_name(submission_id))
        if os.path.isfile(file_path):
            # since we have the file, return parsed JSON
            with open(file_path, "rt", encoding="utf-8") as submission_file:
                return json.loads(submission_file.read())
        else:
            # since we don't have the submission, return an empty dictionary
            return {}

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

        # assemble list of attachments, either for one submission or for all
        attachments = []
        if submission_id:
            subpath = self.attachment_path(submission_id, "")
            for attachment in os.scandir(subpath):
                if attachment.is_file():
                    attachments += [{"name": unquote_plus(attachment.name),
                                     "submission_id": submission_id,
                                     "location_string": self.ATTACHMENT_LOCATION_PREFIX + attachment.path}]

        else:
            for path in os.scandir(self.submission_path):
                if path.is_dir():
                    for attachment in os.scandir(path):
                        if attachment.is_file():
                            attachments += [{"name": unquote_plus(attachment.name),
                                             "submission_id": unquote_plus(path.name),
                                             "location_string": self.ATTACHMENT_LOCATION_PREFIX + attachment.path}]

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
        attpath = self._attachment_path_from_params(attachment_location=attachment_location,
                                                    submission_id=submission_id, attachment_name=attachment_name)
        # return whether we have it
        return os.path.isfile(attpath)

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

        # create attachment directory if it doesn't exist already
        attdir = self.attachment_path(submission_id, "")
        if not os.path.exists(attdir):
            os.makedirs(attdir)

        # stream the attachment data directly into the appropriate attachment file
        attpath = self.attachment_path(submission_id, attachment_name)
        with open(attpath, "wb") as attachment:
            shutil.copyfileobj(attachment_data, attachment)
        return self.ATTACHMENT_LOCATION_PREFIX + attpath

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
        attpath = self._attachment_path_from_params(attachment_location=attachment_location,
                                                    submission_id=submission_id, attachment_name=attachment_name)

        # return a reference to the file, allowing exception if not found
        return open(attpath, "rb")

    def submission_file_name(self, submission_id: str) -> str:
        """
        Get submission filename for specific submission.

        :param submission_id: Unique submission ID
        :type submission_id: str
        :return: Filename for submission
        :rtype: str
        """

        # combine submission ID (URL-encoded, including /'s) with suffix to create .json filename
        return quote_plus(submission_id, safe="") + self.SUBMISSION_FILE_SUFFIX

    def submission_id(self, filename: str) -> str:
        """
        Get submission ID from filename.

        :param filename: Filename (e.g., from submission_file_name())
        :type filename: str
        :return: Submission ID
        :rtype: str
        """

        # reverse everything submission_file_name() does to a submission ID
        return unquote_plus(filename[:-len(self.SUBMISSION_FILE_SUFFIX)])

    def attachment_path(self, submission_id: str, attachment_name: str) -> str:
        """
        Get attachment path for specific attachment.

        :param submission_id: Unique submission ID
        :type submission_id: str
        :param attachment_name: Attachment filename
        :type attachment_name: str
        :return: Path for submission
        :rtype: str
        """

        # combine path prefix with submission ID and attachment name (both URL-encoded, including /'s) to create path
        return os.path.join(self.submission_path, quote_plus(submission_id, safe=""),
                            quote_plus(attachment_name, safe=""))

    def _attachment_path_from_params(self, attachment_location: str = "", submission_id: str = "",
                                     attachment_name: str = "") -> str:
        """
        Get attachment path from parameters, throwing exceptions as appropriate.

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

            # construct path from submission ID and attachment name
            return self.attachment_path(submission_id, attachment_name)
        else:
            # confirm attachment location looks legit; if not, raise exception
            if not attachment_location.startswith(self.ATTACHMENT_LOCATION_PREFIX):
                raise ValueError(f"File attachment locations must start with {self.ATTACHMENT_LOCATION_PREFIX} prefix.")

            # extract path from location string
            return attachment_location[len(self.ATTACHMENT_LOCATION_PREFIX):]
