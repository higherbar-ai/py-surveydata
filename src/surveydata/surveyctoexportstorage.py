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

"""Read-only support for SurveyCTO survey data exports."""

from surveydata.storagesystem import StorageSystem
import csv
import os
from typing import BinaryIO


class SurveyCTOExportStorage(StorageSystem):
    """Implementation of storage interface for read-only access to SurveyCTO survey data exports."""

    # define constants
    ID_FIELD = "KEY"                            # unique submission ID field
    ATTACHMENTS_SUBDIR = "media"                # name of attachments subdirectory, if present

    def __init__(self, export_file: str, attachments_available: bool):
        """
        Initialize SurveyCTO export data.

        :param export_file: Path to the export file
        :type export_file: str
        :param attachments_available: True if attachments available from SurveyCTO Desktop (in media subfolder)
        :type attachments_available: bool
        """

        # save export file and attachment availability
        self.export_file = export_file
        self.attachments_available = attachments_available

        # load export file into memory
        file = open(export_file, 'rt', encoding="utf-8")
        reader = csv.DictReader(file)
        self.submissions = list(reader)

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
        raise NotImplementedError

    def get_metadata(self, metadata_id: str) -> str:
        """
        Get metadata string from storage.

        :param metadata_id: Unique metadata ID (should begin and end with __ and not conflict with any submission ID)
        :type metadata_id: str
        :return: Metadata string from storage, or empty string if no such metadata exists
        :rtype: str
        """
        raise NotImplementedError

    def list_submissions(self) -> list:
        """
        List all submissions currently in storage.

        :return: List of submission IDs
        :rtype: list
        """

        # spin through all submissions, to assemble our list of submissions
        submission_keys = []
        for submission in self.submissions:
            submission_keys += [submission[self.ID_FIELD]]

        # return all submissions found
        return submission_keys

    def query_submission(self, submission_id: str) -> bool:
        """
        Query whether specific submission exists in storage.

        :param submission_id: Unique submission ID
        :type submission_id: str
        :return: True if submission exists in storage; otherwise False
        :rtype: bool
        """

        # spin through all submissions to find the requested submission
        for submission in self.submissions:
            if submission[self.ID_FIELD] == submission_id:
                return True

        # if we didn't find it, it's not there
        return False

    def store_submission(self, submission_id: str, submission_data: dict):
        """
        Store submission data in storage.

        :param submission_id: Unique submission ID
        :type submission_id: str
        :param submission_data: Submission data to store
        :type submission_data: dict
        """
        raise NotImplementedError

    def get_submission(self, submission_id: str) -> dict:
        """
        Get submission data from storage.

        :param submission_id: Unique submission ID
        :type submission_id: str
        :return: Submission data (or empty dictionary if submission not found)
        :rtype: dict
        """

        # spin through all submissions to find the requested submission
        for submission in self.submissions:
            if submission[self.ID_FIELD] == submission_id:
                return submission

        # since we didn't find the requested submission, return an empty dictionary
        return {}

    def get_submissions(self) -> list:
        """
        Get all submission data from storage.

        :return: List of dictionaries, one for each submission
        :rtype: list
        """

        # return all submissions loaded at init time
        return self.submissions

    def attachments_supported(self) -> bool:
        """
        Query whether storage system supports attachments.

        :return: True if attachments supported, otherwise False
        :rtype: bool
        """
        return self.attachments_available

    def list_attachments(self, submission_id: str = "") -> list:
        """
        List all attachments currently in storage.

        :param submission_id: Optional submission ID, to list only attachments for specific submission
        :type submission_id: str
        :return: List of attachments, each as dict with name, submission_id, and location_string
        :rtype: list
        """
        raise NotImplementedError

    def query_attachment(self, attachment_location: str = "", submission_id: str = "",
                         attachment_name: str = "") -> bool:
        """
        Query whether specific submission attachment exists in storage.

        :param attachment_location: Attachment location string (as exported by SurveyCTO Desktop)
        :type attachment_location: str
        :param submission_id: Unique submission ID (in lieu of attachment_location)
        :type submission_id: str
        :param attachment_name: Attachment filename (in lieu of attachment_location)
        :type attachment_name: str
        :return: True if submission exists in storage; otherwise False
        :rtype: bool

        Must pass either attachment_location or both submission_id and attachment_name.
        """

        # parse and/or construct appropriate attachment path
        attpath = self._attachment_path_from_params(attachment_location=attachment_location,
                                                    submission_id=submission_id, attachment_name=attachment_name)

        # return whether attachment is present
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
        raise NotImplementedError

    def get_attachment(self, attachment_location: str = "", submission_id: str = "",
                       attachment_name: str = "") -> BinaryIO:
        """
        Get submission attachment from storage.

        :param attachment_location: Attachment location string (as exported by SurveyCTO Desktop)
        :type attachment_location: str
        :param submission_id: Unique submission ID (in lieu of attachment_location)
        :type submission_id: str
        :param attachment_name: Attachment filename (in lieu of attachment_location)
        :type attachment_name: str
        :return: Attachment as file-like object
        :rtype: BinaryIO

        Must pass either attachment_location or both submission_id and attachment_name.
        """

        # parse and/or construct appropriate attachment path
        attpath = self._attachment_path_from_params(attachment_location=attachment_location,
                                                    submission_id=submission_id, attachment_name=attachment_name)

        # open file and return
        return open(attpath, mode="rb")

    def _attachment_path_from_params(self, attachment_location: str = "", submission_id: str = "",
                                     attachment_name: str = "") -> str:
        """
        Get attachment path from parameters, throwing exceptions as appropriate.

        :param attachment_location: Attachment location string (as exported by SurveyCTO Desktop)
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

            # construct attachment path from export file path and attachment location
            return os.path.join(os.path.join(os.path.split(self.export_file)[0],
                                             SurveyCTOExportStorage.ATTACHMENTS_SUBDIR), attachment_name)
        else:
            # construct attachment path from export file path and attachment location
            return os.path.join(os.path.split(self.export_file)[0], attachment_location)
