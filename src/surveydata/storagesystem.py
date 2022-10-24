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

"""Core interface (informal) for survey data storage systems."""

from typing import BinaryIO
import pandas as pd


class StorageSystem(object):
    """Largely-abstract base class for survey data storage systems."""

    def __init__(self):
        """Initialize storage system."""
        pass

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

        :param metadata_id: Unique metadata ID (should not conflict with any submission ID)
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
        raise NotImplementedError

    def query_submission(self, submission_id: str) -> bool:
        """
        Query whether specific submission exists in storage.

        :param submission_id: Unique submission ID
        :type submission_id: str
        :return: True if submission exists in storage; otherwise False
        :rtype: bool
        """
        raise NotImplementedError

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
        raise NotImplementedError

    def get_submissions(self) -> list:
        """
        Get all submission data from storage.

        :return: List of dictionaries, one for each submission
        :rtype: list
        """

        submissions = []
        submission_ids = self.list_submissions()
        for subid in submission_ids:
            submissions += [self.get_submission(subid)]

        return submissions

    def get_submissions_df(self) -> pd.DataFrame:
        """
        Get all submission data from storage, organized into a Pandas DataFrame.

        :return: Pandas DataFrame containing all submissions currently in storage
        :rtype: pandas.DataFrame
        """

        # fetch all submissions from storage
        submissions = self.get_submissions()

        # convert to DataFrame
        submissions_df = pd.DataFrame(submissions)

        # auto-detect and set data types where possible
        for col in submissions_df.columns:
            # count our non-NaN, non-empty-string values
            nvals = submissions_df.loc[submissions_df[col] != "", col].count()

            if nvals > 0:
                # try converting to datetime
                converted = pd.to_datetime(submissions_df[col], infer_datetime_format=True, errors="coerce")
                # if we didn't lose any data in the process, go with the converted version
                if converted.count() == nvals:
                    submissions_df[col] = converted
                else:
                    # try converting to numbers
                    converted = pd.to_numeric(submissions_df[col], errors="coerce")
                    # if we didn't lose any data in the process, go with the converted version
                    if converted.count() == nvals:
                        submissions_df[col] = converted
        # convert data types based on object types
        submissions_df = submissions_df.convert_dtypes()

        return submissions_df

    def attachments_supported(self) -> bool:
        """
        Query whether storage system supports attachments.

        :return: True if attachments supported, otherwise False
        :rtype: bool
        """
        return False

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

        :param attachment_location: Attachment location string (as returned when attachment stored)
        :type attachment_location: str
        :param submission_id: Unique submission ID (in lieu of attachment_location)
        :type submission_id: str
        :param attachment_name: Attachment filename (in lieu of attachment_location)
        :type attachment_name: str
        :return: True if submission exists in storage; otherwise False
        :rtype: bool
        """
        raise NotImplementedError

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
        raise NotImplementedError
