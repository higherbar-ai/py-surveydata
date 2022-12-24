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

"""Read-only support for ODK Central data exports."""

from surveydata import StorageSystem
import csv
import os
from typing import BinaryIO
import datetime
from urllib.parse import unquote_plus
import pandas as pd
import re


class ODKExportStorage(StorageSystem):
    """Implementation of storage interface for read-only access to ODK Central survey data exports."""

    # define constants
    ID_FIELD = "KEY"                            # unique submission ID field
    ATTACHMENTS_SUBDIR = "media"                # name of attachments subdirectory, if present

    def __init__(self, export_file: str, attachments_available: bool, data_timezone: datetime.timezone = None):
        """
        Initialize ODK Central export data.

        :param export_file: Path to the export file (typically unzipped from an "All data and Attachments" export
            from ODK Central
        :type export_file: str
        :param attachments_available: True if attachments available (must be in media subfolder, relative to
            export_file location)
        :type attachments_available: bool
        :param data_timezone: Timezone for timestamps in the data (defaults to current timezone if not specified)
        :type data_timezone: datetime.timezone
        """

        # save export file, attachment availability, and data timezone
        self.export_file = export_file
        self.attachments_available = attachments_available
        self.data_timezone = data_timezone

        # load main export file into memory
        file = open(export_file, 'rt', encoding="utf-8-sig")
        reader = csv.DictReader(file)
        self.submissions = list(reader)

        # look for additional export files for repeat groups
        repeat_filename_start = f"{os.path.splitext(os.path.split(export_file)[1])[0]}-"
        repeat_filename_end = ".csv"
        repeat_group_submissions=[]
        for possible_repeat_file in os.scandir(os.path.split(export_file)[0]):
            if possible_repeat_file.is_file():
                if possible_repeat_file.name.startswith(repeat_filename_start) \
                        and possible_repeat_file.name.endswith(repeat_filename_end):
                    # since it looks like a repeat-group export file, go ahead and load it into memory
                    file = open(possible_repeat_file, 'rt', encoding="utf-8-sig")
                    reader = csv.DictReader(file)
                    repeat_group_submissions += [ list(reader) ]

        # if we found and loaded repeat groups, process into single wide-format list
        if repeat_group_submissions:
            # create main DataFrame, then merge in each repeat group's data
            df = pd.DataFrame(self.submissions)
            df.set_index([self.ID_FIELD], inplace=True)
            df = df.sort_index()
            for repeat_group in repeat_group_submissions:
                # only worry about repeat groups that (a) have data, (b) have the proper repeat-group columns, and
                # (c) aren't otherwise empty
                if repeat_group and "KEY" in repeat_group[0] and "PARENT_KEY" in repeat_group[0] \
                        and len(repeat_group[0]) > 2:
                    # reshape data for merging, in dict of dicts
                    reshaped_repeat_group = {}
                    for row in repeat_group:
                        # calculate column prefix from KEY, then drop it
                        #   here, we're dropping the submission ID from the front, replacing the [#] indexes with
                        #   /#/, and subtracting 1 from each index to match how ODKPlatform indexes repeat values
                        column_prefix = re.sub(r"/\d+/", lambda m: f"/{int(m.group()[1:-1]) - 1}/",
                                               "/".join(row["KEY"].split('/')[1:]).replace("[", "/").replace("]", "")
                                               + "/")
                        del row["KEY"]
                        # calculate submission ID from PARENT_KEY, then drop it
                        submission_id = row["PARENT_KEY"].split("/")[0]
                        del row["PARENT_KEY"]

                        # add data into reshaped dict of dicts
                        if submission_id not in reshaped_repeat_group:
                            # submission not yet present, so start it out with the appropriate ID column
                            reshaped_repeat_group[submission_id] = {self.ID_FIELD: submission_id}
                        for key in row:
                            reshaped_repeat_group[submission_id][column_prefix + key] = row[key]

                    # convert reshaped repeat group to DataFrame
                    rg_df = pd.DataFrame(reshaped_repeat_group.values())
                    rg_df.set_index([self.ID_FIELD], inplace=True)
                    rg_df = rg_df.sort_index()

                    # merge repeat data into main DataFrame
                    df = df.merge(rg_df, how='left', left_index=True, right_index=True)

            # convert wide-format DataFrame back to list of dictionaries
            self.submissions = df.reset_index().to_dict('records')

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

    def store_metadata_binary(self, metadata_id: str, metadata: bytes):
        """
        Store metadata bytes in storage.

        :param metadata_id: Unique metadata ID (should begin and end with __ and not conflict with any submission ID)
        :type metadata_id: str
        :param metadata: Metadata bytes to store
        :type metadata: bytes
        """
        raise NotImplementedError

    def get_metadata_binary(self, metadata_id: str) -> bytes:
        """
        Get metadata bytes from storage.

        :param metadata_id: Unique metadata ID (should not conflict with any submission ID)
        :type metadata_id: str
        :return: Metadata bytes from storage, or empty bytes array if no such metadata exists
        :rtype: bytes
        """
        raise NotImplementedError

    def list_submissions(self) -> list:
        """
        List all submissions currently in storage.

        :return: List of submission IDs
        :rtype: list
        """

        # spin through all submissions to assemble our list of submissions
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

        :param submission_id: Optional submission ID, to list only attachments for specific submission (not supported
            in this storage type, since all submissions' attachments are mixed together in the same media folder)
        :type submission_id: str
        :return: List of attachments, each as dict with name, submission_id, and location_string (but in this case,
            we can't supply the submission_id, because all submissions' attachments are mixed together)
        :rtype: list
        """

        # assemble list of attachments, either for one submission or for all
        attachments = []
        if self.attachments_available:
            if submission_id:
                # because all submission attachments are mixed up in the same media folder, can't list separately by
                # submission
                raise NotImplementedError
            else:
                for attachment in os.scandir(os.path.join(os.path.split(self.export_file)[0],
                                                    ODKExportStorage.ATTACHMENTS_SUBDIR)):
                    if attachment.is_file():
                        # since ODK Central attachment locations are exported as just the filename (without "media/"),
                        # attachment location strings are just the same as the attachment names
                        attachments += [{"name": unquote_plus(attachment.name),
                                         "location_string": unquote_plus(attachment.name)}]

        # return all attachments found
        return attachments

    def query_attachment(self, attachment_location: str = "", submission_id: str = "",
                         attachment_name: str = "") -> bool:
        """
        Query whether specific submission attachment exists in storage.

        :param attachment_location: Attachment location string (as exported by ODK Central)
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
                                                    attachment_name=attachment_name)

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

        :param attachment_location: Attachment location string (as exported by ODK Central)
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
                                                    attachment_name=attachment_name)

        # open file and return
        return open(attpath, mode="rb")

    def _attachment_path_from_params(self, attachment_location: str = "", attachment_name: str = "") -> str:
        """
        Get attachment path from parameters, throwing exceptions as appropriate.

        :param attachment_location: Attachment location string (as exported by ODK Central)
        :type attachment_location: str
        :param attachment_name: Attachment filename (in lieu of attachment_location)
        :type attachment_name: str
        :return: Attachment object name
        :rtype: str

        Must pass either attachment_location or both submission_id and attachment_name.
        """

        # since ODK Central attachment locations are exported as just the filename (without "media/"), just
        # use the location as the name
        if attachment_location:
            attachment_name = attachment_location

        # construct attachment path from export file path and attachment name
        return os.path.join(os.path.join(os.path.split(self.export_file)[0],
                                         ODKExportStorage.ATTACHMENTS_SUBDIR), attachment_name)

    def set_data_timezone(self, tz: datetime.timezone):
        """
        Set the timezone for timestamps in the data.

        :param tz: Timezone for timestamps in the data
        :type tz: datetime.timezone
        """

        self.data_timezone = tz

    def get_data_timezone(self) -> datetime.timezone:
        """
        Get the timezone for timestamps in the data.

        :return: Timezone for timestamps in the data (defaults to datetime.timezone.utc if unknown)
        :rtype: datetime.timezone
        """

        return self.data_timezone if self.data_timezone is not None else datetime.datetime.now().astimezone().tzinfo
