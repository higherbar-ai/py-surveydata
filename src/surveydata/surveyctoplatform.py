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

"""Support for SurveyCTO as a survey data platform."""

from surveydata.surveyplatform import SurveyPlatform
from surveydata.storagesystem import StorageSystem
import requests
from urllib.parse import quote
from datetime import datetime
import pandas as pd


class SurveyCTOPlatform(SurveyPlatform):
    """SurveyCTO survey data platform implementation."""

    # define constants
    ID_FIELD = "KEY"                                    # unique submission ID field
    CURSOR_METADATA_ID = "__CURSOR__"                   # unique metadata ID for cursor (must start and end with __)

    def __init__(self, server: str = "", username: str = "", password: str = "", formid: str = "",
                 private_key: str = ""):
        """
        Initialize SurveyCTO for access to survey data.

        :param server: SurveyCTO server name (like "use", without the https prefix or .surveycto.com suffix)
        :type server: str
        :param username: Email address for API access
        :type username: str
        :param password: Password for API access
        :type password: str
        :param formid: SurveyCTO form ID
        :type formid: str
        :param private_key: Full text of private key, if using encryption
        :type private_key: str

        If you're not going to call sync_data(), you don't need to supply any of the parameters to this constructor.
        """

        self.server = server
        self.formid = formid
        self.private_key = private_key
        if username and password:
            self.creds = requests.auth.HTTPBasicAuth(username, password)
        else:
            # allow for initialization without sync_data() support
            self.creds = None

        # call base class constructor as well
        super().__init__()

    def sync_data(self, storage: StorageSystem, attachment_storage: StorageSystem = None,
                  no_attachments: bool = False, review_statuses: list = None) -> list:
        """
        Sync survey data to storage system.

        :param storage: Storage system for submissions (and attachments, if supported and other options don't override)
        :type storage: StorageSystem
        :param attachment_storage: Separate storage system for attachments (only if needed)
        :type attachment_storage: StorageSystem
        :param no_attachments: True to not sync attachments
        :type no_attachments: bool
        :param review_statuses: List of review statuses to include (any combo of "approved", "pending", "rejected";
            if not specified, syncs only approved submissions)
        :type review_statuses: list
        :return: List of new submissions stored (submission ID strings)
        :rtype: list
        """

        # fire an exception if we haven't been initialized for syncing
        if not self.server or not self.formid or self.creds is None:
            raise ValueError("SurveyCTOPlatform not initialized with parameters sufficient for syncing data (server, "
                             "formid, username, password).")

        # decide where attachments should go (if anywhere)
        if no_attachments:
            attachment_storage = None
        elif attachment_storage is None:
            attachment_storage = storage

        # fetch current cursor from storage
        cursor = storage.get_metadata(self.CURSOR_METADATA_ID)

        # pull data via server API
        api_url = f"https://{self.server}.surveycto.com/api/v2/forms/data/wide/json/{self.formid}?date=" \
                  + quote(cursor if cursor else "0")
        # (with non-default list of review statuses, if supplied)
        if review_statuses is not None and review_statuses:
            api_url += "&r=" + quote("|".join(review_statuses))
        # (and with private key, if supplied)
        if self.private_key:
            response = requests.post(api_url, files={"private_key": self.private_key}, auth=self.creds)
        else:
            response = requests.get(api_url, auth=self.creds)

        # raise errors as exceptions
        response.raise_for_status()

        # parse and process response
        data = response.json()
        new_submission_list = []
        if data:
            # start out presuming that the last submission is the one with the latest CompletionDate
            newcursor = data[-1]["CompletionDate"]
            newcursor_dt = datetime.strptime(newcursor, "%b %d, %Y %I:%M:%S %p")

            # loop through to process each submission
            for submission in data:
                # if the submission's CompletionDate is greater than our presumed new cursor, use it instead
                #   (this is out of an abundance of caution, as it appears that SurveyCTO sends submissions in
                #    CompletionDate order)
                if datetime.strptime(submission["CompletionDate"], "%b %d, %Y %I:%M:%S %p") > newcursor_dt:
                    newcursor = submission["CompletionDate"]
                    newcursor_dt = datetime.strptime(newcursor, "%b %d, %Y %I:%M:%S %p")

                # generally, we want to write submissions to storage, even if we already have them — but, for
                # efficiency reasons, we don't want to keep re-storing the most recent submission when it matches
                # the cursor we used for the query (since the API query is inclusive of the date used in the cursor)
                subid = submission[self.ID_FIELD]
                if submission["CompletionDate"] != cursor or not storage.query_submission(subid):
                    # if we have somewhere to save attachments — and it supports attachments — save them first,
                    # updating their location URLs
                    if attachment_storage is not None and attachment_storage.attachments_supported():
                        attachment_prefix = f"https://{self.server}.surveycto.com/api/v2/forms/{self.formid}"\
                                            f"/submissions/{subid}/attachments/"
                        for field, value in submission.items():
                            value_str = str(value)
                            if value_str.startswith(attachment_prefix):
                                # fields that match our unique attachment URL format are presumed to be attachments
                                attachment_name = value_str[len(attachment_prefix):]
                                # stream the file from the server
                                if self.private_key:
                                    attresponse = requests.post(value_str, files={"private_key": self.private_key},
                                                                auth=self.creds, stream=True)
                                else:
                                    attresponse = requests.get(value_str, auth=self.creds, stream=True)
                                # stream straight to storage
                                attresponse.raw.decode_content = True
                                attlocation = attachment_storage.store_attachment(subid,
                                                                                  attachment_name=attachment_name,
                                                                                  attachment_data=attresponse.raw)

                                # update data to reference new location
                                submission[field] = attlocation

                    # finally, save the submission itself and remember in list of new submissions
                    storage.store_submission(subid, submission)
                    new_submission_list += [subid]

            # update our cursor, if it changed
            if newcursor != cursor:
                storage.store_metadata(self.CURSOR_METADATA_ID, newcursor)

        return new_submission_list

    @staticmethod
    def get_submissions_df(storage: StorageSystem) -> pd.DataFrame:
        """
        Get all submission data from storage, organized into a Pandas DataFrame and optimized based on the platform.

        :param storage: Storage system for submissions
        :type storage: StorageSystem
        :return: Pandas DataFrame containing all submissions currently in storage
        :rtype: pandas.DataFrame
        """

        # convert to DataFrame
        submissions_df = storage.get_submissions_df()

        # set to index by KEY
        submissions_df.set_index([SurveyCTOPlatform.ID_FIELD], inplace=True)
        submissions_df = submissions_df.sort_index()

        return submissions_df

    @staticmethod
    def get_text_audit_df(storage: StorageSystem, location_string: str = "",
                          location_strings: pd.Series = None) -> pd.DataFrame:
        """
        Get one or more text audits from storage, organized into a Pandas DataFrame.

        :param storage: Storage system for attachments
        :type storage: StorageSystem
        :param location_string: Location string of single text audit to load
        :type location_string: str
        :param location_strings: Series of location strings of text audits to load
        :type location_strings: pandas.Series
        :return: DataFrame with either the single text audit contents or all text audit contents indexed by Series index
        :rtype: pandas.DataFrame

        Pass either a single location_string or a Series of location_strings.
        """

        # first confirm parameters passed correctly
        if location_string and location_strings is not None:
            raise ValueError("Pass either location_string or location_strings to load one or more text audits "
                             "(not both).")

        # load single text audit
        if location_string:
            df = SurveyCTOPlatform._load_text_audit(storage, location_string)
        elif location_strings is not None:
            df = None
            for subid, locstr in location_strings.items():
                if not pd.isna(locstr) and locstr:
                    row_df = SurveyCTOPlatform._load_text_audit(storage, locstr)
                    row_df[SurveyCTOPlatform.ID_FIELD] = subid
                    if df is None:
                        df = row_df
                    else:
                        df = pd.concat([df, row_df], copy=False, ignore_index=True, sort=False)

            # set to index by KEY
            if df is not None:
                df.set_index([SurveyCTOPlatform.ID_FIELD], inplace=True)
                df = df.sort_index()
        else:
            raise ValueError("Pass either location_string or location_strings to load one or more text audits.")

        return df

    @staticmethod
    def _load_text_audit(storage: StorageSystem, location_string: str) -> pd.DataFrame:
        """
        Load a single text audit file from storage to a Pandas DataFrame.

        :param storage: Storage system for attachments
        :type storage: StorageSystem
        :param location_string: Location string of single text audit to load
        :type location_string: str
        :return: DataFrame with the single text audit contents
        :rtype: pandas.DataFrame
        """

        # load text audit .csv file into a file-type object
        tafile = storage.get_attachment(attachment_location=location_string)

        # then load into a DataFrame
        df = pd.read_csv(tafile, header=0)

        # then rename columns
        df.rename(columns={'Field name': 'field', 'Total duration (seconds)': 'duration_s',
                           'First appeared (seconds into survey)': 'visited_s', 'Choices values': 'values',
                           'Choices labels': 'labels', 'Device time': 'device_time', 'Form time (ms)': 'form_time_ms',
                           'Event': 'event', 'Duration (ms)': 'duration_ms'}, inplace=True)

        # finally, return the DataFrame
        return df
