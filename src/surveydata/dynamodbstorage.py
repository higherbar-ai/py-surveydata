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

"""Support for AWS DynamoDB survey data storage."""

from surveydata.storagesystem import StorageSystem
import boto3
from boto3.dynamodb import conditions
from typing import BinaryIO


class DynamoDBStorage(StorageSystem):
    """AWS DynamoDB survey data storage implementation."""

    # define constants
    CURSOR_ID = "__CURSOR__"                        # faux submission ID for cursor
    CURSOR_KEY = "Cursor"                           # key for cursor value

    def __init__(self, aws_region: str, table_name: str, id_field_name: str, partition_key_name: str = "",
                 partition_key_value: str = "", aws_access_key_id: str = None, aws_secret_access_key: str = None,
                 aws_session_token: str = None):
        """
        Initialize DynamoDB storage for survey data.

        :param aws_region: AWS region to use
        :type aws_region: str
        :param table_name: DynamoDB table name (must already exist)
        :type table_name: str
        :param id_field_name: Field name for unique submission ID (e.g., "KEY")
        :type id_field_name: str
        :param partition_key_name: Partition key name for optional fixed partition (e.g., "FormID")
        :type partition_key_name: str
        :param partition_key_value: Partition value for optional fixed partition (e.g., form ID)
        :type partition_key_value: str
        :param aws_access_key_id: AWS access key ID; if None, will use local config file and/or environment vars
        :type aws_access_key_id: str
        :param aws_secret_access_key: AWS access key secret; if None, will use local config file and/or environment vars
        :type aws_secret_access_key: str
        :param aws_session_token: AWS session token to use, only if using temporary credentials
        :type aws_session_token: str

        The DynamoDB table should already exist with the primary key configured in one of two ways:
          #. a fixed partition key with the name passed as partition_key_name, and the sort key with the name passed
             as id_field_name; or
          #. a partition key with the name passed as id_field_name (and no sort key).
        """

        # start an AWS session, and use passed credentials (if specified)
        self.aws_session = boto3.Session(region_name=aws_region, aws_access_key_id=aws_access_key_id,
                                         aws_secret_access_key=aws_secret_access_key,
                                         aws_session_token=aws_session_token)

        # open a DynamoDB resource and table
        self.dynamodb = self.aws_session.resource('dynamodb')
        self.table = self.dynamodb.Table(table_name)

        # save our table name, ID field name, and partition key (if any)
        self.table_name = table_name
        self.id_field_name = id_field_name
        self.partition_key_name = partition_key_name
        self.partition_key_value = partition_key_value

        # call base class constructor as well
        super().__init__()

    def store_cursor(self, cursor: str):
        """
        Store cursor string in storage.

        :param cursor: Cursor string to store.
        :type cursor: str
        """

        # store cursor as faux submission with CURSOR_ID as the submission ID
        cursor_dict = self.submission_primary_key(self.CURSOR_ID)
        cursor_dict[self.CURSOR_KEY] = cursor
        self.table.put_item(Item=cursor_dict)

    def get_cursor(self) -> str:
        """
        Get cursor string from storage.

        :return: Cursor string from storage, or empty string if no cursor exists
        :rtype: str
        """

        # try to fetch the cursor
        response = self.table.get_item(Key=self.submission_primary_key(self.CURSOR_ID))
        if "Item" in response:
            # cursor found, so return cursor value
            return response["Item"][self.CURSOR_KEY]
        else:
            # cursor not found, so return empty string
            return ""

    def list_submissions(self) -> list:
        """
        List all submissions currently in storage.

        :return: List of submission IDs
        :rtype: list
        """

        # query for all submissions, possibly within a fixed partition
        if self.partition_key_name:
            response = self.table.query(
                KeyConditionExpression=conditions.Key(self.partition_key_name).eq(self.partition_key_value),
                ProjectionExpression="#id", ExpressionAttributeNames = {"#id": self.id_field_name})
            page_type = "query"
        else:
            response = self.table.scan(ProjectionExpression="#id",
                                       ExpressionAttributeNames = {"#id": self.id_field_name})
            page_type = "scan"

        # loop through all found submissions on all pages of results
        submissions = []
        while True:
            if "Items" in response:
                for item in response["Items"]:
                    # add any non-cursor submissions to the list to return
                    if item[self.id_field_name] != self.CURSOR_ID:
                        submissions += [item[self.id_field_name]]

            # keep on to the next page if there is one, otherwise break from loop
            if response.get("LastEvaluatedKey"):
                if page_type == "query":
                    response = self.table.query(
                        KeyConditionExpression=conditions.Key(self.partition_key_name).eq(self.partition_key_value),
                        ProjectionExpression="#id", ExpressionAttributeNames = {"#id": self.id_field_name},
                        ExclusiveStartKey=response["LastEvaluatedKey"])
                else:
                    response = self.table.scan(ProjectionExpression="#id",
                                               ExpressionAttributeNames = {"#id": self.id_field_name},
                                               ExclusiveStartKey=response["LastEvaluatedKey"])
            else:
                break

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

        # query for submission (only fetching the ID)
        response = self.table.get_item(Key=self.submission_primary_key(submission_id),
                                       ProjectionExpression="#id",
                                       ExpressionAttributeNames = {"#id": self.id_field_name})
        # return success only if the submission was found
        return "Item" in response

    def store_submission(self, submission_id: str, submission_data: dict):
        """
        Store submission data in storage.

        :param submission_id: Unique submission ID
        :type submission_id: str
        :param submission_data: Submission data to store
        :type submission_data: dict
        """

        # if we have a partition, store its value in the submission record
        if self.partition_key_name:
            submission_data[self.partition_key_name] = self.partition_key_value

        # store submission data directly in table
        self.table.put_item(Item=submission_data)

    def get_submission(self, submission_id: str) -> dict:
        """
        Get submission data from storage.

        :param submission_id: Unique submission ID
        :type submission_id: str
        :return: Submission data (or empty dictionary if submission not found)
        :rtype: dict
        """

        # try to fetch the submission, returning an empty dictionary if it's not found
        response = self.table.get_item(Key=self.submission_primary_key(submission_id))
        if "Item" in response:
            return response["Item"]
        else:
            return {}

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

        Must pass either attachment_location or both submission_id and attachment_name.
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

    def submission_primary_key(self, submission_id: str) -> dict:
        """
        Get submission primary key for specific submission.

        :param submission_id: Unique submission ID
        :type submission_id: str
        :return: Primary key for submission
        :rtype: dict
        """

        if self.partition_key_name:
            return {self.partition_key_name: self.partition_key_value, self.id_field_name: submission_id}
        else:
            return {self.id_field_name: submission_id}
