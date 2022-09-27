=============
py-surveydata
=============

The ``surveydata`` Python package offers flexible access to survey data.

--------
Overview
--------

To use the ``surveydata`` package, you access data from specific survey platforms via an
appropriate ``SurveyPlatform`` object:

* ``SurveyCTOPlatform`` provides support for `SurveyCTO <https://www.surveycto.com>`_ data,
  including `text audits <https://docs.surveycto.com/02-designing-forms/01-core-concepts/03zd.field-types-text-audit.html>`_

All survey data must be stored somewhere, and storage is handled via an appropriate
``StorageSystem`` object:

* ``FileStorage`` provides support for local file storage
* ``S3Storage`` provides support for `AWS S3 <https://aws.amazon.com/s3/>`_ storage
* ``DynamoDBStorage`` provides support `AWS DynamoDB <https://aws.amazon.com/dynamodb/>`_ storage
* ``SurveyCTOExportStorage`` provides support for local exports from `SurveyCTO Desktop <https://docs.surveycto.com/05-exporting-and-publishing-data/02-exporting-data-with-surveycto-desktop/01.using-desktop.html>`_

In general, the workflow goes like this:

#. Initialize the survey platform
#. Initialize one or more storage systems
#. Synchronize data between the survey platform and the storage system(s) to ensure that
   data in storage is fully up-to-date
#. Load data and/or attachments via the survey platform and storage API's

(When using a static data export for storage, via a class like ``SurveyCTOExportStorage``,
the *synchronize* step is skipped, but otherwise everything is the same.)

--------
Examples
--------

See `this example notebook <https://github.com/orangechairlabs/py-surveydata/blob/main/src/surveydata-surveycto-examples.ipynb>`_
for a series of usage examples.