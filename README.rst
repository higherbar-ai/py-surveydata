=============
py-surveydata
=============

The ``surveydata`` Python package offers flexible access to survey data and support for
multiple local and cloud storage options.

Installation
------------

Installing the latest version with pip::

    pip install surveydata

Overview
--------

To use the ``surveydata`` package, you access data from specific survey platforms via an
appropriate ``SurveyPlatform`` object:

* ``SurveyCTOPlatform`` provides support for `SurveyCTO <https://www.surveycto.com>`_ data,
  including methods to process `text audits <https://docs.surveycto.com/02-designing-forms/01-core-concepts/03zd.field-types-text-audit.html>`_ and submit submission updates via the `review and correction workflow <https://docs.surveycto.com/04-monitoring-and-management/01-the-basics/04.reviewing-and-correcting.html>`_
  (in support of SurveyCTO's `machine learning roadmap <https://www.surveycto.com/blog/machine-learning-for-quality-control/>`_,
  with `the ml4qc project <https://github.com/orangechairlabs/ml4qc>`_)
* ``ODKPlatform`` provides support for `Open Data Kit <https://getodk.org/>`_ data via an `ODK Central <https://docs.getodk.org/central-intro/>`_ server

All survey data must be stored somewhere, and storage is handled via an appropriate
``StorageSystem`` object:

* ``FileStorage`` provides support for local file storage
* ``S3Storage`` provides support for `AWS S3 <https://aws.amazon.com/s3/>`_ storage
* ``DynamoDBStorage`` provides support for `AWS DynamoDB <https://aws.amazon.com/dynamodb/>`_ storage
* ``GoogleCloudStorage`` provides support for `Google Cloud Storage <https://cloud.google.com/storage>`_
* ``AzureBlobStorage`` provides support for `Azure Blob Storage <https://azure.microsoft.com/en-us/products/storage/blobs/>`_
* ``SurveyCTOExportStorage`` provides support for local data exported with `SurveyCTO Desktop <https://docs.surveycto.com/05-exporting-and-publishing-data/02-exporting-data-with-surveycto-desktop/01.using-desktop.html>`_ (in wide format)
* ``ODKExportStorage`` provides support for local data downloaded and unzipped from an `ODK Central <https://docs.getodk.org/central-intro/>`_ *All data and Attachments* export

In general, the workflow goes like this:

#. Initialize the survey platform
#. Initialize one or more storage systems
#. Synchronize data between the survey platform and the storage system(s) to ensure that
   data in storage is fully up-to-date (except for static export storage, via ``SurveyCTOExportStorage`` or ``ODKExportStorage``,
   which doesn't require synchronization)
#. Load data and/or attachments via the survey platform and storage API's
#. Optionally: Save processed data and then, later, load it back again, for cases where ingestion and processing tasks
   are separated from actual analysis or use

Examples
--------

See these notebooks for detailed usage examples:

* `SurveyCTO usage examples <https://github.com/orangechairlabs/py-surveydata/blob/main/src/surveydata-surveycto-examples.ipynb>`_
* `ODK usage examples <https://github.com/orangechairlabs/py-surveydata/blob/main/src/surveydata-odk-examples.ipynb>`_

Documentation
-------------

See the full reference documentation here:

    https://surveydata.readthedocs.io/

Development
-----------

To develop locally:

#. ``git clone https://github.com/orangechairlabs/py-surveydata.git``
#. ``cd py-surveydata``
#. ``python -m venv venv``
#. ``source venv/bin/activate``
#. ``pip install -r requirements.txt``

For convenience, the repo includes ``.idea`` project files for PyCharm.

To rebuild the documentation:

#. Update version number in ``/docs/source/conf.py``
#. Update layout or options as needed in ``/docs/source/index.rst``
#. In a terminal window, from the project directory:
    a. ``cd docs``
    b. ``SPHINX_APIDOC_OPTIONS=members,show-inheritance sphinx-apidoc -o source ../src/surveydata --separate --force``
    c. ``make clean html``

To rebuild the distribution packages:

#. For the PyPI package:
    a. Update version number (and any build options) in ``/setup.py``
    b. Confirm credentials and settings in ``~/.pypirc``
    c. Run ``/setup.py`` for the ``bdist_wheel`` and ``sdist`` build types (*Tools... Run setup.py task...* in PyCharm)
    d. Delete old builds from ``/dist``
    e. In a terminal window:
        i. ``twine upload dist/* --verbose``
#. For GitHub:
    a. Commit everything to GitHub and merge to ``main`` branch
    b. Add new release, linking to new tag like ``v#.#.#`` in main branch
#. For readthedocs.io:
    a. Go to https://readthedocs.org/projects/surveydata/, log in, and click to rebuild from GitHub (only if it doesn't automatically trigger)
