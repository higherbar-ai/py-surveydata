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

from setuptools import setup

with open('README.rst') as file:
    readme = file.read()

setup(
    name='surveydata',
    version='0.1.3',
    packages=['surveydata'],
    package_dir={'': 'src'},
    url='https://github.com/orangechairlabs/py-surveydata',
    project_urls={'Documentation': 'https://surveydata.readthedocs.io/'},
    license='Apache 2.0',
    author='Christopher Robert',
    author_email='crobert@orangechairlabs.com',
    description='Flexible access to survey data',
    long_description=readme
)
