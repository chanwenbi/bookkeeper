#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

source scripts/common.sh

BINDIR=`dirname "$0"`
DOC_HOME=`cd $BINDIR/..;pwd`

LATEST_VERSION=`grep latest_version _config.yml | sed 's/^latest_version: "\(.*\)"/\1/'`

if [[ $LATEST_VERSION =~ "*-SNAPSHOT" ]]; then
  echo "Latest version is not a SNAPSHOT version : $LATEST_VERSION"
  exit 1
fi

RELEASE_VERSION=`echo $LATEST_VERSION | sed 's/^\(.*\)-SNAPSHOT/\1/'`

echo "Releasing version $RELEASE_VERSION ..."

# create a release directory

if [[ -d ${DOC_HOME}/docs/${RELEASE_VERSION} ]]; then
  echo "Release $RELEASE_VERSION docs dir '${DOC_HOME}/docs/${RELEASE_VERSION}' already exists."
  exit 1
fi

cp -r ${DOC_HOME}/docs/latest ${DOC_HOME}/docs/${RELEASE_VERSION}

# add the release to git repo
git add ${DOC_HOME}/docs/${RELEASE_VERSION}

cd ${DOC_HOME}/docs/${RELEASE_VERSION}

find . -name *.md | xargs sed -i'.bak' "s/{{ site\.latest_version }}/${RELEASE_VERSION}/"
find . -name *.md.bak | xargs rm
find . -name *.md | xargs sed -i'.bak' "s/${LATEST_VERSION}/${RELEASE_VERSION}/"
find . -name *.md.bak | xargs rm
cp releaseNotesTemplate.md releaseNotes.md

# go to doc home

cd ${DOC_HOME}

REGEX="""s/## News/
## News

### [date]: release ${RELEASE_VERSION} available/

[INERT SUMMARY]

"""
find releases.md | xargs sed -i'.bak' "$REGEX"
