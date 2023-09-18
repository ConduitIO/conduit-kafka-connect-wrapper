#!/bin/bash

#
# Copyright 2023 Meroxa, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

usage() {
    echo "Usage: $0 GROUP_ID ARTIFACT_ID VERSION [OUTPUT_DIR]"
    echo "Arguments:"
    echo "  GROUP_ID:    The group ID of the artifact"
    echo "  ARTIFACT_ID: The artifact ID"
    echo "  VERSION:     The version of the artifact"
    echo "  OUTPUT_DIR:  (Optional) The output directory (default: dist/libs in the current directory)"
    exit 1
}

# Check if there are at least 3 arguments or if the --help flag is provided
if [ $# -lt 3 ] || [ "$1" == "--help" ]; then
    usage
fi

# Assign arguments to variables
GROUP_ID="$1"
ARTIFACT_ID="$2"
VERSION="$3"

# Check if OUTPUT_DIR is provided, otherwise set the default value
if [ $# -ge 4 ]; then
    OUTPUT_DIR="$4"
else
    OUTPUT_DIR="$(pwd)/dist/libs"
fi

# Print the values
echo "GROUP_ID: $GROUP_ID"
echo "ARTIFACT_ID: $ARTIFACT_ID"
echo "VERSION: $VERSION"
echo "OUTPUT_DIR: $OUTPUT_DIR"

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

mvn -f "$SCRIPT_DIR/download-connector-pom.xml" \
  -Dconnector.groupId=$GROUP_ID \
  -Dconnector.artifactId=$ARTIFACT_ID \
  -Dconnector.version=$VERSION \
  -DoutputDirectory=$OUTPUT_DIR \
  dependency:copy-dependencies \
