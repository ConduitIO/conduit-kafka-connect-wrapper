#!/bin/bash

#
# Copyright 2024 Meroxa, Inc.
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

# Function to check if a string is a valid semantic version
is_semantic_version() {
    # Regular expression to match semantic versioning
    # Taken from: https://semver.org/#is-there-a-suggested-regular-expression-regex-to-check-a-semver-string
    regex='^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-((0|[1-9][0-9]*|[0-9]*[a-zA-Z-][0-9a-zA-Z-]*)(\.(0|[1-9][0-9]*|[0-9]*[a-zA-Z-][0-9a-zA-Z-]*))*))?(\+([0-9a-zA-Z-]+(\.[0-9a-zA-Z-]+)*))?$'
    [[ $1 =~ $regex ]]
}

# Check if version argument is provided
if [ -z "$1" ]; then
    echo "Usage: $0 <semantic-version>"
    exit 1
fi

# Assign the version provided as argument
version=$1

# Check if the provided version is a valid semantic version
if ! is_semantic_version "$version"; then
    echo "Error: Invalid semantic version. Please provide a valid semantic version."
    exit 1
fi

# Maven: Bump project version
mvn versions:set -DnewVersion=$version

# Maven: Commit the version change
git add pom.xml
git commit -m "Bump version to $version"

# Git: Tag the code
tag="v$version"
git tag $tag
git push tag $tag

# Call dist.sh script and check if it returns successfully
./scripts/dist.sh
dist_script_exit_code=$?

# Check if dist.sh script returned successfully
if [ $dist_script_exit_code -ne 0 ]; then
    echo "Error: scripts/dist.sh script failed. Aborting release process."
    exit 1
fi

# GitHub CLI: Create a new release
gh release create $tag
gh release upload $tag dist/*

# Maven: Set the development version
mvn versions:set -DnewVersion=next-version-SNAPSHOT
git add pom.xml
git commit -m "Set development version"

echo "Release process completed successfully."
