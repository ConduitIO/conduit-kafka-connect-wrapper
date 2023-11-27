#!/bin/bash

#
# Copyright 2022 Meroxa, Inc.
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

# Function to extract the version from pom.xml
get_version() {
    local version
    version=$(grep -A 2 '<artifactId>jdbc-connector-for-apache-kafka</artifactId>' pom.xml | grep '<version>' | awk -F '>' '{print $2}' | awk -F '<' '{print $1}')
    echo "$version"
}

# Check if the required input directory is provided
if [ -z "$1" ]; then
    echo "Usage: $0 <output_directory>"
    exit 1
fi

output_directory=$1

# Get the version from pom.xml
version=$(get_version)

if [ -z "$version" ]; then
    echo "Error: Version not found in pom.xml"
    exit 1
fi

# Clone the repository with the specified version
repo_url="git@github.com:Aiven-Open/jdbc-connector-for-apache-kafka.git"
clone_url="$repo_url"

echo "Cloning repository $clone_url into $output_directory..."
git clone "$clone_url" "$output_directory/jdbc-connector-for-apache-kafka"

cd "$output_directory/jdbc-connector-for-apache-kafka" || exit

git checkout "tags/v$version"

./gradlew clean build publishToMavenLocal -x test

echo "Done."
