#!/bin/bash
#
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

# This script helps automate the Beads process setup for TinkerPop.
# It initializes Beads and clones the tinkerpop/tinkerbeads database.
# Contributors can use this to quickly set up their local environment.

set -e

# Check if bd command exists
if ! command -v bd &> /dev/null; then
    echo "Error: 'bd' command not found. Please install Beads first: https://github.com/steveyegge/beads"
    exit 1
fi

# Check if dolt command exists
if ! command -v dolt &> /dev/null; then
    echo "Error: 'dolt' command not found. Please install Dolt first: https://www.dolthub.com/docs/introduction/installation/"
    exit 1
fi

# Check if dolt credentials are valid
echo "Checking Dolt credentials..."
if ! dolt creds check; then
    echo "Error: Dolt credentials check failed."
    echo "Please run 'dolt login' to authenticate with DoltHub."
    exit 1
fi

# Ensure we are in the project root
if [ ! -f "pom.xml" ]; then
    echo "Error: This script must be run from the TinkerPop project root."
    exit 1
fi

echo "Initializing Beads..."
# Run bd init if .beads directory doesn't exist
if [ ! -d ".beads" ]; then
    bd init --skip-agents --skip-hooks --database tinkerbeads --prefix tinkerpop
else
    echo ".beads directory already exists. Skipping 'bd init'."
fi

echo "Setting up Dolt database..."
# Navigate to the embedded dolt directory
mkdir -p .beads/embeddeddolt
pushd .beads/embeddeddolt > /dev/null

# Remove existing database if any, to allow cloning
if [ -d "tinkerbeads" ]; then
    echo "Removing existing local tinkerbeads database..."
    rm -rf tinkerbeads
fi

echo "Cloning tinkerpop/tinkerbeads from DoltHub..."
dolt clone tinkerpop/tinkerbeads

popd > /dev/null

echo "Removing project_id from metadata.json to avoid identity mismatch..."
if [ -f ".beads/metadata.json" ]; then
    # Use jq to safely remove the project_id field.
    if command -v jq &> /dev/null; then
        jq 'del(.project_id)' .beads/metadata.json > .beads/metadata.json.tmp && mv .beads/metadata.json.tmp .beads/metadata.json
    else
        # Fallback to sed if jq is not available
        sed -i '/"project_id":/d' .beads/metadata.json
    fi
fi

echo "Finalizing setup..."
bd ready

echo "Beads bootstrap complete for TinkerPop!"
