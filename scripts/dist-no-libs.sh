#!/bin/bash
mvn clean package

# Copies the script and the connector JAR into dist/.
TO_DIR=dist/
mkdir -p $TO_DIR
cp scripts/conduit-kafka-connect-wrapper $TO_DIR
cp target/conduit-kafka-connect-wrapper-0.1.0-SNAPSHOT.jar $TO_DIR
