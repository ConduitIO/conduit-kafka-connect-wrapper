#!/bin/bash
# Copies the script and the connector JAR into dist/.
TO_DIR=dist/
cp scripts/kafka-connector-plugin $TO_DIR
cp target/kafka-connector-plugin-0.1.0-SNAPSHOT.jar $TO_DIR
