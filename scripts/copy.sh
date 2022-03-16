#!/bin/bash
# Copies the script, the connector JAR and pre-packaged dependencies into dist/.
TO_DIR=dist/
mkdir -p $TO_DIR

cp scripts/kafka-connector-plugin $TO_DIR
cp -r libs/ $TO_DIR/libs/
cp target/kafka-connector-plugin-0.1.0-SNAPSHOT.jar $TO_DIR
