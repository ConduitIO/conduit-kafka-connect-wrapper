#!/bin/bash
mvn clean package

TO_DIR=dist/
mkdir -p $TO_DIR/libs
cp scripts/conduit-kafka-connect-wrapper $TO_DIR
cp target/conduit-kafka-connect-wrapper-0.1.0-SNAPSHOT.jar $TO_DIR
