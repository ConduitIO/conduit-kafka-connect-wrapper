#!/bin/bash
mvn clean package

TO_DIR=dist/
rm -r $TO_DIR
mkdir -p $TO_DIR/libs
cp scripts/conduit-kafka-connect-wrapper $TO_DIR
cp target/conduit-kafka-connect-wrapper-*.jar $TO_DIR/libs
