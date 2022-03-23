#!/bin/bash
./dist-no-libs.sh
mkdir -p $TO_DIR/libs/
cp -r libs/ $TO_DIR/libs/

