#!/bin/bash
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
"$SCRIPT_DIR"/dist-no-libs.sh

TO_DIR=dist/
cp -r libs/ $TO_DIR/
