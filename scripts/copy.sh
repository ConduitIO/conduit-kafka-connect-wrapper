TO_DIR=../conduit/pkg/plugins/kafka-connector-plugin/
rm -rf $TO_DIR
mkdir -p $TO_DIR

cp scripts/kafka-connector-plugin $TO_DIR
cp -r libs/ $TO_DIR/libs/
cp target/kafka-connector-plugin-0.1.0-SNAPSHOT.jar $TO_DIR
