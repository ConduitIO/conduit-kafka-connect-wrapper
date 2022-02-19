rm -rf ../conduit/pkg/plugins/kafka-connector-plugin/
mkdir -p ../conduit/pkg/plugins/kafka-connector-plugin/

cp scripts/kafka-connector-plugin ../conduit/pkg/plugins/kafka-connector-plugin/
cp -r libs/ ../conduit/pkg/plugins/kafka-connector-plugin/libs/
cp target/kafka-connector-plugin-0.1.0-SNAPSHOT.jar ../conduit/pkg/plugins/kafka-connector-plugin/