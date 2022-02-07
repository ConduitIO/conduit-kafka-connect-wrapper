#!/bin/bash
mvn clean package
echo '#!/usr/bin/java -jar' > kafka-connector-plugin
cat target/kafka-connector-plugin-*.jar >> kafka-connector-plugin
chmod +x kafka-connector-plugin
