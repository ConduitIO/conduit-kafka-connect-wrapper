#!/bin/bash
mvn clean package
echo '#!/usr/bin/env java -jar' > kafka-connector-plugin
cat target/kafka-connector-plugin-*.jar >> kafka-connector-plugin
chmod +x kafka-connector-plugin
