#!/bin/bash
mvn clean package
echo '#!/usr/bin/env java -cp "kafka-connector-plugin-0.1.0-SNAPSHOT.jar:libs/*" io.conduit.Application' > kafka-connector-plugin
chmod +x kafka-connector-plugin
