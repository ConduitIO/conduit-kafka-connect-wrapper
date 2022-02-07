### Conduit plugin for Kafka connectors
The Kafka connector Conduit plugin's goal is to make it possible to use existing Kafka connectors with Conduit.  

#### Pre-requisites
* JDK 11
* To use the JDBC Kafka connectors, you need to have [Aiven's JDBC Connectors for Apache Kafka®](https://github.com/aiven/jdbc-connector-for-apache-kafka) 
installed to your local Maven repository (by running `mvn install` in a clone of that repository)
* The plugin needs to have write permissions to the directory /var/log/kafka-connector-plugin/

#### How to build
Run `scripts/package.sh` to build an executable. For development purposes, a utility script, `scripts/copy.sh`, is provided 
to also quickly copy the executable to Conduit's plugin directory (it assumes that the repository is at `../conduit`).

#### General notes

1. Logs are written to `/var/log/kafka-connector-plugin/`, and not stdout. This is because the plugin is required to perform
a handshake with Conduit via standard output, and that is expected to be the first line in the standard output.
2. Currently, only sink connectors are supported. Work is under way to support source connectors too.
3. Currently, it's possible to use this plugin only on Unix-like systems.