#!/bin/bash
mvn install:install-file -Dfile=libs/jdbc-connector-for-apache-kafka-6.7.0-SNAPSHOT.jar -DgroupId=io.aiven -DartifactId=aiven-kafka-connect-jdbc -Dversion=6.7.0-SNAPSHOT -Dpackaging=jar
