### Conduit connector for Kafka connectors

The connector's goal is to make it possible to use existing Kafka connectors with Conduit.

#### Pre-requisites

* JDK 11
* The connector needs to have write permissions to the directory /var/log/kafka-connector-plugin/
* Currently, only Unix-like OSes are supported.

### Development

The complete server-side code for this connector is **not** committed to the repo. Rather, it's generated from a proto
file, when the project is compiled.

IDEs may not automatically add the generated sources to the class. If that's the case, you need to:

1. Run `mvn clean compile` (so that the needed code is generated)
2. Change the project settings in your IDE to include the generated source. In IntelliJ, for example, you do that by
   going to File > Project structure > Project Settings > Modules. Then, right-click on `target/generated-source` and
   select "Sources".

#### How to build

Run `scripts/package.sh` to build an executable. For development purposes, a utility script, `scripts/copy.sh`, is
provided to also quickly copy the executable to Conduit's plugin directory (it assumes that the repository is
at `../conduit`).

### Loading connectors

The connector will load all Kafka connectors and all the other dependencies from a `libs` directory, which is expected to be in the
same directory as the connector executable itself. For example, if the connector executable is
at `/abc/def/kafka-connector-plugin`, then the dependencies are expected to be in `/abc/def/libs`.

The connector will be able to find the dependencies as soon as they are put into `libs`.

By default, Aiven's JDBC connector is shipped in the `libs` directory. A JDBC connector (generally) will require a
database-specific driver to work (for example, PostgreSQL's driver can be
found [here](https://mvnrepository.com/artifact/org.postgresql/postgresql)).

#### General notes

1. Logs are written to `/var/log/kafka-connector-plugin/`, and not stdout. This is because the connector is required to
   perform a handshake with Conduit via standard output, and that is expected to be the first line in the standard
   output.
2. Currently, only sink connectors are supported. Work is under way to support source connectors too.
3. Currently, it's possible to use this connector only on Unix-like systems.

#### Configuration

This connector's configuration consists of the configuration of the requested Kafka connector, plus:

| Name                  | Description                                                                        | Required | Default | Example                                                                                                                                                                                             | 
|-----------------------|------------------------------------------------------------------------------------|----------|---------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `task.class`          | The class of the requested connector                                               | yes      | none    | `io.aiven.connect.jdbc.sink.JdbcSinkTask`                                                                                                                                                           |
| `schema`              | The schema of the records which will be written to a destination connector.        | no       | none    | `{"type":"struct","fields":[{"type":"int32","optional":true,"field":"id"},{"type":"string","optional":true,"field":"name"},{"type":"boolean","optional":true,"field":"trial"}],"name":"customers"}` |
| `schema.autogenerate` | Automatically infer schema in destination connectors. More about the option below. | no       | `false` | `false`                                                                                                                                                                                             |
| `pipelineId`          | The ID of the pipeline to which this connector will be added.                      | no       | none    |                                                                                                                                                                                                     |
| `connectorName`       | The name of the connector which is to be created. Used in logs.                    | no       | none    | `prod-mysql-destination`                                                                                                                                                                            |

Here's a full example, for a new Conduit destination connector, backed up by a JDBC Kafka sink connector.

```
{
	"task.class": "io.aiven.connect.jdbc.sink.JdbcSinkTask",
	"schema": "{\"type\":\"struct\",\"fields\":[{\"type\":\"int32\",\"optional\":true,\"field\":\"id\"},{\"type\":\"string\",\"optional\":true,\"field\":\"name\"},{\"type\":\"boolean\",\"optional\":true,\"field\":\"trial\"}],\"name\":\"customers\"}",
	"connectorName": "local-pg-destination",
	"pipelineId": "123-456-789",
	"connection.url": "jdbc:postgresql://localhost/my-test-db",
	"connection.user": "user",
	"connection.password": "password123456",
	"auto.create":    "true",
	"auto.evolve":    "true",
	"batch.size": "10"
}
```

#### Schema auto-generation
This connector is able to automatically generate a schema for records coming into a destination connector. To do so, set
`schema.autogenerate` to `true` in your destination connector configuration.

 The way schema auto-generation works is following:
 1. The schema is generated for **each** record coming in. Schemas are not merged (this is TBD).
 2. If the record contains raw data, it will be assumed that the data is in JSON format. Also:
    1. For all integer numbers