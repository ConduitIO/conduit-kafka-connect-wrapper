### Conduit plugin for Kafka connectors
The Kafka connector Conduit plugin's goal is to make it possible to use existing Kafka connectors with Conduit.  

#### Pre-requisites
* JDK 11
* The plugin needs to have write permissions to the directory /var/log/conduit-kafka-connect-wrapper/
* Currently, only Unix-like OSes are supported.

### Development
The complete server-side code for this plugin is **not** committed to the repo. Rather, it's generated from a proto file,
when the project is compiled.

IDEs may not automatically add the generated sources to the class. If that's the case, you need to:
1. Run `mvn clean compile` (so that the needed code is generated)
2. Change the project settings in your IDE to include the generated source. In IntelliJ, for example, you do that by going
to File > Project structure > Project Settings > Modules. Then, right-click on `target/generated-source` and select "Sources".

#### Building and using the connector
Run `scripts/package.sh` to build an executable. `scripts/copy.sh` will create a directory called `dist` with following contents:
1. A script (which runs the connector)
2. The connector JAR itself
3. Directory `libs` with Aiven's JDBC connector (but no JDBC drivers).

### Loading connectors
The plugin will load connectors and all the other dependencies from a `libs` directory, which is expected to be in the 
same directory as the plugin executable itself. For example, if the plugin executable is at `/abc/def/conduit-kafka-connect-wrapper`,
then the dependencies are expected to be in `/abc/def/libs`.

The plugin will be able to find the dependencies as soon as they are put into `libs`. 

By default, Aiven's JDBC connector is shipped in the `libs` directory. A JDBC connector (generally) will require a 
database-specific driver to work (for example, PostgreSQL's driver can be found [here](https://mvnrepository.com/artifact/org.postgresql/postgresql)).

#### General notes

1. Logs are written to `/var/log/conduit-kafka-connect-wrapper/`, and not stdout. This is because the plugin is required to perform
a handshake with Conduit via standard output, and that is expected to be the first line in the standard output.
2. Currently, only sink connectors are supported. Work is under way to support source connectors too.
3. Currently, it's possible to use this plugin only on Unix-like systems.

#### Configuration
This plugin's configuration consists of the configuration of the requested Kafka connector, plus:

| Name                            | Description                                                                                  | Required                                                              | Default | Example                                                                                                                                                                                             | 
|---------------------------------|----------------------------------------------------------------------------------------------|-----------------------------------------------------------------------|---------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `connector.class`               | The class of the requested connector.                                                        | yes                                                                   | none    | `io.aiven.connect.jdbc.sink.JdbcSinkTask`                                                                                                                                                           |
| `schema`                        | The schema of the records which will be written to a destination connector.                  | the plugin doesn't require it, but the underlying Kafka connector may | none    | `{"type":"struct","fields":[{"type":"int32","optional":true,"field":"id"},{"type":"string","optional":true,"field":"name"},{"type":"boolean","optional":true,"field":"trial"}],"name":"customers"}` |
| `schema.autogenerate.enabled`   | Automatically generate schemas (destination connector). Cannot be `true` if a schema is set. | no                                                                    | `false` | `true`                                                                                                                                                                                              |
| `schema.autogenerate.name`      | Name of automatically generated schema.                                                      | yes, if schema auto-generation is turned on                           | none    | `customers`                                                                                                                                                                                         |
| `schema.autogenerate.overrides` | A (partial) schema which overrides types in the auto-generated schema.                       | no                                                                    | none    | `{"type":"struct","fields":[{"type":"boolean","optional":true,"field":"joined"}],"name":"customers"}`                                                                                               |
| `pipelineId`                    | The ID of the pipeline to which this connector will be added.                                | no                                                                    | none    |                                                                                                                                                                                                     |
| `connectorName`                 | The name of the connector which is to be created. Used in logs.                              | no                                                                    | none    | `prod-mysql-destination`                                                                                                                                                                            |

Here's a full example, for a new Conduit destination connector, backed up by a JDBC Kafka sink connector.
```json
{
  "wrapper.connector.class": "io.aiven.connect.jdbc.JdbcSourceConnector",
  "wrapper.connector.name": "my-pg-source",
  "wrapper.pipeline.id": "239bb7a4-dafd-4ee3-8a01-2b0d95b3be0e",
  "connection.url": "jdbc:postgresql://localhost/test-db",
  "connection.user": "test-user",
  "connection.password": "Passw0rd",
  "incrementing.column.name": "id",
  "mode": "incrementing",
  "tables": "my_table",
  "topic.prefix": "my_topic_prefix"
}
```

### Schema auto-generation
If `schema.autogenerate.enabled` is set to `true`, the plugin will try to automatically generate Kafka connector schemas 
for a destination. If schema auto-generation is enabled, then a schema name must be provided (through the `schema.autogenerate.name` parameters).

Optionally, it's possible to override types for individual fields. This is useful in cases where the plugin's inferred type
for a field is not suitable. To override types for individual fields, specify a schema through `schema.autogenerate.overrides`.
The specified schema is, of course, partial. 

Here's an example:
```json
{
    "type": "struct",
    "fields":
    [
        {
            "type": "boolean",
            "optional": false,
            "field": "joined"
        }
    ],
    "name": "customers"
}
```

In this example we specify a partial schema, where a single field, `joined`, is defined. Schema generator will skip its
own specifications for this field and instead use the provided one.

Schema auto-generation works differently for records with structured data and records with raw data.
1. Records with structured data: A record with structured data contains a `google.protobuf.Struct`. The mappings are as
follows:

| `google.protobuf.Struct` (protobuf) | Kafka schema                                                                      |
|-------------------------------------|-----------------------------------------------------------------------------------|
| bool                                | OPTIONAL_BOOLEAN_SCHEMA                                                           |
| number (only double is supported)   | OPTIONAL_FLOAT64_SCHEMA                                                           |
| string                              | OPTIONAL_STRING_SCHEMA                                                            |
| NullValue                           | STRUCT                                                                            |
| ListValue                           | ARRAY, where element types correspond to element type from the protobuf ListValue |

3. Records with raw data - TBD