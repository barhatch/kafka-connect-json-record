Kafka Connect SMT to add a schema for persistence to Postgres

This SMT adds a schema to a schemaless value
Properties:

| Name                | Description           | Type   | Default | Importance |
| ------------------- | --------------------- | ------ | ------- | ---------- |
| `decode.field.name` | Field name for record | String | `uuid`  | High       |

Example configuration:

```
"transforms": "decode",
"transforms.decode.type": "com.github.barhatch.kafka.connect.smt.AddSchema$Value",
"transforms.decode.field.name": "record"
```

To use the connector, copy it to the /data/connect-jars folder on a connect pod (this should be a persistent volume & shared between all connect pods):
`kubectl cp ./kafka-connect-add-schema-1.0-SNAPSHOT.jar kafka/connect-<CONTAINERID>:/data/connect-jars`

Lots borrowed from the Apache Kafka® `InsertField` SMT
