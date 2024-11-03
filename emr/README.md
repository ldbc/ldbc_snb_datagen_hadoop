# AWS EMR support for the LDBC SNB Hadoop Datagen

AWS EMR script based on the [Spark Datagen's EMR tooling](https://github.com/ldbc/ldbc_snb_datagen_spark/tree/main/tools/emr).

## Prerequisites

Build the fat JAR as follows:

```bash
mvn clean package -DskipTests
```

This will place the JAR to `ldbc_snb_datagen_hadoop/target/ldbc_snb_datagen-${DATAGEN_VERSION}.jar`.

## Submitting Hadoop jobs

Usage:

```bash
python submit_hadoop_datagen_job.py --name ...
```
