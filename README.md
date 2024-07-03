Managed Iceberg Examples
------------------------

These examples demonstrate how to use Dataflow's new [Managed](https://cloud.google.com/dataflow/docs/guides/managed-io)
IcebergIO to read and write data to Iceberg tables.

Included are examples for both Hadoop and BigLake catalogs. Dependencies can be found in their respective `build.gradle`
files. To run these examples, fork this repository and follow the instructions below.

**Note**: DirectRunner is used by default. Feel free to tag on `--runner=DataflowRunner` to run on Dataflow.

## Run Hadoop examples with the following gradle commands

### _Write_
```bash
./gradlew hadoop:execute -PmainClass=org.example.hadoop.WriteExample \
    -Pexec.args="--catalogName=$CATALOG_NAME --table=$TABLE --warehouse=$WAREHOUSE 
    --createTable=true --project=$PROJECT"
```
### _Read_
```bash
./gradlew hadoop:execute -PmainClass=org.example.hadoop.ReadExample \
    -Pexec.args="--catalogName=$CATALOG_NAME --table=$TABLE --warehouse=$WAREHOUSE"
```

## Run BigLake examples with the following gradle commands

### _Write_
```bash
./gradlew biglake:execute -PmainClass=org.example.biglake.WriteToIcebergBigLake \
    -Pexec.args="--catalogName=$CATALOG_NAME --table=$TABLE 
    --warehouse=$WAREHOUSE --project=$PROJECT --region=$REGION"
```
### _Read_
```bash
./gradlew biglake:execute -PmainClass=org.example.biglake.ReadFromIcebergBigLake \
    -Pexec.args="--catalogName=$CATALOG_NAME --table=$TABLE 
    --warehouse=$WAREHOUSE --project=$PROJECT --region=$REGION"

```


**Note**: The Hadoop catalog is included by default and works out of the box with this example. However, it's primarily
for testing and may not be suitable for production. For production, consider other catalogs that offer more features and
scalability, but you'll need to install their respective JAR files