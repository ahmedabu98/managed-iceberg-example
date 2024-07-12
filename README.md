Managed Iceberg Examples
------------------------

These examples demonstrate how to use Dataflow's new [Managed](https://cloud.google.com/dataflow/docs/guides/managed-io)
IcebergIO to read and write data to Iceberg tables.

Included are examples for both Hadoop and BigLake catalogs. Dependencies can be found in their respective `build.gradle`
files. To run these examples, clone this repository and follow the instructions below.

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
**Note**: HadoopCatalog is included by default and works out of the box with this example. However, it's primarily
for testing and may not be suitable for production. Consider other catalogs that offer more features and potential for
scalability, but you'll need to install their respective JAR files

## Run BigLake examples with the following gradle commands

### _Write_
<details>
<summary>Need to create the warehouse and BigLake table first?</summary>

<h3>Use terraform scripts to create your warehouse and BigLake table.</h3>

**Step 1:** Modify the variables in [variables.tf](https://github.com/ahmedabu98/managed-iceberg-example/tree/master/biglake/create-biglake-table/variables.tf) to match your specifications.

**Step 2:** Prepare the terraform environment:
```bash
terraform -chdir=biglake/create-biglake-table init 
```
**Step 3:** Run the script:
```bash
terraform -chdir=biglake/create-biglake-table apply 
```

**Finally**, don't forget to add `--createTable=true` to the write command below. This step is necessary to create the
first metadata file.

_Note that BigLakeCatalog doesn't support creating a table so this operation is done using HadoopCatalog_
<br>
<br>
</details>

```bash
./gradlew biglake:execute -PmainClass=org.example.biglake.WriteToIcebergBigLake \
    -Pexec.args="--catalogName=$CATALOG_NAME --table=$TABLE --warehouse=$WAREHOUSE
    --createTable=true --project=$PROJECT --region=$REGION"
```
### _Read_
```bash
./gradlew biglake:execute -PmainClass=org.example.biglake.ReadFromIcebergBigLake \
    -Pexec.args="--catalogName=$CATALOG_NAME --table=$TABLE 
    --warehouse=$WAREHOUSE --project=$PROJECT --region=$REGION"

```
**Note**: BigLakeCatalog is a custom catalog provided by Google. The jar is included here for convenience, but it can be
downloaded using the [these instructions](https://cloud.google.com/bigquery/docs/iceberg-tables#before_you_begin).
