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
downloaded using [these instructions](https://cloud.google.com/bigquery/docs/iceberg-tables#before_you_begin).
