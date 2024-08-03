## Run BigQuery Metastore examples with the following gradle commands

### _Write_

```bash
./gradlew bigquery:execute -PmainClass=org.example.bigquery.WriteExample \
    -Pexec.args="--catalogName=$CATALOG_NAME --table=$TABLE --warehouse=$WAREHOUSE
    --createTable=true --project=$PROJECT --region=$REGION"
```
### _Read_
```bash
./gradlew bigquery:execute -PmainClass=org.example.bigquery.ReadExample \
    -Pexec.args="--catalogName=$CATALOG_NAME --table=$TABLE 
    --warehouse=$WAREHOUSE --project=$PROJECT --region=$REGION"
```
**Note**: BigQueryMetastoreCatalog is a custom catalog provided by Google. The jar is included here for convenience,
but it can be downloaded from this GCS bucket: `gs://spark-lib/biglake/iceberg-bigquery-catalog-iceberg1.5.2-0.1.0-with-dependencies.jar`
