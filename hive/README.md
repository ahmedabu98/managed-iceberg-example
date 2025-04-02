## Run Iceberg Hive examples with the following gradle commands

### _Write_

Writes to an Iceberg table using HiveCatalog. If you have a Hive Metastore running, 
you can specify it with `--metastoreUri=$METASTORE_URI`. If nothing  is specified, this example will spin
up a local metastore for you. The actual data is written to a directory specified by `--warehouse`.

**Note:** after the pipeline finishes running, the metastore will be shut down, so you will
end up losing the stored metadata. If you'd like to keep the metastore open for some time while you
do further experimenting (e.g. reading from the metastore), you can optionally pass in a value for `--stallSeconds`.
A value of `-1` will make it stall indefinitely. 

You can find the URI for the created metastore in the logs. 

```bash
./gradlew hive:execute -PmainClass=org.example.hive.WriteExample \
    -Pexec.args="--catalogName=$CATALOG_NAME --table=$TABLE --warehouse=$WAREHOUSE
    --project=$PROJECT --region=$REGION --metastoreUri=$METASTORE_URI
    --stallSeconds=$STALL_SECONDS"
```
### _Read_

This example requires a running Hive metastore. You can use the metastore created by the write example, or
pass in your own metastore URI. Either way, the metastore URI must be specified with `--metastoreUri=$METASTORE_URI`.

```bash
./gradlew hive:execute -PmainClass=org.example.hive.ReadExample \
    -Pexec.args="--catalogName=$CATALOG_NAME --table=$TABLE 
    --warehouse=$WAREHOUSE --project=$PROJECT --region=$REGION --metastoreUri=$METASTORE_URI"
```
