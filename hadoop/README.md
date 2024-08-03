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