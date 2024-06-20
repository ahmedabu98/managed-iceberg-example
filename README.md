Managed Iceberg Example
-----------------------

This is a simple example reading from an **existing** Iceberg GCS table using Apache Beam's Managed IcebergIO.

Dependencies can be found in [`build.gradle`](https://github.com/ahmedabu98/managed-iceberg-example/blob/master/build.gradle). This file was used to also generate a [`pom.xml`](https://github.com/ahmedabu98/managed-iceberg-example/blob/master/pom.xml).

Run examples with the following gradle commands
```bash
./gradlew execute -PmainClass=org.example.WriteExample -Pexec.args="--table=$TABLE --warehouseLocation=$WAREHOUSE_LOCATION --createTable=true --gcpProject=$PROJECT"

./gradlew execute -PmainClass=org.example.ReadExample -Pexec.args="--table=$TABLE --warehouseLocation=$WAREHOUSE_LOCATION"
```

**Note**: The Hadoop catalog is included by default and works out of the box with this example. However, it's primarily
for testing and may not be suitable for production. For production, consider other catalogs that offer more features and
scalability, but you'll need to install their respective JAR files