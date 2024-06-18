Managed Iceberg Example

This is a simple example reading from an **existing** Iceberg GCS table using Apache Beam's Managed IcebergIO.

Dependencies can be found in [`build.gradle`](https://github.com/ahmedabu98/managed-iceberg-example/blob/master/build.gradle). This file was used to also generate a [`pom.xml`](https://github.com/ahmedabu98/managed-iceberg-example/blob/master/pom.xml).

Run with the following gradle command
```bash
./gradlew execute -PmainClass=org.example.ReadExample -Pexec.args="--table=$TABLE --warehouseLocation=$WAREHOUSE_LOCATION"
```
