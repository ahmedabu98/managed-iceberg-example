Managed Iceberg Examples
------------------------

These examples demonstrate how to use Dataflow's new [Managed](https://cloud.google.com/dataflow/docs/guides/managed-io)
IcebergIO to read and write data to Iceberg tables.

Included are examples for a few catalogs. Dependencies can be found in their respective `build.gradle`
files. To run these examples, clone this repository and follow the instructions in their respective README files.

**Note**: DirectRunner is used by default. Feel free to tag on `--runner=DataflowRunner` to run on Dataflow.

- [HiveCatalog](https://github.com/ahmedabu98/managed-iceberg-example/tree/master/hive)
- [HadoopCatalog](https://github.com/ahmedabu98/managed-iceberg-example/tree/master/hadoop)
- [BigLakeCatalog](https://github.com/ahmedabu98/managed-iceberg-example/tree/master/biglake)
- [BigQueryMetastoreCatalog](https://github.com/ahmedabu98/managed-iceberg-example/tree/master/bigquery)