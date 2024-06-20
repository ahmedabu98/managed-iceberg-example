package org.example;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

public interface IcebergPipelineOptions extends PipelineOptions {
    @Description("Warehouse location where the table resides. Example: gs://my-bucket/iceberg-dir")
    @Validation.Required
    String getWarehouseLocation();

    void setWarehouseLocation(String warehouseLocation);

    @Description("Iceberg table identifier, including namespace. Example: my_namespace.my_table")
    @Validation.Required
    String getTable();

    void setTable(String table);

    @Description("GCP project ID. Used to potentially create an Iceberg table in GCS.")
    String getGcpProject();

    void setGcpProject(String createTable);

    @Default.String("example-catalog")
    String getCatalogName();

    void setCatalogName(String catalogName);

    @Description("Catalog type. Valid types are: {hadoop, hive, rest}")
    @Default.String("hadoop")
    String getCatalogType();

    void setCatalogType(String catalogType);

    @Description("If true, will create the Iceberg table before running the pipeline.")
    @Default.Boolean(false)
    Boolean getCreateTable();

    void setCreateTable(Boolean createTable);
}
