package org.example.utils;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;

public interface IcebergPipelineOptions extends DataflowPipelineOptions {
    @Description("Warehouse location where the table resides. Example: gs://my-bucket/my-warehouse")
    @Validation.Required
    String getWarehouse();

    void setWarehouse(String warehouse);

    @Description("Iceberg table identifier, including namespace. Example: my_namespace.my_table")
    @Validation.Required
    String getTable();

    void setTable(String table);

    @Validation.Required
    String getCatalogName();

    void setCatalogName(String catalogName);

    @Description("If true, will attempt to create the Iceberg table before running the pipeline.")
    @Default.Boolean(false)
    Boolean getCreateTable();

    void setCreateTable(Boolean createTable);
}
