package org.example;

import com.google.common.collect.ImmutableMap;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.managed.Managed;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Types;

import java.util.Map;


public class WriteExample {
    static Schema inputSchema = Schema.builder()
            .addStringField("str")
            .addInt64Field("number")
            .build();

    public static void main(String[] args) {
        IcebergPipelineOptions options = PipelineOptionsFactory.fromArgs(args).as(IcebergPipelineOptions.class);

        // Currently, the IcebergIO connector doesn't support creating the table from scratch.
        // If required, create it manually here
        if (options.getCreateTable()) {
            Preconditions.checkNotNull(options.getGcpProject(),
                    "To create the table, please provide the GCP project using --gcpProject");
            createTable(options);
        }

        Pipeline pipeline = Pipeline.create(options);

        pipeline
                .apply("Generate some longs",
                        GenerateSequence.from(0).to(10))
                .apply("Convert longs to Beam Rows",
                        MapElements.into(TypeDescriptors.rows()).via(l ->
                                Row.withSchema(inputSchema).addValues("record_" + l, l).build()))
                .setRowSchema(inputSchema)
                .apply("Write to Iceberg", Managed.write(Managed.ICEBERG).withConfig(ImmutableMap.<String, Object>builder()
                        .put("table", options.getTable())
                        .put("catalog_config", ImmutableMap.<String, Object>builder()
                                .put("catalog_name", options.getCatalogName())
                                .put("catalog_type", options.getCatalogType())
                                .put("warehouse_location", options.getWarehouseLocation())
                                .build())
                        .build()));

        pipeline.run();
    }

    public static void createTable(IcebergPipelineOptions options) {
        Configuration catalogConf = new Configuration();
        catalogConf.set("fs.gs.project.id", options.getGcpProject());

        Catalog catalog = new HadoopCatalog(catalogConf, options.getWarehouseLocation());

        catalog.createTable(TableIdentifier.parse(options.getTable()),
                new org.apache.iceberg.Schema(
                        Types.NestedField.required(1, "str", Types.StringType.get()),
                        Types.NestedField.required(2, "number", Types.LongType.get())));
    }
}