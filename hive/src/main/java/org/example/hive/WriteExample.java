package org.example.hive;

import com.google.common.collect.ImmutableMap;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.managed.Managed;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.example.utils.IcebergPipelineOptions;
import org.example.hive.metastoreutils.HiveMetastore;

import static org.example.utils.Values.BEAM_SCHEMA;
import static org.example.utils.Values.ROW_FUNC;

public class WriteExample {
    public static void main(String[] args) throws Exception {
        IcebergPipelineOptions options = PipelineOptionsFactory.fromArgs(args).as(IcebergPipelineOptions.class);
        HiveMetastore hiveMetastore = new HiveMetastore(options.getWarehouse(), options.getMetastoreUri());

        // Currently, IcebergIO connector doesn't support automatically creating the table
        // If required, create it manually here
        if (options.getCreateTable()) {
            hiveMetastore.createTable(options.getCatalogName(), options.getTable());
        }

        Pipeline pipeline = Pipeline.create(options);

        pipeline
                .apply("Generate some longs",
                        GenerateSequence.from(0).to(100))
                .apply("Convert longs to Beam Rows", MapElements.via(ROW_FUNC))
                .setRowSchema(BEAM_SCHEMA)
                .apply("Write to Iceberg (Hive)", Managed.write(Managed.ICEBERG).withConfig(ImmutableMap.<String, Object>builder()
                        .put("table", options.getTable())
                        .put("catalog_name", options.getCatalogName())
                        .put("config_properties", ImmutableMap.<String, Object>builder()
                                .put("hive.metastore.uris", hiveMetastore.hiveMetastoreUri)
                                .build())
                        .build()));

        pipeline.run().waitUntilFinish();

        System.out.println("\nxxxxxxxxxxx\n" +
                "\nWrite has completed." +
                "\nTable: " + options.getTable() +
                "\nWarehouse: " + hiveMetastore.hiveWarehousePath +
                "\nMetastore URI: " + hiveMetastore.hiveMetastoreUri +
                "\n\nxxxxxxxxxxx\n");

        long stallTime = options.getStallSeconds();
        if (stallTime >= 0) {
            Thread.sleep(options.getStallSeconds() * 1000);
            cleanup(hiveMetastore);
        }
    }

    private static void cleanup(HiveMetastore hiveMetastore) {
        try {
            hiveMetastore.close();
        } catch (Exception e) {
            throw new RuntimeException("Error while closing hive metastore", e);
        }
    }
}