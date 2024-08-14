package org.example.biglake;

import com.google.common.collect.ImmutableMap;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.managed.Managed;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Types;
import org.example.utils.IcebergPipelineOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;


public class WriteToIcebergBigLake {
    private static final Logger LOG = LoggerFactory.getLogger(WriteToIcebergBigLake.class);

    // change `SCHEMA` and `ROW_FUNC` to fit the existing Iceberg BigLake table's schema
    static Schema SCHEMA = Schema.builder()
            .addStringField("str")
            .addInt64Field("number")
            .build();
    static SimpleFunction<Long, Row> ROW_FUNC = new SimpleFunction<Long, Row>() {
        @Override
        public Row apply(Long input) {
            return Row.withSchema(SCHEMA)
                    .addValue("record_" + input)
                    .addValue(input)
                    .build();
        }
    };

    public static void main(String[] args) {
        IcebergPipelineOptions options = PipelineOptionsFactory.fromArgs(args).as(IcebergPipelineOptions.class);

        if (options.getCreateTable()) {
            LOG.info("BigLakeCatalog doesn't support creating tables. Will instead attempt to create a table " +
                    "using HadoopCatalog.");
            createTable(options);
        }

        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("gcp_project", options.getProject())
                .put("gcp_location", options.getRegion())
                .put("warehouse", options.getWarehouse())
                .put("catalog-impl", "org.apache.iceberg.gcp.biglake.BigLakeCatalog")
                .build();

        Pipeline pipeline = Pipeline.create(options);

        pipeline
                .apply("Generate some longs",
                        GenerateSequence.from(0).to(100L))
                .apply("Convert longs to Beam Rows", MapElements.via(ROW_FUNC))
                .setRowSchema(SCHEMA)
                .apply("Write to Iceberg (Biglake)", Managed.write(Managed.ICEBERG).withConfig(ImmutableMap.<String, Object>builder()
                        .put("table", options.getTable())
                        .put("catalog_name", options.getCatalogName())
                        .put("catalog_properties", properties)
                        .build()));

        pipeline.run();
    }

    public static void createTable(IcebergPipelineOptions options) {
        Configuration catalogConf = new Configuration();
        catalogConf.set("fs.gs.project.id", Preconditions.checkNotNull(options.getProject(),
                "To create the table, please provide your GCP project using --project"));
        catalogConf.set(
                "fs.gs.auth.service.account.json.keyfile", System.getenv("GOOGLE_APPLICATION_CREDENTIALS"));
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("warehouse", options.getWarehouse())
                .build();

        HadoopCatalog catalog;
        catalog = new HadoopCatalog();
        catalog.setConf(catalogConf);
        catalog.initialize(options.getCatalogName(), properties);

        TableIdentifier table = TableIdentifier.parse(options.getTable());

        catalog.createTable(table,
                new org.apache.iceberg.Schema(
                        Types.NestedField.required(1, "str", Types.StringType.get()),
                        Types.NestedField.required(2, "number", Types.LongType.get())));
        LOG.info("Successfully created table {}", table);
    }
}