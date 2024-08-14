package org.example.bigquery;

import com.google.common.collect.ImmutableMap;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.iceberg.IcebergUtils;
import org.apache.beam.sdk.managed.Managed;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.gcp.bigquery.BigQueryMetastoreCatalog;
import org.example.utils.IcebergPipelineOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.LongStream;


public class WriteExample {
    private static final Logger LOG = LoggerFactory.getLogger(WriteExample.class);

    // change `SCHEMA` and `ROW_FUNC` to fit your existing Iceberg table's schema
    private static final Schema SCHEMA =
            Schema.builder()
                    .addStringField("str")
                    .addBooleanField("bool")
                    .addNullableInt32Field("nullable_int")
                    .addNullableInt64Field("nullable_long")
                    .addArrayField("arr_long", Schema.FieldType.INT64)
                    .build();
    static SimpleFunction<Long, Row> ROW_FUNC = new SimpleFunction<Long, Row>() {
        @Override
        public Row apply(Long num) {
            String strNum = Long.toString(num);
            return Row.withSchema(SCHEMA)
                    .addValue("str_value_" + strNum)
                    .addValue(num % 2 == 0)
                    .addValue(Integer.valueOf(strNum))
                    .addValue(num)
                    .addValue(LongStream.range(0, num % 10).boxed().collect(Collectors.toList()))
                    .build();
        }
    };

    public static void main(String[] args) {
        IcebergPipelineOptions options = PipelineOptionsFactory.fromArgs(args).as(IcebergPipelineOptions.class);
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("gcp_project", options.getProject())
                .put("gcp_location", options.getRegion())
                .put("warehouse", options.getWarehouse())
                .put("allow_external_catalog_options_for_iceberg", "true")
                .put("catalog-impl", "org.apache.iceberg.gcp.bigquery.BigQueryMetastoreCatalog")
                .build();

        if (options.getCreateTable()) {
            createTable(options, properties);
        }

        Pipeline pipeline = Pipeline.create(options);

        pipeline
                .apply("Generate some longs",
                        GenerateSequence.from(0).to(100L))
                .apply("Convert longs to Beam Rows", MapElements.via(ROW_FUNC))
                .setRowSchema(SCHEMA)
                .apply("Write to Iceberg (BigQuery)", Managed.write(Managed.ICEBERG).withConfig(ImmutableMap.<String, Object>builder()
                        .put("table", options.getTable())
                        .put("catalog_name", options.getCatalogName())
                        .put("catalog_properties", properties)
                        .build()));

        pipeline.run();
    }

    public static void createTable(IcebergPipelineOptions options, Map<String, String> properties) {
        Configuration catalogConf = new Configuration();
        catalogConf.set("fs.gs.project.id", Preconditions.checkNotNull(options.getProject(),
                "To create the table, please provide your GCP project using --project"));
        catalogConf.set("fs.gs.auth.type", "APPLICATION_DEFAULT");

        BigQueryMetastoreCatalog catalog = new BigQueryMetastoreCatalog();

        catalog.setConf(catalogConf);
        catalog.initialize(options.getCatalogName(), properties);

        TableIdentifier table = TableIdentifier.parse(options.getTable());

        catalog.createTable(table, IcebergUtils.beamSchemaToIcebergSchema(SCHEMA));
        LOG.info("Successfully created table {}", table);
    }
}