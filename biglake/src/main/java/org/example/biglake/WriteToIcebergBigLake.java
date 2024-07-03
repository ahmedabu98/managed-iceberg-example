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
import org.example.utils.IcebergPipelineOptions;

import java.util.Map;


public class WriteToIcebergBigLake {
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
            throw new UnsupportedOperationException("BigLakeCatalog doesn't support creating tables.");
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
                .apply("Write to Iceberg BigLake", Managed.write(Managed.ICEBERG).withConfig(ImmutableMap.<String, Object>builder()
                        .put("table", options.getTable())
                        .put("catalog_name", options.getCatalogName())
                        .put("catalog_properties", properties)
                        .build()));

        pipeline.run();
    }
}