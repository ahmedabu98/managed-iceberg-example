package org.example.hadoop;

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


public class WriteExample {
    // change `SCHEMA` and `ROW_FUNC` to fit your existing Iceberg table's schema
    private static final Schema SCHEMA = Schema.builder()
            .addStringField("str")
            .addInt64Field("number")
            .build();
    private static final SimpleFunction<Long, Row> ROW_FUNC = new SimpleFunction<Long, Row>() {
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

        Pipeline pipeline = Pipeline.create(options);

        pipeline
                .apply("Generate some longs",
                        GenerateSequence.from(0).to(100))
                .apply("Convert longs to Beam Rows", MapElements.via(ROW_FUNC))
                .setRowSchema(SCHEMA)
                .apply("Write to Iceberg (Hadoop)", Managed.write(Managed.ICEBERG).withConfig(ImmutableMap.<String, Object>builder()
                        .put("table", options.getTable())
                        .put("catalog_name", options.getCatalogName())
                        .put("catalog_properties", ImmutableMap.<String, Object>builder()
                                .put("catalog-impl", "org.apache.iceberg.hadoop.HadoopCatalog")
                                .put("warehouse", options.getWarehouse())
                                .build())
                        .build()));

        pipeline.run();
    }
}