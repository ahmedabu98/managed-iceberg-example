package org.example;

import com.google.common.collect.ImmutableMap;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.managed.Managed;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.TypeDescriptors;

public class ReadExample {
    public static void main(String[] args) {
        IcebergPipelineOptions options = PipelineOptionsFactory.fromArgs(args).as(IcebergPipelineOptions.class);
        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply(
                Managed.read(Managed.ICEBERG).withConfig(ImmutableMap.<String, Object>builder()
                        .put("table", options.getTable())
                        .put("catalog_config", ImmutableMap.<String, Object>builder()
                                .put("catalog_name", options.getCatalogName())
                                .put("catalog_type", options.getCatalogType())
                                .put("warehouse_location", options.getWarehouseLocation())
                                .build())
                        .build()))
                .getSinglePCollection()
                .apply(MapElements.into(TypeDescriptors.voids()).via(r -> {
                    System.out.println("row: " + r);

                    return null;
                }));

        pipeline.run();
    }
}