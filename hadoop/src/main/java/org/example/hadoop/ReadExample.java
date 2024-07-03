package org.example.hadoop;

import com.google.common.collect.ImmutableMap;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.managed.Managed;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.example.utils.IcebergPipelineOptions;

public class ReadExample {
    public static void main(String[] args) {
        IcebergPipelineOptions options = PipelineOptionsFactory.fromArgs(args).as(IcebergPipelineOptions.class);
        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply(
                Managed.read(Managed.ICEBERG).withConfig(ImmutableMap.<String, Object>builder()
                        .put("table", options.getTable())
                        .put("catalog_name", options.getCatalogName())
                        .put("catalog_properties", ImmutableMap.<String, Object>builder()
                                .put("catalog-impl", "org.apache.iceberg.hadoop.HadoopCatalog")
                                .put("warehouse", options.getWarehouse())
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