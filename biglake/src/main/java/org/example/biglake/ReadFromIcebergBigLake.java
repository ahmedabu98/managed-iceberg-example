package org.example.biglake;

import com.google.common.collect.ImmutableMap;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.managed.Managed;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.example.utils.IcebergPipelineOptions;

import java.util.Map;

public class ReadFromIcebergBigLake {
    public static void main(String[] args) {
        IcebergPipelineOptions options = PipelineOptionsFactory.fromArgs(args).as(IcebergPipelineOptions.class);
        Pipeline pipeline = Pipeline.create(options);

        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("gcp_project", options.getProject())
                .put("gcp_location", options.getRegion())
                .put("warehouse", options.getWarehouse())
                .put("catalog-impl", "org.apache.iceberg.gcp.biglake.BigLakeCatalog")
                .build();

        pipeline
                .apply(Managed.read(Managed.ICEBERG).withConfig(ImmutableMap.<String, Object>builder()
                        .put("table", options.getTable())
                        .put("catalog_name", options.getCatalogName())
                        .put("catalog_properties", properties)
                        .build()))
                .getSinglePCollection()
                .apply(MapElements.into(TypeDescriptors.voids()).via(r -> {
                    System.out.println("row: " + r);

                    return null;
                }));

        pipeline.run();
    }
}