package org.example.hive;

import com.google.common.collect.ImmutableMap;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.managed.Managed;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.example.utils.IcebergPipelineOptions;

public class ReadExample {
    public static void main(String[] args) {
        IcebergPipelineOptions options = PipelineOptionsFactory.fromArgs(args).as(IcebergPipelineOptions.class);
        Preconditions.checkState(!Strings.isNullOrEmpty(options.getMetastoreUri()),
                "Please specify a Hive Metastore URI using --metastoreUri=<uri>");

        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply(
                        Managed.read(Managed.ICEBERG).withConfig(ImmutableMap.<String, Object>builder()
                                .put("table", options.getTable())
                                .put("catalog_name", options.getCatalogName())
                                .put("config_properties", ImmutableMap.<String, Object>builder()
                                        .put("hive.metastore.uris", options.getMetastoreUri())
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