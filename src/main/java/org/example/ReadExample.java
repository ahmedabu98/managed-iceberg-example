package org.example;

import com.google.common.collect.ImmutableMap;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.managed.Managed;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.TypeDescriptors;

public class ReadExample {
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();

        pipeline.apply(
                Managed.read(Managed.ICEBERG).withConfig(ImmutableMap.<String, Object>builder()
                        .put("table", "testRead.table2ac46fb5")
                        .put("catalog_config", ImmutableMap.<String, Object>builder()
                                .put("catalog_name", "name")
                                .put("catalog_type", "hadoop")
                                .put("warehouse_location", "gs://temp-storage-for-end-to-end-tests/IcebergIOIT/testRead/0cc304b3-f27b-417b-b16a-29868880442b")
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