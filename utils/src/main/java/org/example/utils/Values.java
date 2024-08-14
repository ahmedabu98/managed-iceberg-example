package org.example.utils;

import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.apache.beam.sdk.io.iceberg.IcebergUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.iceberg.data.Record;

public class Values {
    private static final Schema DOUBLY_NESTED_ROW_SCHEMA =
            Schema.builder()
                    .addStringField("doubly_nested_str")
                    .addInt64Field("doubly_nested_float")
                    .build();

    private static final Schema NESTED_ROW_SCHEMA =
            Schema.builder()
                    .addStringField("nested_str")
                    .addInt32Field("nested_int")
                    .addFloatField("nested_float")
                    .addRowField("nested_row", DOUBLY_NESTED_ROW_SCHEMA)
                    .build();
    public static final Schema BEAM_SCHEMA =
            Schema.builder()
                    .addStringField("str")
                    .addBooleanField("bool")
                    .addNullableInt32Field("nullable_int")
                    .addNullableInt64Field("nullable_long")
                    .addArrayField("arr_long", Schema.FieldType.INT64)
                    .addRowField("row", NESTED_ROW_SCHEMA)
                    .addNullableRowField("nullable_row", NESTED_ROW_SCHEMA)
                    .build();

    public static final SimpleFunction<Long, Row> ROW_FUNC =
            new SimpleFunction<Long, Row>() {
                @Override
                public Row apply(Long num) {
                    String strNum = Long.toString(num);
                    Row nestedRow =
                            Row.withSchema(NESTED_ROW_SCHEMA)
                                    .addValue("nested_str_value_" + strNum)
                                    .addValue(Integer.valueOf(strNum))
                                    .addValue(Float.valueOf(strNum + "." + strNum))
                                    .addValue(
                                            Row.withSchema(DOUBLY_NESTED_ROW_SCHEMA)
                                                    .addValue("doubly_nested_str_value_" + strNum)
                                                    .addValue(num)
                                                    .build())
                                    .build();

                    return Row.withSchema(BEAM_SCHEMA)
                            .addValue("str_value_" + strNum)
                            .addValue(num % 2 == 0)
                            .addValue(Integer.valueOf(strNum))
                            .addValue(num)
                            .addValue(LongStream.range(1, num % 10).boxed().collect(Collectors.toList()))
                            .addValue(nestedRow)
                            .addValue(num % 2 == 0 ? null : nestedRow)
                            .build();
                }
            };

    public static final org.apache.iceberg.Schema ICEBERG_SCHEMA =
            IcebergUtils.beamSchemaToIcebergSchema(BEAM_SCHEMA);
    public static final SimpleFunction<Row, Record> RECORD_FUNC =
            new SimpleFunction<Row, Record>() {
                @Override
                public Record apply(Row input) {
                    return IcebergUtils.beamRowToIcebergRecord(ICEBERG_SCHEMA, input);
                }
            };
}
