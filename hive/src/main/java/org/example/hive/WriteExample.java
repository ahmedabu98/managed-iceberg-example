package org.example.hive;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.managed.Managed;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.thrift.TException;
import org.example.utils.IcebergPipelineOptions;
import org.example.hive.metastore.HiveMetastore;

import java.util.concurrent.TimeUnit;

import static org.example.utils.Values.BEAM_SCHEMA;
import static org.example.utils.Values.ROW_FUNC;
import static org.example.utils.Values.ICEBERG_SCHEMA;

public class WriteExample {
    public static void main(String[] args) throws TException, InterruptedException {
        IcebergPipelineOptions options = PipelineOptionsFactory.fromArgs(args).as(IcebergPipelineOptions.class);
        HiveMetastore hiveMetastore = new HiveMetastore(options.getWarehouse(), options.getMetastoreUri());

        // Currently, IcebergIO connector doesn't support automatically creating the table
        // If required, create it manually here
        if (options.getCreateTable()) {
            createTable(hiveMetastore, options);
        }

        Pipeline pipeline = Pipeline.create(options);

        pipeline
                .apply("Generate some longs",
                        GenerateSequence.from(0).to(100))
                .apply("Convert longs to Beam Rows", MapElements.via(ROW_FUNC))
                .setRowSchema(BEAM_SCHEMA)
                .apply("Write to Iceberg (Hive)", Managed.write(Managed.ICEBERG).withConfig(ImmutableMap.<String, Object>builder()
                        .put("table", options.getTable())
                        .put("catalog_name", options.getCatalogName())
                        .put("config_properties", ImmutableMap.<String, Object>builder()
                                .put(HiveConf.ConfVars.METASTOREURIS.varname, hiveMetastore.hiveMetastoreUri)
                                .build())
                        .build()));

        pipeline.run().waitUntilFinish();

        System.out.println("\nxxxxxxxxxxx\n" +
                "\nWrite has completed." +
                "\nTable: " + options.getTable() +
                "\nWarehouse: " + hiveMetastore.hiveWarehousePath +
                "\nMetastore URI: " + hiveMetastore.hiveMetastoreUri +
                "\n\nxxxxxxxxxxx\n");

        long stallTime = options.getStallSeconds();
        if (stallTime >= 0) {
            Thread.sleep(options.getStallSeconds() * 1000);
            cleanup(hiveMetastore);
        }
    }

    private static void createTable(HiveMetastore hiveMetastore, IcebergPipelineOptions options) throws TException {
        TableIdentifier tableIdentifier = TableIdentifier.parse(options.getTable());

        String[] levels = tableIdentifier.namespace().levels();
        Preconditions.checkState(levels.length == 1, "Error when retrieving the table identifier's database. Expected 1 namespace level but found " + levels.length);
        String database = levels[0];
        String dbPath = hiveMetastore.getDatabasePath(levels[0]);
        Database db = new Database(database, "Managed Iceberg example", dbPath, Maps.newHashMap());
        try {
            hiveMetastore.metastoreClient().createDatabase(db);
            System.out.printf("Successfully created database: '%s', path: %s%n", database, dbPath);
        } catch (AlreadyExistsException e) {
            System.out.printf("Database '%s' already exists. Ignoring exception: %s\n", database, e);
        }

        HiveCatalog catalog = initializeCatalog(hiveMetastore, options);

        Table table = catalog.createTable(tableIdentifier, ICEBERG_SCHEMA);
        System.out.printf("Successfully created table '%s', path: %s\n", table.name(), table.location());
    }

    private static HiveCatalog initializeCatalog(HiveMetastore hiveMetastore, IcebergPipelineOptions options) {
        return (HiveCatalog) CatalogUtil.loadCatalog(HiveCatalog.class.getName(),
                options.getCatalogName(),
                ImmutableMap.of(
                        CatalogProperties.CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS,
                        String.valueOf(TimeUnit.SECONDS.toMillis(10))),
                hiveMetastore.hiveConf());
    }

    private static void cleanup(HiveMetastore hiveMetastore) {
        try {
            hiveMetastore.close();
        } catch (Exception e) {
            throw new RuntimeException("Error while closing hive metastore", e);
        }
    }
}