package com.crowdstrike.dr.replay.iceberg;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.hadoop.HadoopCatalog;

/**
 * Creates Iceberg catalog (Hadoop catalog with local or HDFS warehouse).
 */
public final class IcebergCatalogFactory {

    private IcebergCatalogFactory() {}

    /**
     * Create a Hadoop catalog with the given warehouse path.
     * Path can be file:///path or hdfs://host/path.
     */
    public static Catalog createHadoopCatalog(String warehousePath) {
        Configuration conf = new Configuration();
        return new HadoopCatalog(conf, warehousePath);
    }
}
