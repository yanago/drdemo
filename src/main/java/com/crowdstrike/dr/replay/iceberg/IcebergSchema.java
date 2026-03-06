package com.crowdstrike.dr.replay.iceberg;

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;

/**
 * Iceberg schema for security events table.
 */
public final class IcebergSchema {

    public static final Schema SECURITY_EVENTS = new Schema(
            Types.NestedField.required(1, "cid", Types.StringType.get()),
            Types.NestedField.required(2, "event_timestamp", Types.StringType.get()),
            Types.NestedField.required(3, "event_time", Types.LongType.get()),
            Types.NestedField.required(4, "event_type", Types.StringType.get()),
            Types.NestedField.required(5, "event_id", Types.StringType.get())
    );

    private IcebergSchema() {}
}
