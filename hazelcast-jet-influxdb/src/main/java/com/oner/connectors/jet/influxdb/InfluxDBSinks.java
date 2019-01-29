package com.oner.connectors.jet.influxdb;

import com.hazelcast.jet.datamodel.Tuple3;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.SinkBuilder;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;

import java.util.Properties;

public final class InfluxDBSinks {

    private InfluxDBSinks() {
    }

    public static Sink<Tuple3<String, String, Point>> influxdb(Properties properties) {
        return SinkBuilder.sinkBuilder("influx", (unused) ->
                InfluxDBFactory.connect(properties.getProperty("url"), properties.getProperty("username"), properties.getProperty("password")))
                .<Tuple3<String, String, Point>>receiveFn((influxDB, tuple3) -> influxDB.write(tuple3.f0(), tuple3.f1(), tuple3.f2()))
                .destroyFn(InfluxDB::close)
                .build();
    }

    public static Sink<Point> influxdb(Properties properties, String database, String retentionPolicy) {
        return SinkBuilder.sinkBuilder("influx", (unused) -> createConnection(properties, database, retentionPolicy))
                .<Point>receiveFn(InfluxDB::write)
                .destroyFn(InfluxDB::close)
                .build();
    }

    private static InfluxDB createConnection(Properties properties, String database, String retentionPolicy) {
        InfluxDB influxDB = InfluxDBFactory.connect(
                properties.getProperty("url"),
                properties.getProperty("username"),
                properties.getProperty("password"));
        if (database != null) {
            influxDB.setDatabase(database);
        }
        if (retentionPolicy != null) {
            influxDB.setRetentionPolicy(retentionPolicy);
        }
        return influxDB;
    }

}
