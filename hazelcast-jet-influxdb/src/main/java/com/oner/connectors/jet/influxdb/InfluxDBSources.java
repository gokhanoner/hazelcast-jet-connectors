package com.oner.connectors.jet.influxdb;

import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.SourceBuilder;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;

import java.util.Properties;

public class InfluxDBSources {

    private InfluxDBSources() {
    }

    public static BatchSource<QueryResult.Result> influxdb(Properties properties, String query, String db) {
        return SourceBuilder.batch("influxdb", context ->
                InfluxDBFactory.connect(properties.getProperty("url"), properties.getProperty("username"), properties.getProperty("password")))
                .<QueryResult.Result>fillBufferFn((influxDB, buffer) -> {
                    QueryResult result = influxDB.query(new Query(query, db));
                    if (result.hasError()) {
                        throw new IllegalArgumentException("influxdb query failed: " + result.getError());
                    } else {
                        result.getResults().forEach(buffer::add);
                        buffer.close();
                    }
                })
                .destroyFn(InfluxDB::close)
                .build();
    }
}
