package com.oner.connectors.jet.influxdb;

import com.hazelcast.config.SerializerConfig;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.StreamStage;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static com.hazelcast.jet.Traversers.empty;
import static com.hazelcast.jet.Traversers.traverseIterable;
import static com.hazelcast.jet.pipeline.JournalInitialPosition.START_FROM_OLDEST;
import static org.influxdb.dto.QueryResult.Result;
import static org.influxdb.dto.QueryResult.Series;
import static org.junit.Assert.assertEquals;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class InfluxDBTests {

    private static final Properties INFUXDB_PROPS = new Properties();
    private static final String DB_NAME = "mydb";
    private static final String TABLE_NAME = "mytable";
    private final int recordCount = 100;

    @BeforeClass
    public static void setup() {
        INFUXDB_PROPS.setProperty("url", "http://localhost:8086");
        INFUXDB_PROPS.setProperty("username", "x");
        INFUXDB_PROPS.setProperty("password", "x");

        //Create test db
        InfluxDB influxDB = null;
        try {
            influxDB = InfluxDBFactory.connect(INFUXDB_PROPS.getProperty("url"));
            influxDB.query(new Query("CREATE DATABASE " + DB_NAME, "x", true));
        } finally {
            influxDB.close();
        }
    }

    @AfterClass
    public static void tearDown() {
        InfluxDB influxDB = null;
        try {
            influxDB = InfluxDBFactory.connect(INFUXDB_PROPS.getProperty("url"));
            influxDB.query(new Query("DROP DATABASE " + DB_NAME, "x", true));
        } finally {
            influxDB.close();
        }
    }

    @Test
    public void o1_insertInfluxDB() throws InterruptedException {
        try {
            JetConfig jetConfig = new JetConfig();
            jetConfig.getHazelcastConfig().getMapEventJournalConfig(TABLE_NAME).setEnabled(true).setCapacity(1000);
            JetInstance jet = Jet.newJetInstance(jetConfig);

            IntStream.range(0, recordCount)
                    .forEach(i -> jet.getMap(TABLE_NAME).put(i, System.nanoTime()));

            Pipeline p = Pipeline.create();
            StreamStage<Point> streamStage = p.drawFrom(Sources.<Integer, Long>mapJournal(TABLE_NAME, START_FROM_OLDEST))
                    .map(e -> Point.measurement(TABLE_NAME).time(e.getValue(), TimeUnit.NANOSECONDS).addField("tag", e.getKey()).build());
            streamStage.drainTo(Sinks.logger());
            streamStage.drainTo(InfluxDBSinks.influxdb(INFUXDB_PROPS, DB_NAME, "autogen"));

            Job job = jet.newJob(p);

            Thread.sleep(5_000);
            job.cancel();
        } finally {
            Jet.shutdownAll();
        }
    }

    @Test
    public void o2_queryInfluxDB() {
        try {
            JetConfig jetConfig = new JetConfig();
            jetConfig.getHazelcastConfig()
                    .getSerializationConfig()
                    .addSerializerConfig(new SerializerConfig().setClass(ResultSerializer.class).setTypeClass(Result.class))
                    .addSerializerConfig(new SerializerConfig().setClass(SeriesSerializer.class).setTypeClass(Series.class));

            JetInstance jet = Jet.newJetInstance(jetConfig);
            Pipeline p = Pipeline.create();

            BatchStage<List<Object>> influxdb = p.drawFrom(InfluxDBSources.influxdb(INFUXDB_PROPS, "SELECT * FROM " + TABLE_NAME, DB_NAME))
                    .flatMap(result -> result.getSeries() == null ? empty() : traverseIterable(result.getSeries()))
                    .flatMap(series -> traverseIterable(series.getValues()));

            influxdb.drainTo(Sinks.logger());
            influxdb.drainTo(Sinks.list("influxdb"));

            jet.newJob(p).join();
            assertEquals(jet.getList("influxdb").size(), recordCount);
        } finally {
            Jet.shutdownAll();
        }
    }

}

