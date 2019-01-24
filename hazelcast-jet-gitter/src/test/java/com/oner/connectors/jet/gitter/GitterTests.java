package com.oner.connectors.jet.gitter;

import com.hazelcast.com.eclipsesource.json.JsonObject;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.StreamStage;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.Properties;

public class GitterTests {

    @Test
    public void getGitterMessages() throws InterruptedException {
        JetInstance jet = Jet.newJetInstance();
        try {
            Pipeline p = Pipeline.create();

            Properties properties = new Properties();
            properties.setProperty("accessToken", "REPLACE_THIS");
            properties.setProperty("roomId", "REPLACE_THIS");

            StreamStage<Tuple2<JsonObject, String>> gitterSource = p.drawFrom(GitterSources.room(properties));

            gitterSource.drainTo(Sinks.logger());
            gitterSource.map(msg -> Tuple2.tuple2(msg.f0().get("id").asString(), msg.f0().toString())).drainTo(Sinks.map("gitter"));

            Job job = jet.newJob(p);
            Thread.sleep(10_000);
            job.cancel();
            while (job.getStatus() != JobStatus.COMPLETED) ;
            Assert.assertTrue(jet.getMap("gitter").size() > 0);
        } finally {
            jet.shutdown();
        }
    }

    @Test
    public void sendGitterMessage() throws InterruptedException {
        JetInstance jet = Jet.newJetInstance();
        try {
            jet.getMap("gitter").put(1, "merhaba dunyali");
            jet.getMap("gitter").put(2, "biz dostuz");

            Pipeline p = Pipeline.create();

            BatchStage<Map.Entry<Integer, String>> mapSource = p.drawFrom(Sources.map("gitter"));

            mapSource.drainTo(Sinks.logger());
            mapSource.map(Map.Entry::getValue).drainTo(GitterSinks.room("REPLACE_THIS", "REPLACE_THIS"));

            Job job = jet.newJob(p);
            Thread.sleep(10_000);
            job.cancel();
            while (job.getStatus() != JobStatus.COMPLETED) ;
            Assert.assertTrue(jet.getMap("gitter").size() > 0);
        } finally {
            jet.shutdown();
        }
    }
}
