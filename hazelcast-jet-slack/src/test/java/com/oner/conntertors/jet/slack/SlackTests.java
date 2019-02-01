package com.oner.conntertors.jet.slack;

import com.hazelcast.com.eclipsesource.json.Json;
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
import com.oner.connectors.jet.slack.SlackSinks;
import com.oner.connectors.jet.slack.SlackSources;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.Properties;

public class SlackTests {

    @Test
    public void getSlackMessages() throws InterruptedException {
        JetInstance jet = Jet.newJetInstance();
        try {
            Pipeline p = Pipeline.create();

            Properties properties = new Properties();
            properties.setProperty("accessToken", "REPLACE_THIS");
            properties.setProperty("channelId", "REPLACE_THIS");

            StreamStage<String> slackSource = p.drawFrom(SlackSources.channel(properties));
            slackSource.drainTo(Sinks.logger());
            slackSource.map(msg -> Tuple2.tuple2(System.currentTimeMillis(), msg)).drainTo(Sinks.map("slackMessages"));

            Job job = jet.newJob(p);
            Thread.sleep(10_000);
            job.cancel();
            while (job.getStatus() != JobStatus.COMPLETED) ;
            Assert.assertTrue(jet.getMap("slackMessages").size() > 0);
        } finally {
            jet.shutdown();
        }
    }

    @Test
    public void sendSlackMessage() throws InterruptedException {
        JetInstance jet = Jet.newJetInstance();
        try {
            jet.getMap("slack").put(1, "merhaba dunyali");
            jet.getMap("slack").put(2, "biz dostuz");

            Pipeline p = Pipeline.create();

            Properties properties = new Properties();
            properties.setProperty("accessToken", "REPLACE_THIS");

            BatchStage<Map.Entry<Integer, String>> mapSource = p.drawFrom(Sources.map("slack"));

            mapSource.drainTo(Sinks.logger());
            mapSource.map(e -> {
                JsonObject msg = Json.object();
                return msg.set("text", e.getValue())
                        .set("channel", "REPLACE_THIS")
                        .set("username", "REPLACE_THIS")
                        .set("icon_url", "REPLACE_THIS")
                        .toString();
            }).drainTo(SlackSinks.channel(properties.getProperty("accessToken")));

            Job job = jet.newJob(p);
            Thread.sleep(10_000);
            job.cancel();
            while (job.getStatus() != JobStatus.COMPLETED) ;
            Assert.assertTrue(jet.getMap("slack").size() > 0);
        } finally {
            jet.shutdown();
        }
    }
}
