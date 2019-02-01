package com.oner.connectors.jet.slack;

import com.hazelcast.com.eclipsesource.json.Json;
import com.hazelcast.com.eclipsesource.json.JsonArray;
import com.hazelcast.com.eclipsesource.json.JsonObject;
import com.hazelcast.com.eclipsesource.json.JsonValue;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.StreamSource;
import com.oner.connectors.util.SimpleHttpClient;

import java.util.ListIterator;
import java.util.Properties;

public final class SlackSources {

    private SlackSources() {
    }

    public static StreamSource<String> channel(Properties properties) {
        return SourceBuilder.stream("slack", context -> new SlackRestHelper(properties))
                .fillBufferFn(SlackRestHelper::fillBuffer)
                .build();
    }

    private static class SlackRestHelper {
        private static final String URL = "https://slack.com/api/channels.history";
        private static final String POLL_INTERVAL = "15000";

        private SimpleHttpClient httpClient;
        private String oldestTs;
        private long lastPolled;
        private long pollIntervalMillis;

        SlackRestHelper(Properties properties) {
            String channelId = properties.getProperty("channelId");
            String accessToken = properties.getProperty("accessToken");

            if (isMissing(accessToken) || isMissing(channelId)) {
                throw new IllegalArgumentException("Slack properties are missing!");
            }

            String inclusive = properties.getProperty("inclusive", "0");
            oldestTs = properties.getProperty("oldest", "0");

            pollIntervalMillis = Long.parseLong(properties.getProperty("pollInterval", POLL_INTERVAL));
            pollIntervalMillis = Math.max(pollIntervalMillis, 1_000);

            httpClient = SimpleHttpClient.create(URL)
                    .withHeader("Authorization", String.format("Bearer %s", accessToken))
                    .withParam("channel", channelId)
                    .withParam("inclusive", inclusive);
        }

        public static boolean isMissing(String test) {
            return test.isEmpty() || "REPLACE_THIS".equals(test);
        }

        void fillBuffer(SourceBuilder.SourceBuffer<String> buf) {
            if (!readyToPoll()) {
                return;
            }

            String response = httpClient
                    .withParam("oldest", oldestTs)
                    .postWithParams();

            JsonObject resp = Json.parse(response).asObject();
            JsonArray messages = resp.get("messages").asArray();

            //Slack gives you messages in the reverse order, new messages first
            if (messages.size() > 0) {
                ListIterator<JsonValue> iterator = messages.values().listIterator(messages.size());
                while (iterator.hasPrevious()) {
                    JsonObject message = iterator.previous().asObject();
                    oldestTs = message.get("ts").asString();
                    buf.add(message.toString());
                }
            }

        }

        private boolean readyToPoll() {
            long now = System.currentTimeMillis();
            if (now - lastPolled < pollIntervalMillis) {
                return false;
            }
            lastPolled = now;
            return true;
        }
    }
}
