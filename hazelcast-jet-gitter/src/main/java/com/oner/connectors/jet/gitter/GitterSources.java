package com.oner.connectors.jet.gitter;

import com.hazelcast.com.eclipsesource.json.Json;
import com.hazelcast.com.eclipsesource.json.JsonArray;
import com.hazelcast.com.eclipsesource.json.JsonValue;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.StreamSource;
import com.oner.connectors.util.SimpleHttpClient;

import java.util.Properties;

public final class GitterSources {

    private GitterSources() {
    }

    public static StreamSource<String> room(Properties prop) {
        return SourceBuilder.stream("gitter", context -> new GitterRestHelper(prop))
                .fillBufferFn(GitterRestHelper::fillBuffer)
                .build();
    }

    private static class GitterRestHelper {
        private static final String URL = "https://api.gitter.im/v1/rooms/%s/chatMessages";
        private static final String POLL_INTERVAL = "15000";

        private SimpleHttpClient httpClient;
        private long lastPolled;
        private long pollIntervalMillis;
        private String afterId;

        GitterRestHelper(Properties properties) {
            String roomId = properties.getProperty("roomId");
            String accessToken = properties.getProperty("accessToken");

            if (isMissing(accessToken) || isMissing(roomId)) {
                throw new IllegalArgumentException("Gitter credentials are missing!");
            }

            afterId = properties.getProperty("afterId", "");

            pollIntervalMillis = Long.parseLong(properties.getProperty("pollInterval", POLL_INTERVAL));
            pollIntervalMillis = Math.max(pollIntervalMillis, 1_000);

            httpClient = SimpleHttpClient.create(String.format(URL, roomId))
                    .withHeader("Authorization", String.format("Bearer %s", accessToken));
        }

        public static boolean isMissing(String test) {
            return test.isEmpty() || "REPLACE_THIS".equals(test);
        }

        void fillBuffer(SourceBuilder.SourceBuffer<String> buf) {
            if (!readyToPoll()) {
                return;
            }

            String response = httpClient
                    .withParam("afterId", afterId)
                    .get();

            JsonArray messages = Json.parse(response).asArray();

            for (JsonValue message : messages.values()) {
                afterId = message.asObject().getString("id", "");
                buf.add(message.toString());
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
