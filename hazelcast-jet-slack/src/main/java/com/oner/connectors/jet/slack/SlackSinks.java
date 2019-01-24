package com.oner.connectors.jet.slack;

import com.hazelcast.com.eclipsesource.json.JsonObject;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.SinkBuilder;
import com.oner.connectors.util.SimpleHttpClient;

public class SlackSinks {

    private static final String URL = "https://slack.com/api/chat.postMessage";

    public static Sink<String> channel(String channel, String accessToken) {
        return SinkBuilder.sinkBuilder("slack", context ->
                SimpleHttpClient
                        .create(URL))
                .receiveFn(((SimpleHttpClient httpClient, String message) ->
                        httpClient
                                .withHeader("Authorization", String.format("Bearer %s", accessToken))
                                .withParam("channel", channel)
                                .withParam("text", message)
                                .postWithParams()))
                .build();
    }

    public static Sink<JsonObject> channel(String accessToken) {
        return SinkBuilder.sinkBuilder("slack", context ->
                SimpleHttpClient
                        .create(URL)
                        .withHeader("Authorization", String.format("Bearer %s", accessToken))
                        .withHeader("Content-Type", "application/json; charset=utf-8"))
                .receiveFn(((SimpleHttpClient httpClient, JsonObject message) ->
                        httpClient
                                .withBody(message.toString())
                                .postWithBody()))
                .build();
    }

}
