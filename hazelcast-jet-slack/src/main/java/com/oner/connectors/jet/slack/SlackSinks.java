package com.oner.connectors.jet.slack;

import com.hazelcast.com.eclipsesource.json.JsonObject;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.SinkBuilder;
import com.oner.connectors.util.SimpleHttpClient;

public final class SlackSinks {

    private static final String URL = "https://slack.com/api/chat.postMessage";

    private SlackSinks() {
    }

    public static Sink<String> channel(String channel, String accessToken) {
        return SinkBuilder.sinkBuilder("slack", context ->
                SimpleHttpClient
                        .create(URL))
                .<String>receiveFn(((httpClient, message) ->
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
                .<JsonObject>receiveFn(((httpClient, message) ->
                        httpClient
                                .withBody(message.toString())
                                .postWithBody()))
                .build();
    }

}
