package com.oner.connectors.jet.gitter;

import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.SinkBuilder;
import com.oner.connectors.util.SimpleHttpClient;

public class GitterSinks {

    private static final String URL = "https://api.gitter.im/v1/rooms/%s/chatMessages";

    public static Sink<String> room(String roomId, String accessToken) {
        return SinkBuilder.sinkBuilder("gitter", context ->
                SimpleHttpClient
                        .create(String.format(URL, roomId)))
                .receiveFn(((SimpleHttpClient httpClient, String message) ->
                        httpClient
                                .withHeader("Authorization", String.format("Bearer %s", accessToken))
                                .withParam("text", message)
                                .postWithParams()))
                .build();
    }

}
