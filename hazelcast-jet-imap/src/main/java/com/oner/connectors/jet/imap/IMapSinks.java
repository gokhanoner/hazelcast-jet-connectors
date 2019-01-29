package com.oner.connectors.jet.imap;

import com.hazelcast.jet.datamodel.Tuple3;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.SinkBuilder;

import java.util.Map;

import static com.hazelcast.jet.core.Processor.Context;

public class IMapSinks {

    private IMapSinks() {
    }

    public static <K, V> Sink<Tuple3<String, K, V>> dynamicMap() {
        return SinkBuilder.sinkBuilder("dynamic-map", Context::jetInstance)
                .<Tuple3<String, K, V>>receiveFn((jetInstance, data) -> jetInstance.getMap(data.f0()).put(data.f1(), data.f2()))
                .build();
    }

    public static <K, V> Sink<Map.Entry<String, Map<K, V>>> dynamicMapB() {
        return SinkBuilder.sinkBuilder("dynamic-map", Context::jetInstance)
                .<Map.Entry<String, Map<K, V>>>receiveFn((jetInstance, data) -> jetInstance.getMap(data.getKey()).putAll(data.getValue()))
                .build();
    }
}
