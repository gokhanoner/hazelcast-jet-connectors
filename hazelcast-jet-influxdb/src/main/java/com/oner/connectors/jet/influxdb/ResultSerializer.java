package com.oner.connectors.jet.influxdb;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;
import org.influxdb.dto.QueryResult;

import java.io.IOException;

public final class ResultSerializer implements StreamSerializer<QueryResult.Result> {
    @Override
    public void write(ObjectDataOutput out, QueryResult.Result object) throws IOException {
        out.writeUTF(object.getError());
        out.writeObject(object.getSeries());
    }

    @Override
    public QueryResult.Result read(ObjectDataInput in) throws IOException {
        QueryResult.Result result = new QueryResult.Result();
        result.setError(in.readUTF());
        result.setSeries(in.readObject());
        return result;
    }

    @Override
    public int getTypeId() {
        return 3434344;
    }

    @Override
    public void destroy() {

    }
}
