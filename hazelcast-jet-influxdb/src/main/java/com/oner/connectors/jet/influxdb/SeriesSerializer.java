package com.oner.connectors.jet.influxdb;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;
import org.influxdb.dto.QueryResult;

import java.io.IOException;

public final class SeriesSerializer implements StreamSerializer<QueryResult.Series> {
    @Override
    public void write(ObjectDataOutput out, QueryResult.Series object) throws IOException {
        out.writeUTF(object.getName());
        out.writeObject(object.getColumns());
        out.writeObject(object.getTags());
        out.writeObject(object.getValues());
    }

    @Override
    public QueryResult.Series read(ObjectDataInput in) throws IOException {
        QueryResult.Series series = new QueryResult.Series();
        series.setName(in.readUTF());
        series.setColumns(in.readObject());
        series.setTags(in.readObject());
        series.setValues(in.readObject());
        return series;
    }

    @Override
    public int getTypeId() {
        return 3434345;
    }

    @Override
    public void destroy() {

    }
}
