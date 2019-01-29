package com.oner.connectors.jet.imap;

import com.hazelcast.jet.IMapJet;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.datamodel.Tuple3;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.util.ExceptionUtil;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Stream;

import static com.hazelcast.jet.aggregate.AggregateOperations.toMap;
import static org.junit.Assert.assertEquals;

public class IMapConnectorTests {

    private static final String DATA_FOLDER = "src/test/data";

    @Test
    public void putSingleTest() throws IOException {
        try {
            JetInstance jet = Jet.newJetInstance();
            Pipeline p = Pipeline.create();

            p.drawFrom(Sources.filesBuilder(DATA_FOLDER).build((file, line) -> {
                String[] data = line.split(",");
                return Tuple3.tuple3(file, data[0], data[1]);
            })).drainTo(IMapSinks.dynamicMap());

            jet.newJob(p).join();

            checkResults(jet, DATA_FOLDER);

        } finally {
            Jet.shutdownAll();
        }
    }

    @Test
    public void putBatchTest() throws IOException {
        try {
            JetInstance jet = Jet.newJetInstance();
            Pipeline p = Pipeline.create();

            p.drawFrom(Sources.filesBuilder("src/test/data").build((file, line) -> {
                        String[] data = line.split(",");
                        return Tuple3.tuple3(file, data[0], data[1]);
                    })
            ).groupingKey(Tuple3::f0).aggregate(toMap(Tuple3::f1, Tuple3::f2)).drainTo(IMapSinks.dynamicMapB());

            jet.newJob(p).join();

            checkResults(jet, DATA_FOLDER);
        } finally {
            Jet.shutdownAll();
        }
    }

    private void checkResults(JetInstance jet, String folder) throws IOException {
        try (Stream<Path> files = Files.list(Paths.get(folder))) {
            files.filter(path -> path.toFile().isFile())
                    .forEach(file -> {
                        try {
                            List<String> lines = Files.readAllLines(file);
                            IMapJet<Object, Object> map = jet.getMap(file.getFileName().toString());
                            assertEquals(lines.size(), map.size());
                            lines.stream().map(l -> l.split(",")).forEach(a -> assertEquals(a[1], map.get(a[0])));
                        } catch (IOException e) {
                            ExceptionUtil.sneakyThrow(e);
                        }
                    });
        }

    }
}
