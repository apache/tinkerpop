package com.tinkerpop.gremlin.process.computer.bulk;

import com.tinkerpop.gremlin.process.computer.KeyValue;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.io.graphson.GraphSONWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SaveGraphMapReduce implements MapReduce<MapReduce.NullObject, String, MapReduce.NullObject, String, Iterator<String>> {

    private static final String SIDE_EFFECT_KEY = "gremlin.bulkdump.se";

    private static final GraphSONWriter GRAPHSON_WRITER = GraphSONWriter.build().create();

    @Override
    public void map(final Vertex vertex, final MapEmitter<NullObject, String> emitter) {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            GRAPHSON_WRITER.writeVertex(baos, vertex, Direction.BOTH);
        } catch (final IOException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
        emitter.emit(baos.toString());
    }

    @Override
    public boolean doStage(final MapReduce.Stage stage) {
        return stage.equals(Stage.MAP);
    }

    @Override
    public String getMemoryKey() {
        return SIDE_EFFECT_KEY;
    }

    @Override
    public Iterator<String> generateFinalResult(final Iterator<KeyValue<NullObject, String>> iterator) {
        return new Iterator<String>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public String next() {
                return iterator.next().getValue();
            }
        };
    }
}

