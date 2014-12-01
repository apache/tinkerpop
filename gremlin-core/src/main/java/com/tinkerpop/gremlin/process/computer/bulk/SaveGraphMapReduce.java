package com.tinkerpop.gremlin.process.computer.bulk;

import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.structure.Vertex;
import org.javatuples.Pair;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SaveGraphMapReduce implements MapReduce<MapReduce.NullObject, String, MapReduce.NullObject, String, Iterator<String>> {

    public static final String SIDE_EFFECT_KEY = "gremlin.saveGraphMapReduce.serializedVertices";

    @Override
    public void map(final Vertex vertex, final MapEmitter<NullObject, String> emitter) {
        emitter.emit(vertex.value(SaveGraphVertexProgram.VERTEX_SERIALIZATION));
    }

    @Override
    public boolean doStage(final Stage stage) {
        return stage.equals(Stage.MAP);
    }

    @Override
    public Iterator<String> generateFinalResult(final Iterator<Pair<NullObject, String>> keyValues) {
        return new Iterator<String>() {
            @Override
            public boolean hasNext() {
                return keyValues.hasNext();
            }

            @Override
            public String next() {
                return keyValues.next().getValue1();
            }
        };
    }

    @Override
    public String getMemoryKey() {
        return SIDE_EFFECT_KEY;
    }
}
