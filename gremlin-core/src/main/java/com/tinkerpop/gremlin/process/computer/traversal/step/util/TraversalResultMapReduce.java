package com.tinkerpop.gremlin.process.computer.traversal.step.util;

import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import com.tinkerpop.gremlin.process.computer.traversal.TraverserCountTracker;
import com.tinkerpop.gremlin.process.computer.traversal.TraverserPathTracker;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import org.javatuples.Pair;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TraversalResultMapReduce implements MapReduce<MapReduce.NullObject, Object, MapReduce.NullObject, Object, Iterator<Object>> {

    public static final String TRAVERSERS = "gremlin.traversers";

    public String getSideEffectKey() {
        return TRAVERSERS;
    }

    public boolean doStage(final Stage stage) {
        return stage.equals(Stage.MAP);
    }

    public void map(final Vertex vertex, final MapEmitter<MapReduce.NullObject, Object> emitter) {
        final Property mapProperty = vertex.property(TraversalVertexProgram.TRAVERSER_TRACKER);
        if (mapProperty.isPresent()) {
            if (mapProperty.value() instanceof TraverserCountTracker) {
                TraverserCountTracker tracker = (TraverserCountTracker) mapProperty.value();
                tracker.getDoneObjectTracks().forEach((traverser, count) -> {
                    for (int i = 0; i < count; i++) {
                        emitter.emit(NullObject.get(), traverser);
                    }
                });
                tracker.getDoneGraphTracks().forEach((traverser, count) -> {
                    for (int i = 0; i < count; i++) {
                        emitter.emit(NullObject.get(), traverser);
                    }
                });
            } else {
                TraverserPathTracker tracker = (TraverserPathTracker) mapProperty.value();
                tracker.getDoneObjectTracks().forEach((object, traversers) -> {
                    for (Traverser traverser : traversers) {
                        emitter.emit(NullObject.get(), traverser);
                    }
                });
                tracker.getDoneGraphTracks().forEach((object, traversers) -> {
                    for (Traverser traverser : traversers) {
                        emitter.emit(NullObject.get(), traverser);
                    }
                });
            }
        }

    }

    public Iterator<Object> generateSideEffect(final Iterator<Pair<MapReduce.NullObject, Object>> keyValues) {
        return new Iterator<Object>() {
            public boolean hasNext() {
                return keyValues.hasNext();
            }

            public Object next() {
                return keyValues.next().getValue1();
            }
        };
    }
}