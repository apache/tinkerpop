package com.tinkerpop.gremlin.process.computer.traversal.step.util;

import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import com.tinkerpop.gremlin.process.computer.traversal.TraverserCountTracker;
import com.tinkerpop.gremlin.process.computer.traversal.TraverserPathTracker;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.util.StreamFactory;
import org.javatuples.Pair;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TraversalResultMapReduce implements MapReduce<Object, Object, Object, Object, Iterator<Object>> {

    public static final String TRAVERSERS = "traversers";

    public String getGlobalVariable() {
        return TRAVERSERS;
    }

    public boolean doReduce() {
        return false;
    }

    public void map(final Vertex vertex, final MapEmitter<Object, Object> emitter) {
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

    public Iterator<Object> getResult(final Iterator<Pair<Object, Object>> keyValues) {
        return StreamFactory.stream(keyValues).map(Pair::getValue1).iterator();
    }
}