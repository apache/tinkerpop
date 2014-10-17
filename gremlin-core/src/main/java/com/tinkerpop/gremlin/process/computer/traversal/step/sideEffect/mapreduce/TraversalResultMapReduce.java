package com.tinkerpop.gremlin.process.computer.traversal.step.sideEffect.mapreduce;

import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import com.tinkerpop.gremlin.process.computer.traversal.TraverserCountTracker;
import com.tinkerpop.gremlin.process.computer.traversal.TraverserPathTracker;
import com.tinkerpop.gremlin.process.computer.util.GraphComputerHelper;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import org.javatuples.Pair;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class TraversalResultMapReduce implements MapReduce<MapReduce.NullObject, Object, MapReduce.NullObject, Object, Iterator<Object>> {

    public static final String TRAVERSERS = Graph.System.system("traversers");

    @Override
    public boolean doStage(final Stage stage) {
        return stage.equals(Stage.MAP);
    }

    @Override
    public void map(final Vertex vertex, final MapEmitter<MapReduce.NullObject, Object> emitter) {
        final Property mapProperty = vertex.property(TraversalVertexProgram.TRAVERSER_TRACKER);
        if (mapProperty.isPresent()) {
            if (mapProperty.value() instanceof TraverserCountTracker) {
                TraverserCountTracker tracker = (TraverserCountTracker) mapProperty.value();
                tracker.getDoneObjectTracks().forEach((traverser, count) -> {
                    traverser.setBulk(count); // TODO: Remove when updating TrackerCounter
                    emitter.emit(NullObject.instance(), traverser);
                });
                tracker.getDoneGraphTracks().forEach((traverser, count) -> {
                    traverser.setBulk(count); // TODO: Remove when updating TrackerCounter
                    emitter.emit(NullObject.instance(), traverser);
                });
            } else {
                TraverserPathTracker tracker = (TraverserPathTracker) mapProperty.value();
                tracker.getDoneObjectTracks().forEach((object, traversers) -> {
                    for (Traverser traverser : traversers) {
                        emitter.emit(NullObject.instance(), traverser);
                    }
                });
                tracker.getDoneGraphTracks().forEach((object, traversers) -> {
                    for (Traverser traverser : traversers) {
                        emitter.emit(NullObject.instance(), traverser);
                    }
                });
            }
        }

    }

    @Override
    public Iterator<Object> generateSideEffect(final Iterator<Pair<MapReduce.NullObject, Object>> keyValues) {
        return new Iterator<Object>() {
            @Override
            public boolean hasNext() {
                return keyValues.hasNext();
            }

            @Override
            public Object next() {
                return keyValues.next().getValue1();
            }
        };
    }

    @Override
    public String getSideEffectKey() {
        return TRAVERSERS;
    }

    @Override
    public int hashCode() {
        return (this.getClass().getCanonicalName() + TRAVERSERS).hashCode();
    }

    @Override
    public boolean equals(final Object object) {
        return GraphComputerHelper.areEqual(this, object);
    }

    @Override
    public String toString() {
        return StringFactory.mapReduceString(this, "");
    }
}