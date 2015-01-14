package com.tinkerpop.gremlin.hadoop.process.computer.giraph;

import com.tinkerpop.gremlin.hadoop.structure.io.ObjectWritable;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.computer.MessageScope;
import com.tinkerpop.gremlin.process.computer.Messenger;
import com.tinkerpop.gremlin.process.graph.step.map.VertexStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.StartStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.apache.hadoop.io.LongWritable;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphMessenger<M> implements Messenger<M> {

    private GiraphComputeVertex giraphComputeVertex;
    private Iterable<ObjectWritable<M>> messages;

    public void setCurrentVertex(final GiraphComputeVertex giraphComputeVertex, final Iterable<ObjectWritable<M>> messages) {
        this.giraphComputeVertex = giraphComputeVertex;
        this.messages = messages;
    }

    @Override
    public Iterable<M> receiveMessages(final MessageScope messageScope) {
        return IteratorUtils.map(this.messages, ObjectWritable::get);
    }

    @Override
    public void sendMessage(final MessageScope messageScope, final M message) {
        if (messageScope instanceof MessageScope.Local) {
            final MessageScope.Local<M> localMessageScope = (MessageScope.Local) messageScope;
            final Traversal<Vertex, Edge> incidentTraversal = GiraphMessenger.setVertexStart(localMessageScope.getIncidentTraversal().get(), this.giraphComputeVertex.getBaseVertex());
            final Direction direction = GiraphMessenger.getOppositeDirection(incidentTraversal);
            incidentTraversal.forEachRemaining(edge ->
                    this.giraphComputeVertex.sendMessage(
                            new LongWritable(Long.valueOf(edge.iterators().vertexIterator(direction).next().id().toString())),
                            new ObjectWritable<>(localMessageScope.getEdgeFunction().apply(message, edge))));
        } else {
            final MessageScope.Global globalMessageScope = (MessageScope.Global) messageScope;
            globalMessageScope.vertices().forEach(vertex ->
                    this.giraphComputeVertex.sendMessage(new LongWritable(Long.valueOf(vertex.id().toString())), new ObjectWritable<>(message)));
        }
    }

    private static <T extends Traversal<Vertex, Edge>> T setVertexStart(final Traversal<Vertex, Edge> incidentTraversal, final Vertex vertex) {
        incidentTraversal.asAdmin().addStep(0, new StartStep<>(incidentTraversal, vertex));
        return (T) incidentTraversal;
    }

    private static Direction getOppositeDirection(final Traversal<Vertex, Edge> incidentTraversal) {
        final VertexStep step = TraversalHelper.getLastStep(incidentTraversal, VertexStep.class).get();
        return step.getDirection().opposite();
    }
}
