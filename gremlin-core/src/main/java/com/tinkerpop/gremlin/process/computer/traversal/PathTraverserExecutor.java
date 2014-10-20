package com.tinkerpop.gremlin.process.computer.traversal;

import com.tinkerpop.gremlin.process.Path;
import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.computer.MessageType;
import com.tinkerpop.gremlin.process.computer.Messenger;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.referenced.ReferencedFactory;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class PathTraverserExecutor extends TraverserExecutor {

    public static boolean execute(final Vertex vertex, final Messenger<Traverser.Admin<?>> messenger, final Traversal traversal) {
        final TraverserTracker tracker = vertex.value(TraversalVertexProgram.TRAVERSER_TRACKER);
        final AtomicBoolean voteToHalt = new AtomicBoolean(true);
        messenger.receiveMessages(MessageType.Global.to()).forEach(traverser -> {
            if (traverser.isDone()) {
                // no need to deflate as they are already deflated
                tracker.getDoneGraphTracks().add((Traverser.Admin) traverser);
            } else {
                traverser.attach(vertex);
                final Step<?, ?> step = TraversalHelper.getStep(traverser.getFuture(), traversal);
                step.addStart((Traverser) traverser);
                if (processStep(step, messenger, tracker))
                    voteToHalt.set(false);
            }
        });
        tracker.getPreviousObjectTracks().forEach(traverser -> {
            if (traverser.isDone()) {
                // no need to deflate as they are already deflated
                tracker.getDoneObjectTracks().add((Traverser.Admin) traverser);
            } else {
                traverser.attach(vertex);
                final Step<?, ?> step = TraversalHelper.getStep(traverser.getFuture(), traversal);
                step.addStart((Traverser) traverser);
                if (processStep(step, messenger, tracker))
                    voteToHalt.set(false);
            }
        });
        return voteToHalt.get();

    }

    private static boolean processStep(final Step<?, ?> step, final Messenger<Traverser.Admin<?>> messenger, final TraverserTracker tracker) {
        final boolean messageSent = step.hasNext();
        step.forEachRemaining(traverser -> {
            final Object end = traverser.get();
            traverser.asAdmin().detach();
            if (end instanceof Element || end instanceof Property)
                messenger.sendMessage(MessageType.Global.to(getHostingVertex(end)), traverser.asAdmin());
            else {
                if (end instanceof Path)
                    ((Traverser<Object>) traverser).asAdmin().set(ReferencedFactory.detach((Path) end));
                tracker.getObjectTracks().add((Traverser.Admin) traverser.asAdmin());
            }

        });
        return messageSent;
    }

}