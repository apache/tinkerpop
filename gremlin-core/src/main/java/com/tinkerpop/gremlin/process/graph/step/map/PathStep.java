package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Path;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.marker.TraversalHolder;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import com.tinkerpop.gremlin.process.util.path.MutablePath;
import com.tinkerpop.gremlin.process.util.traversal.TraversalHelper;
import com.tinkerpop.gremlin.process.util.traversal.TraversalRing;
import com.tinkerpop.gremlin.process.util.traversal.TraversalUtil;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class PathStep<S> extends MapStep<S, Path> implements TraversalHolder {

    private TraversalRing<Object, Object> traversalRing;

    public PathStep(final Traversal traversal) {
        super(traversal);
        this.traversalRing = new TraversalRing<>();
        PathStep.generateFunction(this);
    }

    @Override
    public PathStep<S> clone() throws CloneNotSupportedException {
        final PathStep<S> clone = (PathStep<S>) super.clone();
        clone.traversalRing = new TraversalRing<>();
        for (final Traversal.Admin<Object, Object> traversal : this.traversalRing.getTraversals()) {
            final Traversal.Admin<Object, Object> traversalClone = traversal.clone();
            clone.traversalRing.addTraversal(traversalClone);
            clone.executeTraversalOperations(traversalClone, TYPICAL_LOCAL_OPERATIONS);
        }
        PathStep.generateFunction(clone);
        return clone;
    }

    @Override
    public void reset() {
        super.reset();
        this.traversalRing.reset();
    }

    @Override
    public List<Traversal.Admin<Object, Object>> getLocalTraversals() {
        return this.traversalRing.getTraversals();
    }

    @Override
    public void addLocalTraversal(final Traversal.Admin<?, ?> pathTraversal) {
        this.traversalRing.addTraversal((Traversal.Admin<Object, Object>) pathTraversal);
        this.executeTraversalOperations(pathTraversal, TYPICAL_LOCAL_OPERATIONS);
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.traversalRing);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.PATH);
    }

    /////////////////////////

    private static final <S> void generateFunction(final PathStep<S> pathStep) {
        pathStep.setFunction(traverser -> {
            final Path path;
            if (pathStep.traversalRing.isEmpty())
                path = traverser.path();
            else {
                path = MutablePath.make();
                traverser.path().forEach((object, labels) -> path.extend(TraversalUtil.function(object, pathStep.traversalRing.next()), labels.toArray(new String[labels.size()])));
            }
            pathStep.traversalRing.reset();
            return path;
        });
    }
}
