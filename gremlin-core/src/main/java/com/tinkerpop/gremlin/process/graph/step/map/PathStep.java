package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Path;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.marker.FunctionHolder;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import com.tinkerpop.gremlin.process.util.FunctionRing;
import com.tinkerpop.gremlin.process.util.MutablePath;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class PathStep<S> extends MapStep<S, Path> implements FunctionHolder<Object, Object> {

    private FunctionRing<Object, Object> functionRing;

    public PathStep(final Traversal traversal) {
        super(traversal);
        this.functionRing = new FunctionRing<>();
        PathStep.generateFunction(this);
    }

    @Override
    public PathStep<S> clone() throws CloneNotSupportedException {
        final PathStep<S> clone = (PathStep<S>) super.clone();
        clone.functionRing = this.functionRing.clone();
        PathStep.generateFunction(clone);
        return clone;
    }

    @Override
    public void reset() {
        super.reset();
        this.functionRing.reset();
    }

    @Override
    public void addFunction(final Function<Object, Object> function) {
        this.functionRing.addFunction(function);
    }

    @Override
    public List<Function<Object, Object>> getFunctions() {
        return this.functionRing.getFunctions();
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.functionRing);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.PATH);
    }

    /////////////////////////

    private static final <S> void generateFunction(final PathStep<S> pathStep) {
        pathStep.setFunction(traverser -> {
            final Path path;
            if (pathStep.functionRing.isEmpty())
                path = traverser.path();
            else {
                path = MutablePath.make();
                traverser.path().forEach((object,labels) -> path.extend(pathStep.functionRing.next().apply(object), labels.toArray(new String[labels.size()])));
            }
            pathStep.functionRing.reset();
            return path;
        });
    }
}
