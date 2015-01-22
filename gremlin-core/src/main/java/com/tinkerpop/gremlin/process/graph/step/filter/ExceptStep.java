package com.tinkerpop.gremlin.process.graph.step.filter;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class ExceptStep<S> extends FilterStep<S> implements Reversible {

    private final String sideEffectKeyOrPathLabel;

    public ExceptStep(final Traversal traversal, final String sideEffectKeyOrPathLabel) {
        super(traversal);
        this.sideEffectKeyOrPathLabel = sideEffectKeyOrPathLabel;
        this.setPredicate(traverser -> {
            final Object except = traverser.asAdmin().getSideEffects().exists(this.sideEffectKeyOrPathLabel) ?
                    traverser.sideEffects(this.sideEffectKeyOrPathLabel) :
                    traverser.path(this.sideEffectKeyOrPathLabel);
            return except instanceof Collection ?
                    !((Collection) except).contains(traverser.get()) :
                    !except.equals(traverser.get());
        });
    }

    public ExceptStep(final Traversal traversal, final Collection<S> exceptionCollection) {
        super(traversal);
        this.sideEffectKeyOrPathLabel = null;
        this.setPredicate(traverser -> !exceptionCollection.contains(traverser.get()));
    }

    public ExceptStep(final Traversal traversal, final S exceptionObject) {
        super(traversal);
        this.sideEffectKeyOrPathLabel = null;
        this.setPredicate(traverser -> !exceptionObject.equals(traverser.get()));
    }

    public String toString() {
        return TraversalHelper.makeStepString(this, this.sideEffectKeyOrPathLabel);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return null == this.sideEffectKeyOrPathLabel ?
                Collections.singleton(TraverserRequirement.OBJECT) :
                Stream.of(TraverserRequirement.OBJECT,
                        TraverserRequirement.SIDE_EFFECTS,
                        TraverserRequirement.PATH_ACCESS).collect(Collectors.toSet());
    }
}
