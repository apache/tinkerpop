package com.tinkerpop.gremlin.process.traverser;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.TraverserGenerator;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.HashSet;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface TraverserGeneratorFactory {

    public TraverserGenerator getTraverserGenerator(final Traversal traversal);

    public default Set<TraverserRequirement> getRequirements(final Traversal<?, ?> traversal) {
        final Set<TraverserRequirement> requirements = new HashSet<>();
        traversal.asAdmin().getTraversalEngine().ifPresent(engine -> {
            if (engine.equals(TraversalEngine.COMPUTER))
                requirements.add(TraverserRequirement.BULK);
        });
        requirements.addAll(TraversalHelper.getRequirements(traversal));
        return requirements;
    }
}
