package com.tinkerpop.gremlin.process.traverser;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.TraverserGenerator;

import java.util.Collections;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class O_TraverserGenerator implements TraverserGenerator {

    private static final Set<TraverserRequirement> REQUIREMENTS = Collections.singleton(TraverserRequirement.OBJECT);
    private static final O_TraverserGenerator INSTANCE = new O_TraverserGenerator();

    private O_TraverserGenerator() {
    }

    @Override
    public <S> Traverser.Admin<S> generate(final S start, final Step<S, ?> startStep, final long initialBulk) {
        return new O_Traverser<>(start);
    }

    @Override
    public Set<TraverserRequirement> requirements() {
        return REQUIREMENTS;
    }

    public static O_TraverserGenerator instance() {
        return INSTANCE;
    }
}