package com.tinkerpop.gremlin.process.traverser;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.TraverserGenerator;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class B_O_TraverserGenerator implements TraverserGenerator {

    private static final Set<TraverserRequirement> REQUIREMENTS = new HashSet<>(Arrays.asList(
            TraverserRequirement.OBJECT,
            TraverserRequirement.BULK));

    private static final B_O_TraverserGenerator INSTANCE = new B_O_TraverserGenerator();

    private B_O_TraverserGenerator() {
    }

    @Override
    public <S> Traverser.Admin<S> generate(final S start, final Step<S, ?> startStep, final long initialBulk) {
        return new B_O_Traverser<>(start, initialBulk);
    }

    @Override
    public Set<TraverserRequirement> requirements() {
        return REQUIREMENTS;
    }

    public static B_O_TraverserGenerator instance() {
        return INSTANCE;
    }
}