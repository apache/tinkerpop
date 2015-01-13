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
public class B_O_P_PA_S_SE_SL_TraverserGenerator implements TraverserGenerator {

    private static final B_O_P_PA_S_SE_SL_TraverserGenerator INSTANCE = new B_O_P_PA_S_SE_SL_TraverserGenerator();
    private static final Set<TraverserRequirement> REQUIREMENTS = new HashSet<>(Arrays.asList(
            TraverserRequirement.OBJECT,
            TraverserRequirement.BULK,
            TraverserRequirement.SINGLE_LOOP,
            TraverserRequirement.PATH_ACCESS,
            TraverserRequirement.PATH,
            TraverserRequirement.SACK,
            TraverserRequirement.SIDE_EFFECTS));

    private B_O_P_PA_S_SE_SL_TraverserGenerator() {
    }

    @Override
    public <S> Traverser.Admin<S> generate(final S start, final Step<S, ?> startStep, final long initialBulk) {
        final B_O_P_PA_S_SE_SL_Traverser<S> traverser = new B_O_P_PA_S_SE_SL_Traverser<>(start, startStep);
        traverser.setBulk(initialBulk);
        return traverser;
    }

    @Override
    public Set<TraverserRequirement> requirements() {
        return REQUIREMENTS;
    }

    public static B_O_P_PA_S_SE_SL_TraverserGenerator instance() {
        return INSTANCE;
    }
}