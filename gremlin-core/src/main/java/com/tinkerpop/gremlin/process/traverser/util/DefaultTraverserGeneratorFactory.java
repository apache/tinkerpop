package com.tinkerpop.gremlin.process.traverser.util;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraverserGenerator;
import com.tinkerpop.gremlin.process.traverser.B_O_PA_S_SE_SL_TraverserGenerator;
import com.tinkerpop.gremlin.process.traverser.B_O_P_PA_S_SE_SL_TraverserGenerator;
import com.tinkerpop.gremlin.process.traverser.B_O_TraverserGenerator;
import com.tinkerpop.gremlin.process.traverser.O_TraverserGenerator;
import com.tinkerpop.gremlin.process.traverser.TraverserGeneratorFactory;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;

import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DefaultTraverserGeneratorFactory implements TraverserGeneratorFactory {

    private static DefaultTraverserGeneratorFactory INSTANCE = new DefaultTraverserGeneratorFactory();

    public static DefaultTraverserGeneratorFactory instance() {
        return INSTANCE;
    }

    private DefaultTraverserGeneratorFactory() {
    }

    @Override
    public TraverserGenerator getTraverserGenerator(final Traversal traversal) {
        final Set<TraverserRequirement> requirements = this.getRequirements(traversal);

        if (O_TraverserGenerator.instance().requirements().containsAll(requirements))
            return O_TraverserGenerator.instance();

        if (B_O_TraverserGenerator.instance().requirements().containsAll(requirements))
            return B_O_TraverserGenerator.instance();

        if (B_O_PA_S_SE_SL_TraverserGenerator.instance().requirements().containsAll(requirements))
            return B_O_PA_S_SE_SL_TraverserGenerator.instance();

        if (B_O_P_PA_S_SE_SL_TraverserGenerator.instance().requirements().containsAll(requirements))
            return B_O_P_PA_S_SE_SL_TraverserGenerator.instance();

        throw new IllegalStateException("The provided traverser generator factory does not support the requirements of the traversal: " + this.getClass().getCanonicalName() + requirements);
    }
}
