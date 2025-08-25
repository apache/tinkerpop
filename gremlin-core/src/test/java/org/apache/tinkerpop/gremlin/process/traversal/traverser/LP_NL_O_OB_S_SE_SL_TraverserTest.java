package org.apache.tinkerpop.gremlin.process.traversal.traverser;

public class LP_NL_O_OB_S_SE_SL_TraverserTest extends NL_TraverserTest {

    @Override
    NL_SL_Traverser<String> createTraverser() {
        return new LP_NL_O_OB_S_SE_SL_Traverser<>(TEST, mockStep);
    }
}
