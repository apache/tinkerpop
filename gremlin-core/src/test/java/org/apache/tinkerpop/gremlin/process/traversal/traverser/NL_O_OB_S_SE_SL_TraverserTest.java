package org.apache.tinkerpop.gremlin.process.traversal.traverser;

public class NL_O_OB_S_SE_SL_TraverserTest extends NL_TraverserTest {

    @Override
    NL_SL_Traverser<String> createTraverser() {
        return new NL_O_OB_S_SE_SL_Traverser<>(TEST, mockStep);
    }
}
