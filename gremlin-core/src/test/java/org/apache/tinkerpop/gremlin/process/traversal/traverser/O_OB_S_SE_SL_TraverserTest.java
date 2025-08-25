package org.apache.tinkerpop.gremlin.process.traversal.traverser;

public class O_OB_S_SE_SL_TraverserTest extends SL_TraverserTest {

    @Override
    NL_SL_Traverser<String> createTraverser() {
        return new O_OB_S_SE_SL_Traverser<>(TEST, mockStep);
    }
}
