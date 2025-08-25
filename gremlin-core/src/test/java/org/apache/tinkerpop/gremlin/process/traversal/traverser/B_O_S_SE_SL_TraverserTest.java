package org.apache.tinkerpop.gremlin.process.traversal.traverser;

public class B_O_S_SE_SL_TraverserTest extends SL_TraverserTest {
    @Override
    B_O_S_SE_SL_Traverser<String> createTraverser() {
        return new B_O_S_SE_SL_Traverser<>(TEST, mockStep, BULK);
    }
}