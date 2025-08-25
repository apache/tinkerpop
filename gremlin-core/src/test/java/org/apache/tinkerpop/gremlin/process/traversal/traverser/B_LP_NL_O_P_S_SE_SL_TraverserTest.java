package org.apache.tinkerpop.gremlin.process.traversal.traverser;

public class B_LP_NL_O_P_S_SE_SL_TraverserTest extends NL_TraverserTest {

    @Override
    B_LP_NL_O_P_S_SE_SL_Traverser<String> createTraverser() {
        return new B_LP_NL_O_P_S_SE_SL_Traverser<>(TEST, mockStep, BULK);
    }
}