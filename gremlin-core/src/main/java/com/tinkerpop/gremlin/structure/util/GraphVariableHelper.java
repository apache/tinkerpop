package com.tinkerpop.gremlin.structure.util;

import com.tinkerpop.gremlin.structure.Graph;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GraphVariableHelper {

    public static void validateVariable(final String variable, final Object value) throws IllegalArgumentException {
        if (null == value)
            throw Graph.Variables.Exceptions.variableValueCanNotBeNull();
        if (null == variable)
            throw Graph.Variables.Exceptions.variableKeyCanNotBeNull();
        if (variable.isEmpty())
            throw Graph.Variables.Exceptions.variableKeyCanNotBeEmpty();
    }
}
