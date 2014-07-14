package com.tinkerpop.gremlin.groovy.util

import groovy.inspect.swingui.AstNodeToScriptVisitor

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class GremlinHelper {

    public static String getClosureString(final Closure closure) {
        def node = closure.getMetaClass().getClassNode().getDeclaredMethods("doCall")[0].getCode();
        def writer = new StringWriter()
        node.visit(new AstNodeToScriptVisitor(writer))
        return writer.toString().substring(8)
    }
}
