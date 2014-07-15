package com.tinkerpop.gremlin.groovy.util

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class GremlinHelper {

    /**
     * Converts the body of a closure to a String.  It will not always be an exact replica of the code originally
     * provided, but it should be fully functional such that an evaluation of the same code should produce the
     * same output.  It is important to remember that for this function to work, the groovy code for which the
     * body is being extracted needs to be on the path.
     */
    public static String getClosureBody(final Closure closure) {
        def node = closure.getMetaClass().getClassNode().getDeclaredMethods("doCall")[0].getCode();
        def writer = new StringWriter()
        node.visit(new AstNodeToGremlinScriptVisitor(writer))
        return writer.toString()
    }
}
