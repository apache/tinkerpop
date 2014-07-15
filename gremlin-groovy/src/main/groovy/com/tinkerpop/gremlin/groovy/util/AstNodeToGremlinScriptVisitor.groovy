package com.tinkerpop.gremlin.groovy.util

import groovy.inspect.swingui.AstNodeToScriptVisitor
import org.codehaus.groovy.ast.ClassNode
import org.codehaus.groovy.ast.GenericsType
import org.codehaus.groovy.ast.expr.ArgumentListExpression
import org.codehaus.groovy.ast.expr.DeclarationExpression
import org.codehaus.groovy.ast.stmt.ReturnStatement

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class AstNodeToGremlinScriptVisitor extends AstNodeToScriptVisitor {
    def AstNodeToGremlinScriptVisitor(Writer writer, boolean showScriptFreeForm = true, boolean showScriptClass = true) {
        super(writer, showScriptFreeForm, showScriptClass)
    }

    @Override
    void visitReturnStatement(ReturnStatement statement) {
        // printLineBreak()
        // print 'return '
        statement.getExpression().visit(this)
        printLineBreak()
    }
}
