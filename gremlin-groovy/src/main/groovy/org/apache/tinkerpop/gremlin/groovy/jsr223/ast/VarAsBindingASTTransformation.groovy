/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.tinkerpop.gremlin.groovy.jsr223.ast

import org.apache.tinkerpop.gremlin.process.traversal.Order
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.Translator
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode
import org.apache.tinkerpop.gremlin.process.traversal.Bindings
import org.codehaus.groovy.ast.ASTNode
import org.codehaus.groovy.ast.ClassNode
import org.codehaus.groovy.ast.CodeVisitorSupport
import org.codehaus.groovy.ast.expr.ArgumentListExpression
import org.codehaus.groovy.ast.expr.ClassExpression
import org.codehaus.groovy.ast.expr.ConstantExpression
import org.codehaus.groovy.ast.expr.DeclarationExpression
import org.codehaus.groovy.ast.expr.Expression
import org.codehaus.groovy.ast.expr.MethodCallExpression
import org.codehaus.groovy.ast.expr.PropertyExpression
import org.codehaus.groovy.ast.expr.StaticMethodCallExpression
import org.codehaus.groovy.ast.expr.TupleExpression
import org.codehaus.groovy.ast.expr.VariableExpression
import org.codehaus.groovy.ast.stmt.ExpressionStatement
import org.codehaus.groovy.control.CompilePhase
import org.codehaus.groovy.control.SourceUnit
import org.codehaus.groovy.syntax.Token
import org.codehaus.groovy.syntax.Types
import org.codehaus.groovy.transform.ASTTransformation
import org.codehaus.groovy.transform.GroovyASTTransformation

/**
 * Converts variables used in Gremlin to {@link Bindings} declarations. By doing this, the generated traversal ends up
 * having those {@link Bindings} in the Gremlin {@link Bytecode} which means that when passed through a
 * {@link Translator} to generate a script form, the variables are preserved.
 * <p/>
 * This AST Transformation is meant to work with a single lines of Gremlin and only Gremlin. Inclusion of Groovy code
 * might produce odd behavior as all variables found are basically treated as bindings which is not desirable for
 * scripts in general.
 */
@GroovyASTTransformation(phase = CompilePhase.SEMANTIC_ANALYSIS)
class VarAsBindingASTTransformation implements ASTTransformation {

    private def bindingVariableName = 'bInDiNg' + UUID.randomUUID().toString().replace('-', '')

    @Override
    void visit(ASTNode[] nodes, SourceUnit source) {
        // inject a binding declaration here using a unique variable name that should not interfere with variables
        // in the Gremlin itself - basically generates the following line right before the Gremlin statement:
        // bInDiNg<uuid> = Bindings.instance()
        source.AST.statementBlock.statements.add(0, new ExpressionStatement(
                new DeclarationExpression(
                        new VariableExpression(bindingVariableName, new ClassNode(Bindings)),
                        Token.newSymbol(Types.EQUAL, 0, 0),
                        new StaticMethodCallExpression(new ClassNode(Bindings), "instance", new TupleExpression())
                )
        ))

        // analyze the Gremlin and detect variables replacing them with:
        // bInDiNg<uuid>.of(<var>,<some-default>)
        source.AST.statementBlock.statements[1].visit(new CodeVisitorSupport() {

            def currentMethod

            @Override
            void visitMethodCallExpression(MethodCallExpression call) {
                currentMethod = call.methodAsString
                call.getArguments().visit(this)
                call.getObjectExpression().visit(this)
            }

            @Override
            void visitArgumentlistExpression(ArgumentListExpression expression) {
                if (!expression.empty) {
                    expression.eachWithIndex{ Expression entry, int i ->
                        if (entry instanceof VariableExpression) {
                            // need a default binding value - any nonsense that satisfies the step argument should
                            // work typically but this is getting hacky
                            def bindingValue = new ConstantExpression(UUID.randomUUID().toString())
                            switch (currentMethod) {
                                case GraphTraversal.Symbols.map:
                                case GraphTraversal.Symbols.filter:
                                case GraphTraversal.Symbols.flatMap:
                                case GraphTraversal.Symbols.sideEffect:
                                case GraphTraversal.Symbols.choose:
                                case GraphTraversal.Symbols.until:
                                case GraphTraversal.Symbols.branch:
                                    bindingValue = new StaticMethodCallExpression(new ClassNode(__), "identity", new TupleExpression())
                                    break
                                case GraphTraversal.Symbols.by:
                                    if (i == 1) bindingValue = new PropertyExpression(new ClassExpression(new ClassNode(Order)), "desc")
                                    break
                            }
                            def bindingExpression = createBindingFromVar(entry.text, bindingVariableName, bindingValue)
                            bindingExpression.sourcePosition = entry
                            bindingExpression.copyNodeMetaData(entry)
                            expression.expressions[i] = bindingExpression
                        }
                    }
                }
                super.visitArgumentlistExpression(expression)
            }
        })
    }

    private static def createBindingFromVar(String nameOfVar, String bindingVariableName, Expression bindingValue) {
        return new MethodCallExpression(
                        new VariableExpression(bindingVariableName),
                        new ConstantExpression("of"),
                        new ArgumentListExpression(
                                new ConstantExpression(nameOfVar),
                                bindingValue
                        )
                )
    }
}
