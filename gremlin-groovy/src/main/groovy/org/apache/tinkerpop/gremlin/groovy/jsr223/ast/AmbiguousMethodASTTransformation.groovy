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

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal
import org.codehaus.groovy.ast.ASTNode
import org.codehaus.groovy.ast.ClassNode
import org.codehaus.groovy.ast.CodeVisitorSupport
import org.codehaus.groovy.ast.expr.ArgumentListExpression
import org.codehaus.groovy.ast.expr.CastExpression
import org.codehaus.groovy.ast.expr.ConstantExpression
import org.codehaus.groovy.ast.expr.Expression
import org.codehaus.groovy.ast.expr.MethodCallExpression
import org.codehaus.groovy.control.CompilePhase
import org.codehaus.groovy.control.SourceUnit
import org.codehaus.groovy.transform.ASTTransformation
import org.codehaus.groovy.transform.GroovyASTTransformation

/**
 * Some calls to Gremlin steps with {@code null} can lead to ambiguous method exceptions where Groovy doesn't know
 * which method to choose without casting hints. Since the Gremlin in Gherkin tests is not meant to be Groovy specific
 * adding a cast is not desired. This transformation forces a choice for {@code null} when for these cases.
 * <p/>
 * This class is meant for internal use only at this time as it has incomplete coverage over from the gherkin tests.
 */
@GroovyASTTransformation(phase = CompilePhase.SEMANTIC_ANALYSIS)
class AmbiguousMethodASTTransformation implements ASTTransformation {

    @Override
    void visit(ASTNode[] nodes, SourceUnit source) {
        source.AST.statementBlock.statements[0].visit(new CodeVisitorSupport() {

            def currentMethod

            @Override
            void visitMethodCallExpression(MethodCallExpression call) {
                currentMethod = call.methodAsString.trim()
                call.getArguments().visit(this)
                call.getObjectExpression().visit(this)
            }

            @Override
            void visitArgumentlistExpression(ArgumentListExpression expression) {
                if (!expression.empty) {
                    expression.eachWithIndex{ Expression entry, int i ->
                        if (isNullExpression(entry)) {
                            if (currentMethod in [GraphTraversal.Symbols.mergeV,
                                                  GraphTraversal.Symbols.mergeE,
                                                  GraphTraversal.Symbols.option]) {
                                entry.type = new ClassNode(Map)
                                expression.expressions[i] = new CastExpression(new ClassNode(Map), entry)
                            }
                        }
                    }
                }
                super.visitArgumentlistExpression(expression)
            }

            private static boolean isNullExpression(Expression entry) {
                entry instanceof ConstantExpression && ((ConstantExpression) entry).nullExpression
            }
        })
    }
}
