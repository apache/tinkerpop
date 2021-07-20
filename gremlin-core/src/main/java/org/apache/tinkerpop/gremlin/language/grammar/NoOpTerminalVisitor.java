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
package org.apache.tinkerpop.gremlin.language.grammar;

import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

/**
 * {@inheritDoc}
 *
 * The same as parent visitor {@link GremlinAntlrToJava} but returns {@link Bytecode} instead of a {@link Traversal}
 * or {@link GraphTraversalSource}, and uses an overridden terminal step visitor.
 */
public class NoOpTerminalVisitor extends GremlinAntlrToJava {

    public NoOpTerminalVisitor() throws Exception {
        super();
    }

    /**
     * Returns {@link Bytecode} of {@link Traversal} or {@link GraphTraversalSource}, overriding any terminal step
     * operations to prevent them from being executed using the {@link TraversalTerminalMethodVisitor} to append
     * terminal operations to bytecode.
     *
     * @param ctx - the parse tree
     * @return - bytecode from the traversal or traversal source
     */
    @Override
    public Object visitQuery(final GremlinParser.QueryContext ctx){
        final int childCount = ctx.getChildCount();
        if (childCount <= 3) {
            final ParseTree firstChild = ctx.getChild(0);
            if (firstChild instanceof GremlinParser.TraversalSourceContext) {
                if (childCount == 1) {
                    // handle traversalSource
                    return gvisitor.visitTraversalSource((GremlinParser.TraversalSourceContext)firstChild).getBytecode();
                } else {
                    // handle traversalSource DOT transactionPart
                    throw new GremlinParserException("Transaction operation is not supported yet");
                }
            } else if (firstChild instanceof GremlinParser.EmptyQueryContext) {
                // handle empty query
                return "";
            } else {
                if (childCount == 1) {
                    // handle rootTraversal
                    return tvisitor.visitRootTraversal(
                            (GremlinParser.RootTraversalContext)firstChild).asAdmin().getBytecode();
                } else {
                    // handle rootTraversal DOT traversalTerminalMethod
                    // could not keep all of these methods in one visitor due to the need of the terminal visitor to have a traversal,
                    return new TerminalMethodToBytecodeVisitor(tvisitor
                            .visitRootTraversal((GremlinParser.RootTraversalContext)firstChild))
                            .visitTraversalTerminalMethod((GremlinParser.TraversalTerminalMethodContext)ctx.getChild(2));
                }
            }
        } else {
            // not clear what valid Gremlin, if any, will trigger this at the moment.
            throw new GremlinParserException("Unexpected parse tree for NoOpTerminalVisitor");
        }
    }
}

