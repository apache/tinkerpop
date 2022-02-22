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

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

/**
 * Handles transactions via calls to {@code tx()}.
 */
public class TraversalSourceTxVisitor extends DefaultGremlinBaseVisitor<Void> {
    private GraphTraversalSource source;
    private final GremlinAntlrToJava antlr;

    public TraversalSourceTxVisitor(final GraphTraversalSource source, final GremlinAntlrToJava antlr) {
        this.source = source;
        this.antlr = antlr;
    }

    @Override
    public Void visitTransactionPart(final GremlinParser.TransactionPartContext ctx) {
        // position 4 holds the tx command
        final String cmd = ctx.getChild(4).getText();
        switch (cmd) {
            case "begin":
                // script based transactions are automatic, meaning commit()/rollback() should keep g in a state
                // where the transaction is opened on the next script execution. begin() is a more necessary syntax in
                // remote client-side Gremlin where you need to construct a boundary for the session which hosts the
                // transaction. we support begin() here to at least allow the function to be called without error so
                // as to hold syntax consistency, but it is effectively a no-op.
                // this.source.tx().begin();
                break;
            case "commit":
                this.source.tx().commit();
                break;
            case "rollback":
                this.source.tx().rollback();
                break;
            default:
                notImplemented(ctx);
                break;
        }

        return null;
    }
}
