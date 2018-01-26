/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.tinkerpop.gremlin.sparql;

import java.util.List;

import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.expr.E_Equals;
import org.apache.jena.sparql.expr.E_Exists;
import org.apache.jena.sparql.expr.E_GreaterThan;
import org.apache.jena.sparql.expr.E_GreaterThanOrEqual;
import org.apache.jena.sparql.expr.E_LessThan;
import org.apache.jena.sparql.expr.E_LessThanOrEqual;
import org.apache.jena.sparql.expr.E_LogicalAnd;
import org.apache.jena.sparql.expr.E_LogicalOr;
import org.apache.jena.sparql.expr.E_NotEquals;
import org.apache.jena.sparql.expr.E_NotExists;
import org.apache.jena.sparql.expr.Expr;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;

/**
 * Converts SPARQL "where" expressions to Gremlin predicates.
 */
class WhereTraversalBuilder {

    /**
     * Converts a general {@code Expr} to an anonymous {@link GraphTraversal}.
     */
    static GraphTraversal<?, ?> transform(final Expr expression) {
        if (expression instanceof E_Equals) return transform((E_Equals) expression);
        if (expression instanceof E_NotEquals) return transform((E_NotEquals) expression);
        if (expression instanceof E_LessThan) return transform((E_LessThan) expression);
        if (expression instanceof E_LessThanOrEqual) return transform((E_LessThanOrEqual) expression);
        if (expression instanceof E_GreaterThan) return transform((E_GreaterThan) expression);
        if (expression instanceof E_GreaterThanOrEqual) return transform((E_GreaterThanOrEqual) expression);
        if (expression instanceof E_LogicalAnd) return transform((E_LogicalAnd) expression);
        if (expression instanceof E_LogicalOr) return transform((E_LogicalOr) expression);
        if (expression instanceof E_Exists) return transform((E_Exists) expression);
        if (expression instanceof E_NotExists) return transform((E_NotExists) expression);
        throw new IllegalStateException(String.format("Unhandled expression: %s", expression));
    }

    private static GraphTraversal<?, ?> transform(final E_Equals expression) {
        final Object value = expression.getArg2().getConstant().getNode().getLiteralValue();
        return __.as(expression.getArg1().getVarName()).is(P.eq(value));
    }

    private static GraphTraversal<?, ?> transform(final E_NotEquals expression) {
        final Object value = expression.getArg2().getConstant().getNode().getLiteralValue();
        return __.as(expression.getArg1().getVarName()).is(P.neq(value));
    }

    private static GraphTraversal<?, ?> transform(final E_LessThan expression) {
    	final Object value = expression.getArg2().getConstant().getNode().getLiteralValue();
        return __.as(expression.getArg1().getVarName()).is(P.lt(value));
    }

    private static GraphTraversal<?, ?> transform(final E_LessThanOrEqual expression) {
        final Object value = expression.getArg2().getConstant().getNode().getLiteralValue();
        return __.as(expression.getArg1().getVarName()).is(P.lte(value));
    }

    private static GraphTraversal<?, ?> transform(final E_GreaterThan expression) {
        final Object value = expression.getArg2().getConstant().getNode().getLiteralValue();
        return __.as(expression.getArg1().getVarName()).is(P.gt(value));
    }

    private static GraphTraversal<?, ?> transform(final E_GreaterThanOrEqual expression) {
        final Object value = expression.getArg2().getConstant().getNode().getLiteralValue();
        return __.as(expression.getArg1().getVarName()).is(P.gte(value));
    }

    private static GraphTraversal<?, ?> transform(final E_LogicalAnd expression) {
        return __.and(
                transform(expression.getArg1()),
                transform(expression.getArg2()));
    }

    private static GraphTraversal<?, ?> transform(final E_LogicalOr expression) {
        return __.or(
                transform(expression.getArg1()),
                transform(expression.getArg2()));
    }

    private static GraphTraversal<?, ?> transform(final E_Exists expression) {
        final OpBGP opBGP = (OpBGP) expression.getGraphPattern();
        final List<Triple> triples = opBGP.getPattern().getList();
        if (triples.size() != 1) throw new IllegalStateException("Unhandled EXISTS pattern");
        final GraphTraversal<?, ?> traversal = TraversalBuilder.transform(triples.get(0));
        final Step endStep = traversal.asAdmin().getEndStep();
        final String label = (String) endStep.getLabels().iterator().next();
        endStep.removeLabel(label);
        return traversal;
    }
    

    private static GraphTraversal<?, ?> transform(final E_NotExists expression) {
        final OpBGP opBGP = (OpBGP) expression.getGraphPattern();
        final List<Triple> triples = opBGP.getPattern().getList();
        if (triples.size() != 1) throw new IllegalStateException("Unhandled NOT EXISTS pattern");
        final GraphTraversal<?, ?> traversal = TraversalBuilder.transform(triples.get(0));
        final Step endStep = traversal.asAdmin().getEndStep();
        final String label = (String) endStep.getLabels().iterator().next();
        endStep.removeLabel(label);
        return __.not(traversal);
    }
}
