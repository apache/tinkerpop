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
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.TextP;

import java.util.Collection;
import java.util.List;
import java.util.Set;

public class TraversalPredicateVisitor extends GremlinBaseVisitor<P> {
    private static TraversalPredicateVisitor instance;

    public static TraversalPredicateVisitor instance() {
        if (instance == null) {
            instance = new TraversalPredicateVisitor();
        }
        return instance;
    }

    /**
     * @deprecated As of release 3.5.2, replaced by {@link #instance()}.
     */
    @Deprecated
    public static TraversalPredicateVisitor getInstance() {
        return instance();
    }

    private TraversalPredicateVisitor() {}

    /**
     * Cast ParseTree node child into TraversalPredicateContext
     * @param ctx : ParseTree node
     * @param childIndex : child index
     * @return casted TraversalPredicateContext
     */
    private GremlinParser.TraversalPredicateContext castChildToTraversalPredicate(final ParseTree ctx, final int childIndex) {
        return (GremlinParser.TraversalPredicateContext)(ctx.getChild(childIndex));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public P visitTraversalPredicate(final GremlinParser.TraversalPredicateContext ctx) {
        switch(ctx.getChildCount()) {
            case 1:
                // handle simple predicate
                return visitChildren(ctx);
            case 5:
                // handle negate
                return visit(ctx.getChild(0)).negate();
            case 6:
                final int childIndexOfParameterOperator = 2;
                final int childIndexOfCaller = 0;
                final int childIndexOfArgument = 4;

                if (ctx.getChild(childIndexOfParameterOperator).getText().equals("or")) {
                    // handle or
                    return visit(ctx.getChild(childIndexOfCaller)).or(visit(ctx.getChild(childIndexOfArgument)));
                } else {
                    // handle and
                    return visit(ctx.getChild(childIndexOfCaller)).and(visit(ctx.getChild(childIndexOfArgument)));
                }
            default:
                throw new RuntimeException("unexpected number of children in TraversalPredicateContext " + ctx.getChildCount());
        }
    }

    /**
     * get 1 generic literal argument from the antlr parse tree context,
     * where the arguments has the child index of 2
     */
    private Object getSingleGenericLiteralArgument(final ParseTree ctx) {
        final int childIndexOfParameterValue = 2;

        return GenericLiteralVisitor.instance().visitGenericLiteral(
                ParseTreeContextCastHelper.castChildToGenericLiteral(ctx, childIndexOfParameterValue));
    }

    /**
     * {@inheritDoc}
     */
    @Override public P visitTraversalPredicate_eq(final GremlinParser.TraversalPredicate_eqContext ctx) {
        return P.eq(getSingleGenericLiteralArgument(ctx));
    }

    /**
     * {@inheritDoc}
     */
    @Override public P visitTraversalPredicate_neq(final GremlinParser.TraversalPredicate_neqContext ctx) {
        return P.neq(getSingleGenericLiteralArgument(ctx));
    }

    /**
     * {@inheritDoc}
     */
    @Override public P visitTraversalPredicate_lt(final GremlinParser.TraversalPredicate_ltContext ctx) {
        return P.lt(getSingleGenericLiteralArgument(ctx));
    }

    /**
     * {@inheritDoc}
     */
    @Override public P visitTraversalPredicate_lte(final GremlinParser.TraversalPredicate_lteContext ctx) {
        return P.lte(getSingleGenericLiteralArgument(ctx));
    }

    /**
     * {@inheritDoc}
     */
    @Override public P visitTraversalPredicate_gt(final GremlinParser.TraversalPredicate_gtContext ctx) {
        return P.gt(getSingleGenericLiteralArgument(ctx));
    }

    /**
     * {@inheritDoc}
     */
    @Override public P visitTraversalPredicate_gte(final GremlinParser.TraversalPredicate_gteContext ctx) {
        return P.gte(getSingleGenericLiteralArgument(ctx));
    }

    /**
     * get 2 generic literal arguments from the antlr parse tree context,
     * where the arguments has the child index of 2 and 4
     */
    private Object[] getDoubleGenericLiteralArgument(ParseTree ctx) {
        final int childIndexOfParameterFirst = 2;
        final int childIndexOfParameterSecond = 4;

        final Object first = GenericLiteralVisitor.instance().visitGenericLiteral(
                ParseTreeContextCastHelper.castChildToGenericLiteral(ctx, childIndexOfParameterFirst));
        final Object second = GenericLiteralVisitor.instance().visitGenericLiteral(
                ParseTreeContextCastHelper.castChildToGenericLiteral(ctx, childIndexOfParameterSecond));

        return new Object[]{first, second};
    }

    /**
     * {@inheritDoc}
     */
    @Override public P visitTraversalPredicate_inside(final GremlinParser.TraversalPredicate_insideContext ctx) {
        final Object[] arguments = getDoubleGenericLiteralArgument(ctx);
        return P.inside(arguments[0], arguments[1]);
    }

    /**
     * {@inheritDoc}
     */
    @Override public P visitTraversalPredicate_outside(final GremlinParser.TraversalPredicate_outsideContext ctx) {
        final Object[] arguments = getDoubleGenericLiteralArgument(ctx);
        return P.outside(arguments[0], arguments[1]);
    }

    /**
     * {@inheritDoc}
     */
    @Override public P visitTraversalPredicate_between(final GremlinParser.TraversalPredicate_betweenContext ctx) {
        final Object[] arguments = getDoubleGenericLiteralArgument(ctx);
        return P.between(arguments[0], arguments[1]);
    }

    /**
     * {@inheritDoc}
     */
    @Override public P visitTraversalPredicate_within(final GremlinParser.TraversalPredicate_withinContext ctx) {
        final int childIndexOfParameterValues = 2;

        // called with no args which is valid for java/groovy
        if (ctx.getChildCount() == 3) return P.within();

        final Object arguments = GenericLiteralVisitor.instance().visitGenericLiteralList(
                ParseTreeContextCastHelper.castChildToGenericLiteralList(ctx, childIndexOfParameterValues));

        if (arguments instanceof Object[]) {
            // when generic literal list is consist of a comma separated generic literals
            return P.within((Object []) arguments);
        } else if (arguments instanceof List || arguments instanceof Set) {
            // when generic literal list is consist of a collection of generic literals, E.g. range
            return P.within((Collection) arguments);
        } else {
            // when generic literal list is consist of a single generic literal
            return P.within(arguments);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override public P visitTraversalPredicate_without(final GremlinParser.TraversalPredicate_withoutContext ctx) {
        final int childIndexOfParameterValues = 2;

        // called with no args which is valid for java/groovy
        if (ctx.getChildCount() == 3) return P.without();

        final Object arguments = GenericLiteralVisitor.instance().visitGenericLiteralList(
                ParseTreeContextCastHelper.castChildToGenericLiteralList(ctx, childIndexOfParameterValues));

        if (arguments instanceof Object[]) {
            // when generic literal list is consist of a comma separated generic literals
            return P.without((Object [])arguments);
        } else if (arguments instanceof List) {
            // when generic literal list is consist of a collection of generic literals, E.g. range
            return P.without((List)arguments);
        } else {
            // when generic literal list is consist of a single generic literal
            return P.without(arguments);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override public P visitTraversalPredicate_not(final GremlinParser.TraversalPredicate_notContext ctx) {
        final int childIndexOfParameterPredicate = 2;

        return P.not(visitTraversalPredicate(castChildToTraversalPredicate(ctx, childIndexOfParameterPredicate)));
    }

    @Override
    public P visitTraversalPredicate_containing(final GremlinParser.TraversalPredicate_containingContext ctx) {
        return TextP.containing(GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral()));
    }

    @Override
    public P visitTraversalPredicate_notContaining(final GremlinParser.TraversalPredicate_notContainingContext ctx) {
        return TextP.notContaining(GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral()));
    }

    @Override
    public P visitTraversalPredicate_notEndingWith(final GremlinParser.TraversalPredicate_notEndingWithContext ctx) {
        return TextP.notEndingWith(GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral()));
    }

    @Override
    public P visitTraversalPredicate_endingWith(final GremlinParser.TraversalPredicate_endingWithContext ctx) {
        return TextP.endingWith(GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral()));
    }

    @Override
    public P visitTraversalPredicate_startingWith(final GremlinParser.TraversalPredicate_startingWithContext ctx) {
        return TextP.startingWith(GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral()));
    }

    @Override
    public P visitTraversalPredicate_notStartingWith(final GremlinParser.TraversalPredicate_notStartingWithContext ctx) {
        return TextP.notStartingWith(GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral()));
    }
}
