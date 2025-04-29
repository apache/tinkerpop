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
package org.apache.tinkerpop.gremlin.language.translator;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.tinkerpop.gremlin.language.grammar.GremlinBaseVisitor;
import org.apache.tinkerpop.gremlin.language.grammar.GremlinParser;
import org.apache.tinkerpop.gremlin.process.traversal.DT;
import org.apache.tinkerpop.gremlin.process.traversal.Merge;
import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Pick;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.SackFunctions;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.TextP;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A Gremlin to Gremlin translator. Makes no changes to input except:
 * <ul>
 *     <li>Normalizes whitespace</li>
 *     <li>Normalize numeric suffixes to lower case</li>
 *     <li>Makes anonymous traversals explicit with double underscore</li>
 *     <li>Makes enums explicit with their proper name</li>
 * </ul>
 */
public class TranslateVisitor extends GremlinBaseVisitor<Void> {

    protected final String graphTraversalSourceName;

    protected final StringBuilder sb = new StringBuilder();

    protected final Set<String> parameters = new HashSet<>();

    public TranslateVisitor() {
        this("g");
    }

    public TranslateVisitor(final String graphTraversalSourceName) {
        this.graphTraversalSourceName = graphTraversalSourceName;
    }

    public String getTranslated() {
        return sb.toString();
    }

    public Set<String> getParameters() {
        return parameters;
    }

    protected String processGremlinSymbol(final String step) {
        return step;
    }

    protected void appendArgumentSeparator() {
        sb.append(", ");
    }

    protected void appendStepSeparator() {
        sb.append(".");
    }

    protected void appendStepOpen() {
        sb.append("(");
    }

    protected void appendStepClose() {
        sb.append(")");
    }

    protected static String removeFirstAndLastCharacters(final String text) {
        return text != null && !text.isEmpty() ? text.substring(1, text.length() - 1) : "";
    }

    @Override
    public Void visitTraversalSource(final GremlinParser.TraversalSourceContext ctx) {
        // replace "g" with whatever the user wanted to use as the graph traversal source name
        sb.append(graphTraversalSourceName);

        // child counts more than 1 means there is a step separator and a traversal source method
        if (ctx.getChildCount() > 1) {
            if (ctx.getChild(0).getChildCount() > 1) {
                appendStepSeparator();
                visitTraversalSourceSelfMethod((GremlinParser.TraversalSourceSelfMethodContext) ctx.getChild(0).getChild(2));
            }
            appendStepSeparator();
            visitTraversalSourceSelfMethod((GremlinParser.TraversalSourceSelfMethodContext) ctx.getChild(2));
        }
        return null;
    }

    @Override
    public Void visitNestedTraversal(final GremlinParser.NestedTraversalContext ctx) {
        if (ctx.ANON_TRAVERSAL_ROOT() == null)
            appendAnonymousSpawn();
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalScope(final GremlinParser.TraversalScopeContext ctx) {
        appendExplicitNaming(ctx.getText(), Scope.class.getSimpleName());
        return null;
    }

    @Override
    public Void visitTraversalT(final GremlinParser.TraversalTContext ctx) {
        appendExplicitNaming(ctx.getText(), T.class.getSimpleName());
        return null;
    }

    @Override
    public Void visitTraversalMerge(final GremlinParser.TraversalMergeContext ctx) {
        appendExplicitNaming(ctx.getText(), Merge.class.getSimpleName());
        return null;
    }

    @Override
    public Void visitTraversalOrder(final GremlinParser.TraversalOrderContext ctx) {
        appendExplicitNaming(ctx.getText(), Order.class.getSimpleName());
        return null;
    }

    @Override
    public Void visitTraversalBarrier(final GremlinParser.TraversalBarrierContext ctx) {
        appendExplicitNaming(ctx.getText(), SackFunctions.Barrier.class.getSimpleName());
        return null;
    }

    @Override
    public Void visitTraversalDirection(final GremlinParser.TraversalDirectionContext ctx) {
        appendExplicitNaming(ctx.getText(), Direction.class.getSimpleName());
        return null;
    }

    @Override
    public Void visitTraversalCardinality(final GremlinParser.TraversalCardinalityContext ctx) {
        // handle the enum style of cardinality if there are no parens otherwise is the function call.
        if (ctx.LPAREN() != null && ctx.RPAREN() != null) {
            final int idx = ctx.K_CARDINALITY() != null ? 2 : 0;
            final String txt = ctx.getChild(idx).getText();
            appendExplicitNaming(txt, getCardinalityFunctionClass());
            appendStepOpen();
            visit(ctx.genericLiteral());
            appendStepClose();
        } else {
            appendExplicitNaming(ctx.getText(), "Cardinality");
        }

        return null;
    }

    @Override
    public Void visitTraversalColumn(final GremlinParser.TraversalColumnContext ctx) {
        appendExplicitNaming(ctx.getText(), Column.class.getSimpleName());
        return null;
    }

    @Override
    public Void visitTraversalPop(final GremlinParser.TraversalPopContext ctx) {
        appendExplicitNaming(ctx.getText(), Pop.class.getSimpleName());
        return null;
    }

    @Override
    public Void visitTraversalOperator(final GremlinParser.TraversalOperatorContext ctx) {
        appendExplicitNaming(ctx.getText(), Operator.class.getSimpleName());
        return null;
    }

    @Override
    public Void visitTraversalPick(final GremlinParser.TraversalPickContext ctx) {
        appendExplicitNaming(ctx.getText(), Pick.class.getSimpleName());
        return null;
    }

    @Override
    public Void visitTraversalDT(final GremlinParser.TraversalDTContext ctx) {
        appendExplicitNaming(ctx.getText(), DT.class.getSimpleName());
        return null;
    }

    @Override
    public Void visitTraversalPredicate(final GremlinParser.TraversalPredicateContext ctx) {
        switch(ctx.getChildCount()) {
            case 1:
                // handle simple predicate
                visit(ctx.getChild(0));
                break;
            case 5:
                // handle negate of P
                visit(ctx.getChild(0));
                sb.append(".").append(processGremlinSymbol("negate")).append("()");
                break;
            case 6:
                // handle and/or predicates
                final int childIndexOfParameterOperator = 2;
                final int childIndexOfCaller = 0;
                final int childIndexOfArgument = 4;

                visit(ctx.getChild(childIndexOfCaller));
                sb.append(".").append(processGremlinSymbol(ctx.getChild(childIndexOfParameterOperator).getText())).append("(");
                visit(ctx.getChild(childIndexOfArgument));
                sb.append(")");
                break;
        }
        return null;
    }

    @Override
    public Void visitTraversalPredicate_eq(final GremlinParser.TraversalPredicate_eqContext ctx) {
        visitP(ctx, P.class, "eq");
        return null;
    }

    @Override
    public Void visitTraversalPredicate_neq(final GremlinParser.TraversalPredicate_neqContext ctx) {
        visitP(ctx, P.class, "neq");
        return null;
    }

    protected void visitP(final ParserRuleContext ctx, final Class<?> clazzOfP, final String methodName) {
        sb.append(clazzOfP.getSimpleName());
        appendStepSeparator();
        sb.append(processGremlinSymbol(methodName));
        appendStepOpen();

        final List<ParseTree> list =  ctx.children.stream().filter(
                t -> t instanceof GremlinParser.GenericLiteralArgumentContext ||
                              t instanceof GremlinParser.GenericLiteralListArgumentContext ||
                              t instanceof GremlinParser.StringArgumentContext ||
                              t instanceof GremlinParser.TraversalPredicateContext).collect(Collectors.toList());
        for (int ix = 0; ix < list.size(); ix++) {
            visit(list.get(ix));
            if (ix < list.size() - 1) appendArgumentSeparator();
        }

        appendStepClose();
    }

    @Override
    public Void visitTraversalPredicate_lt(final GremlinParser.TraversalPredicate_ltContext ctx) {
        visitP(ctx, P.class, "lt");
        return null;
    }

    @Override
    public Void visitTraversalPredicate_lte(final GremlinParser.TraversalPredicate_lteContext ctx) {
        visitP(ctx, P.class, "lte");
        return null;
    }

    @Override
    public Void visitTraversalPredicate_gt(final GremlinParser.TraversalPredicate_gtContext ctx) {
        visitP(ctx, P.class, "gt");
        return null;
    }

    @Override
    public Void visitTraversalPredicate_gte(final GremlinParser.TraversalPredicate_gteContext ctx) {
        visitP(ctx, P.class, "gte");
        return null;
    }

    @Override
    public Void visitTraversalPredicate_inside(final GremlinParser.TraversalPredicate_insideContext ctx) {
        visitP(ctx, P.class, "inside");
        return null;
    }

    @Override
    public Void visitTraversalPredicate_outside(final GremlinParser.TraversalPredicate_outsideContext ctx) {
        visitP(ctx, P.class, "outside");
        return null;
    }

    @Override
    public Void visitTraversalPredicate_between(final GremlinParser.TraversalPredicate_betweenContext ctx) {
        visitP(ctx, P.class, "between");
        return null;
    }

    @Override
    public Void visitTraversalPredicate_within(final GremlinParser.TraversalPredicate_withinContext ctx) {
        visitP(ctx, P.class, "within");
        return null;
    }

    @Override
    public Void visitTraversalPredicate_without(final GremlinParser.TraversalPredicate_withoutContext ctx) {
        visitP(ctx, P.class, "without");
        return null;
    }

    @Override
    public Void visitTraversalPredicate_not(final GremlinParser.TraversalPredicate_notContext ctx) {
        visitP(ctx, P.class, "not");
        return null;
    }

    @Override
    public Void visitTraversalPredicate_containing(final GremlinParser.TraversalPredicate_containingContext ctx) {
        visitP(ctx, TextP.class, "containing");
        return null;
    }

    @Override
    public Void visitTraversalPredicate_notContaining(final GremlinParser.TraversalPredicate_notContainingContext ctx) {
        visitP(ctx, TextP.class, "notContaining");
        return null;
    }

    @Override
    public Void visitTraversalPredicate_startingWith(final GremlinParser.TraversalPredicate_startingWithContext ctx) {
        visitP(ctx, TextP.class, "startingWith");
        return null;
    }

    @Override
    public Void visitTraversalPredicate_notStartingWith(final GremlinParser.TraversalPredicate_notStartingWithContext ctx) {
        visitP(ctx, TextP.class, "notStartingWith");
        return null;
    }

    @Override
    public Void visitTraversalPredicate_endingWith(final GremlinParser.TraversalPredicate_endingWithContext ctx) {
        visitP(ctx, TextP.class, "endingWith");
        return null;
    }

    @Override
    public Void visitTraversalPredicate_notEndingWith(final GremlinParser.TraversalPredicate_notEndingWithContext ctx) {
        visitP(ctx, TextP.class, "notEndingWith");
        return null;
    }

    @Override
    public Void visitTraversalPredicate_regex(final GremlinParser.TraversalPredicate_regexContext ctx) {
        visitP(ctx, TextP.class, "regex");
        return null;
    }

    @Override
    public Void visitTraversalPredicate_notRegex(final GremlinParser.TraversalPredicate_notRegexContext ctx) {
        visitP(ctx, TextP.class, "notRegex");
        return null;
    }

    @Override
    public Void visitBooleanArgument(final GremlinParser.BooleanArgumentContext ctx) {
        if (ctx.booleanLiteral() != null)
            visitBooleanLiteral(ctx.booleanLiteral());
        else
            visitVariable(ctx.variable());

        return null;
    }

    @Override
    public Void visitGenericLiteralArgument(final GremlinParser.GenericLiteralArgumentContext ctx) {
        if (ctx.genericLiteral() != null)
            visitGenericLiteral(ctx.genericLiteral());
        else
            visitVariable(ctx.variable());

        return null;
    }

    @Override
    public Void visitIntegerLiteral(final GremlinParser.IntegerLiteralContext ctx) {
        sb.append(ctx.getText().toLowerCase());
        return null;
    }

    @Override
    public Void visitFloatLiteral(final GremlinParser.FloatLiteralContext ctx) {
        if (ctx.infLiteral() != null) return visit(ctx.infLiteral());
        if (ctx.nanLiteral() != null) return visit(ctx.nanLiteral());
        sb.append(ctx.getText().toLowerCase());
        return null;
    }

    @Override
    public Void visitBooleanLiteral(final GremlinParser.BooleanLiteralContext ctx) {
        sb.append(ctx.getText());
        return null;
    }

    @Override
    public Void visitNullLiteral(final GremlinParser.NullLiteralContext ctx) {
        sb.append(ctx.getText());
        return null;
    }

    @Override
    public Void visitNanLiteral(final GremlinParser.NanLiteralContext ctx) {
        sb.append(ctx.getText());
        return null;
    }

    @Override
    public Void visitInfLiteral(final GremlinParser.InfLiteralContext ctx) {
        sb.append(ctx.getText());
        return null;
    }

    @Override
    public Void visitUuidLiteral(final GremlinParser.UuidLiteralContext ctx) {
        sb.append(ctx.getText());
        return null;
    }

    @Override
    public Void visitVariable(final GremlinParser.VariableContext ctx) {
        final String var = ctx.getText();
        sb.append(var);
        parameters.add(var);
        return null;
    }

    @Override
    public Void visitKeyword(final GremlinParser.KeywordContext ctx) {
        final String keyword = ctx.getText();

        // translate differently based on the context of the keyword's parent.
        if (ctx.getParent() instanceof GremlinParser.MapKeyContext || ctx.getParent() instanceof GremlinParser.ConfigurationContext) {
            // if the keyword is a key in a map, then it's a string literal essentially
            sb.append(keyword);
        } else {
            // in all other cases it's used more like "new Class()"
            sb.append(keyword).append(" ");
        }

        return null;
    }

    @Override
    public Void visitTerminal(final TerminalNode node) {
        // skip EOF node
        if (null == node || node.getSymbol().getType() == -1) return null;

        final String terminal = node.getSymbol().getText();
        switch (terminal) {
            case "(":
                appendStepOpen();
                break;
            case ")":
                appendStepClose();
                break;
            case ",":
                appendArgumentSeparator();
                break;
            case ".":
                appendStepSeparator();
                break;
            case "new":
                // seems we still sometimes interpret this as a TerminalNode like when newing up a class
                // which is optional syntax and will go away in the future.
                sb.append("new");
                if (!(node.getParent() instanceof GremlinParser.MapKeyContext))
                    sb.append(" "); // includes a space for when not use in context of a Map entry key...one off
                break;
            default:
                sb.append(processGremlinSymbol(terminal));
        }
        return null;
    }

    @Override
    public Void visitTraversalTShort(final GremlinParser.TraversalTShortContext ctx) {
        appendExplicitNaming(ctx.getText(), T.class.getSimpleName());
        return null;
    }

    @Override
    public Void visitTraversalTLong(final GremlinParser.TraversalTLongContext ctx) {
        appendExplicitNaming(ctx.getText(), T.class.getSimpleName());
        return null;
    }

    @Override
    public Void visitTraversalDirectionShort(final GremlinParser.TraversalDirectionShortContext ctx) {
        appendExplicitNaming(ctx.getText(), Direction.class.getSimpleName());
        return null;
    }

    @Override
    public Void visitTraversalDirectionLong(final GremlinParser.TraversalDirectionLongContext ctx) {
        appendExplicitNaming(ctx.getText(), Direction.class.getSimpleName());
        return null;
    }

    protected void appendExplicitNaming(final String txt, final String prefix) {
        if (!txt.startsWith(prefix + ".")) {
            sb.append(processGremlinSymbol(prefix)).append(".");
            sb.append(processGremlinSymbol(txt));
        } else {
            final String[] split = txt.split("\\.");
            sb.append(processGremlinSymbol(split[0])).append(".");
            sb.append(processGremlinSymbol(split[1]));
        }
    }

    protected void appendAnonymousSpawn() {
        sb.append("__.");
    }

    protected String getCardinalityFunctionClass() {
        return Cardinality.class.getSimpleName();
    }
}
