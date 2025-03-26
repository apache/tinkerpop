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

import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.gremlin.language.grammar.GremlinParser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.util.DatetimeHelper;

import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Converts a Gremlin traversal string into a C# source code representation of that traversal with an aim at
 * sacrificing some formatting for the ability to compile correctly.
 * <ul>
 *     <li>Range syntax has no direct support</li>
 *     <li>Normalizes whitespace</li>
 *     <li>Normalize numeric suffixes to lower case</li>
 *     <li>If floats are not suffixed they will translate as BigDecimal</li>
 *     <li>Makes anonymous traversals explicit with double underscore</li>
 *     <li>Makes enums explicit with their proper name</li>
 * </ul>
 */
public class DotNetTranslateVisitor extends AbstractTranslateVisitor {
    public DotNetTranslateVisitor() {
        this("g");
    }

    public DotNetTranslateVisitor(final String graphTraversalSourceName) {
        super(graphTraversalSourceName);
    }

    @Override
    public Void visitTraversalDirection(final GremlinParser.TraversalDirectionContext ctx) {
        final String direction = ctx.getText().toLowerCase();
        sb.append("Direction.");
        if (direction.contains("out"))
            sb.append("Out");
        else if (direction.contains("in"))
            sb.append("In");
        else if (direction.contains("from"))
            sb.append("From");
        else if (direction.contains("to"))
            sb.append("To");
        else
            sb.append("Both");
        return null;
    }

    @Override
    public Void visitNanLiteral(final GremlinParser.NanLiteralContext ctx) {
        sb.append("Double.NaN");
        return null;
    }

    @Override
    public Void visitInfLiteral(final GremlinParser.InfLiteralContext ctx) {
        if (ctx.SignedInfLiteral().getText().equals("-Infinity"))
            sb.append("Double.NegativeInfinity");
        else
            sb.append("Double.PositiveInfinity");
        return null;
    }

    @Override
    public Void visitIntegerLiteral(final GremlinParser.IntegerLiteralContext ctx) {
        final String integerLiteral = ctx.getText().toLowerCase();

        // check suffix
        final int lastCharIndex = integerLiteral.length() - 1;
        final char lastCharacter = integerLiteral.charAt(lastCharIndex);
        switch (lastCharacter) {
            case 'b':
                // parse B/b as byte
                sb.append("(byte) ");
                sb.append(integerLiteral, 0, lastCharIndex);
                break;
            case 's':
                // parse S/s as short
                sb.append("(short) ");
                sb.append(integerLiteral, 0, lastCharIndex);
                break;
            case 'i':
                // parse I/i as integer.
                sb.append(integerLiteral, 0, lastCharIndex);
                break;
            case 'l':
                // parse L/l as long
                sb.append(integerLiteral);
                break;
            case 'n':
                sb.append("new BigInteger(");
                sb.append(integerLiteral, 0, lastCharIndex);
                sb.append(")");
                break;
            default:
                // everything else just goes as specified
                sb.append(integerLiteral);
                break;
        }
        return null;
    }

    @Override
    public Void visitFloatLiteral(final GremlinParser.FloatLiteralContext ctx) {
        final String floatLiteral = ctx.getText().toLowerCase();

        // check suffix
        final int lastCharIndex = floatLiteral.length() - 1;
        final char lastCharacter = floatLiteral.charAt(lastCharIndex);
        switch (lastCharacter) {
            case 'f':
            case 'd':
                // parse F/f as Float and D/d suffix as Double
                sb.append(floatLiteral);
                break;
            case 'm':
                // parse M/m or whatever which could be a parse exception
                sb.append("(decimal) ");
                sb.append(floatLiteral, 0, lastCharIndex);
                break;
            default:
                // everything else just goes as specified
                sb.append(floatLiteral);
                break;
        }
        return null;
    }

    @Override
    public Void visitDateLiteral(final GremlinParser.DateLiteralContext ctx) {
        // child at 2 is the date argument to datetime() and comes enclosed in quotes
        final String dtString = ctx.getChild(2).getText();
        final OffsetDateTime dt = DatetimeHelper.parse(removeFirstAndLastCharacters(dtString));
        // todo: update when dotnet datetime serializer is implemented
        sb.append("DateTimeOffset.FromUnixTimeMilliseconds(");
        sb.append(dt.toInstant().toEpochMilli());
        sb.append(")");
        return null;
    }

    @Override
    public Void visitGenericLiteralRange(final GremlinParser.GenericLiteralRangeContext ctx) {
        throw new TranslatorException(".NET does not support range literals");
    }

    @Override
    public Void visitGenericLiteralMap(final GremlinParser.GenericLiteralMapContext ctx) {
        sb.append("new Dictionary<object, object> {");
        for (int i = 0; i < ctx.mapEntry().size(); i++) {
            final GremlinParser.MapEntryContext mapEntryContext = ctx.mapEntry(i);
            visit(mapEntryContext);
            if (i < ctx.mapEntry().size() - 1)
                sb.append(", ");
        }
        sb.append("}");
        return null;
    }

    @Override
    public Void visitGenericLiteralSet(final GremlinParser.GenericLiteralSetContext ctx) {
        sb.append("new HashSet<object> { ");
        for (int i = 0; i < ctx.genericLiteral().size(); i++) {
            final GremlinParser.GenericLiteralContext genericLiteralContext = ctx.genericLiteral(i);
            visit(genericLiteralContext);
            if (i < ctx.genericLiteral().size() - 1)
                sb.append(", ");
        }
        sb.append(" }");
        return null;
    }

    @Override
    public Void visitGenericLiteralCollection(final GremlinParser.GenericLiteralCollectionContext ctx) {
        sb.append("new List<object> { ");
        for (int i = 0; i < ctx.genericLiteral().size(); i++) {
            final GremlinParser.GenericLiteralContext genericLiteralContext = ctx.genericLiteral(i);
            visit(genericLiteralContext);
            if (i < ctx.genericLiteral().size() - 1)
                sb.append(", ");
        }
        sb.append(" }");
        return null;
    }

    @Override
    public Void visitStringLiteralList(final GremlinParser.StringLiteralListContext ctx) {
        sb.append("new List<string> { ");
        for (int ix = 0; ix < ctx.getChild(1).getChildCount(); ix++) {
            if (ctx.getChild(1).getChild(ix) instanceof TerminalNode)
                continue;
            visit(ctx.getChild(1).getChild(ix));
            if (ix < ctx.getChild(1).getChildCount() - 1)
                sb.append(", ");
        }
        sb.append(" }");
        return null;
    }

    @Override
    public Void visitMapEntry(final GremlinParser.MapEntryContext ctx) {
        sb.append("{ ");
        // if it is a terminal node that isn't a starting form like "(T.id)" then it has to be processed as a string
        // for Java but otherwise it can just be handled as a generic literal
        final boolean isKeyWrappedInParens = ctx.getChild(0).getText().equals("(");
        if (ctx.getChild(0) instanceof TerminalNode && !isKeyWrappedInParens) {
            handleStringLiteralText(ctx.getChild(0).getText());
        }  else {
            final int indexOfActualKey = isKeyWrappedInParens ? 1 : 0;
            visit(ctx.getChild(indexOfActualKey));
        }
        sb.append(", ");
        final int indexOfValue = isKeyWrappedInParens ? 4 : 2;
        visit(ctx.getChild(indexOfValue)); // value
        sb.append(" }");
        return null;
    }

    @Override
    public Void visitTraversalStrategy(final GremlinParser.TraversalStrategyContext ctx) {

        if (ctx.getChildCount() == 1)
            sb.append("new ").append(ctx.getText()).append("()");
        else {
            sb.append("new ").append(ctx.getChild(0).getText().equals("new") ? ctx.getChild(1).getText() : ctx.getChild(0).getText()).append("(");

            final List<ParseTree> configs = ctx.children.stream().
                    filter(c -> c instanceof GremlinParser.ConfigurationContext).collect(Collectors.toList());

            // the rest are the arguments to the strategy
            for (int ix = 0; ix < configs.size(); ix++) {
                visit(configs.get(ix));
                if (ix < configs.size() - 1)
                    sb.append(", ");
            }

            sb.append(")");;
        }
        return null;
    }

    @Override
    public Void visitConfiguration(final GremlinParser.ConfigurationContext ctx) {
        // form of three tokens of key:value to become key=value
        sb.append(ctx.getChild(0).getText());
        sb.append(": ");
        visit(ctx.getChild(2));

        // need to convert List to Set for readPartitions until TINKERPOP-3032
        if (ctx.getChild(0).getText().equals("readPartitions")) {
            // find the last "List" in sb and replace it with "HashSet"
            final int ix = sb.lastIndexOf("List<object>");
            sb.replace(ix, ix + 12, "HashSet<string>");
        }

        return null;
    }

    @Override
    public Void visitTraversalCardinality(final GremlinParser.TraversalCardinalityContext ctx) {
        // handle the enum style of cardinality if there is one child, otherwise it's the function call style
        if (ctx.getChildCount() == 1)
            appendExplicitNaming(ctx.getText(), VertexProperty.Cardinality.class.getSimpleName());
        else {
            String txt = ctx.getChild(0).getText();
            if (txt.startsWith("Cardinality.")) {
                txt = txt.replaceFirst("Cardinality.", "");
            }
            appendExplicitNaming(txt, "CardinalityValue");
            appendStepOpen();
            visit(ctx.getChild(2));
            appendStepClose();
        }

        return null;
    }

    @Override
    public Void visitTraversalSourceSpawnMethod_inject(final GremlinParser.TraversalSourceSpawnMethod_injectContext ctx) {
        return handleGenerics(ctx);
    }

    @Override
    public Void visitTraversalSourceSpawnMethod_io(final GremlinParser.TraversalSourceSpawnMethod_ioContext ctx) {
        return handleGenerics(ctx);
    }

    @Override
    public Void visitTraversalSourceSpawnMethod_call_empty(final GremlinParser.TraversalSourceSpawnMethod_call_emptyContext ctx) {
        return handleGenerics(ctx);
    }

    @Override
    public Void visitTraversalSourceSpawnMethod_call_string(final GremlinParser.TraversalSourceSpawnMethod_call_stringContext ctx) {
        final String step = ctx.getChild(0).getText();
        sb.append(convertToPascalCase(step));
        sb.append("<object>").append("((string) ");
        visit(ctx.stringLiteral());
        sb.append(")");
        return null;
    }

    @Override
    public Void visitTraversalSourceSpawnMethod_call_string_map(final GremlinParser.TraversalSourceSpawnMethod_call_string_mapContext ctx) {
        final String step = ctx.getChild(0).getText();
        sb.append(convertToPascalCase(step));
        sb.append("<object>").append("(");
        visit(ctx.stringLiteral());
        sb.append(", ");
        sb.append("(IDictionary<object, object>) ");
        visit(ctx.genericLiteralMapArgument());
        sb.append(")");
        return null;
    }

    @Override
    public Void visitTraversalSourceSpawnMethod_call_string_traversal(final GremlinParser.TraversalSourceSpawnMethod_call_string_traversalContext ctx) {
        final String step = ctx.getChild(0).getText();
        sb.append(convertToPascalCase(step));
        sb.append("<object>").append("(");
        visit(ctx.stringLiteral());
        sb.append(", ");
        sb.append("(ITraversal) ");
        visit(ctx.nestedTraversal());
        sb.append(")");
        return null;
    }

    @Override
    public Void visitTraversalSourceSpawnMethod_call_string_map_traversal(final GremlinParser.TraversalSourceSpawnMethod_call_string_map_traversalContext ctx) {
        final String step = ctx.getChild(0).getText();
        sb.append(convertToPascalCase(step));
        sb.append("<object>").append("(");
        visit(ctx.stringLiteral());
        sb.append(", ");
        sb.append("(IDictionary<object, object>) ");
        visit(ctx.genericLiteralMapArgument());
        sb.append(", ");
        sb.append("(ITraversal) ");
        visit(ctx.nestedTraversal());
        sb.append(")");
        return null;
    }

    @Override
    public Void visitTraversalSourceSpawnMethod_mergeV_Map(final GremlinParser.TraversalSourceSpawnMethod_mergeV_MapContext ctx) {
        // call is ambiguous without an explicit cast
        visit(ctx.getChild(0));
        sb.append("(");
        sb.append("(IDictionary<object, object>) ");
        visit(ctx.genericLiteralMapNullableArgument());
        sb.append(")");
        return null;
    }

    @Override
    public Void visitTraversalSourceSpawnMethod_mergeV_Traversal(final GremlinParser.TraversalSourceSpawnMethod_mergeV_TraversalContext ctx) {
        // call is ambiguous without an explicit cast
        visit(ctx.getChild(0));
        sb.append("(");
        sb.append("(ITraversal) ");
        visit(ctx.nestedTraversal());
        sb.append(")");
        return null;
    }

    @Override
    public Void visitTraversalSourceSpawnMethod_mergeE_Map(final GremlinParser.TraversalSourceSpawnMethod_mergeE_MapContext ctx) {
        // call is ambiguous without an explicit cast
        visit(ctx.getChild(0));
        sb.append("(");
        sb.append("(IDictionary<object, object>) ");
        visit(ctx.genericLiteralMapNullableArgument());
        sb.append(")");
        return null;
    }

    @Override
    public Void visitTraversalSourceSpawnMethod_mergeE_Traversal(final GremlinParser.TraversalSourceSpawnMethod_mergeE_TraversalContext ctx) {
        // call is ambiguous without an explicit cast
        visit(ctx.getChild(0));
        sb.append("(");
        sb.append("(ITraversal) ");
        visit(ctx.nestedTraversal());
        sb.append(")");
        return null;
    }

    @Override
    public Void visitTraversalSourceSpawnMethod_union(final GremlinParser.TraversalSourceSpawnMethod_unionContext ctx) {
        return handleGenerics(ctx);
    }

    @Override
    public Void visitTraversalMethod_asString_Scope(final GremlinParser.TraversalMethod_asString_ScopeContext ctx) {
        return handleGenerics(ctx);
    }

    @Override
    public Void visitTraversalMethod_branch(final GremlinParser.TraversalMethod_branchContext ctx) {
        return handleGenerics(ctx);
    }

    @Override
    public Void visitTraversalMethod_call_string(final GremlinParser.TraversalMethod_call_stringContext ctx) {
        final String step = ctx.getChild(0).getText();
        sb.append(convertToPascalCase(step));
        sb.append("<object>").append("((string) ");
        visit(ctx.stringLiteral());
        sb.append(")");
        return null;
    }

    @Override
    public Void visitTraversalMethod_call_string_map(final GremlinParser.TraversalMethod_call_string_mapContext ctx) {
        final String step = ctx.getChild(0).getText();
        sb.append(convertToPascalCase(step));
        sb.append("<object>").append("(");
        visit(ctx.stringLiteral());
        sb.append(", ");
        sb.append("(IDictionary<object, object>) ");
        visit(ctx.genericLiteralMapArgument());
        sb.append(")");
        return null;
    }

    @Override
    public Void visitTraversalMethod_call_string_traversal(final GremlinParser.TraversalMethod_call_string_traversalContext ctx) {
        final String step = ctx.getChild(0).getText();
        sb.append(convertToPascalCase(step));
        sb.append("<object>").append("(");
        visit(ctx.stringLiteral());
        sb.append(", ");
        sb.append("(ITraversal) ");
        visit(ctx.nestedTraversal());
        sb.append(")");
        return null;
    }

    @Override
    public Void visitTraversalMethod_call_string_map_traversal(final GremlinParser.TraversalMethod_call_string_map_traversalContext ctx) {
        final String step = ctx.getChild(0).getText();
        sb.append(convertToPascalCase(step));
        sb.append("<object>").append("(");
        visit(ctx.stringLiteral());
        sb.append(", ");
        sb.append("(IDictionary<object, object>) ");
        visit(ctx.genericLiteralMapArgument());
        sb.append(", ");
        sb.append("(ITraversal) ");
        visit(ctx.nestedTraversal());
        sb.append(")");
        return null;
    }

    @Override
    public Void visitTraversalMethod_cap(final GremlinParser.TraversalMethod_capContext ctx) {
        return handleGenerics(ctx);
    }

    @Override
    public Void visitTraversalMethod_choose_Function(final GremlinParser.TraversalMethod_choose_FunctionContext ctx) {
        return handleGenerics(ctx);
    }

    @Override
    public Void visitTraversalMethod_choose_Predicate_Traversal(final GremlinParser.TraversalMethod_choose_Predicate_TraversalContext ctx) {
        return handleGenerics(ctx);
    }

    @Override
    public Void visitTraversalMethod_choose_Predicate_Traversal_Traversal(final GremlinParser.TraversalMethod_choose_Predicate_Traversal_TraversalContext ctx) {
        return handleGenerics(ctx);
    }

    @Override
    public Void visitTraversalMethod_choose_Traversal(final GremlinParser.TraversalMethod_choose_TraversalContext ctx) {
        return handleGenerics(ctx);
    }

    @Override
    public Void visitTraversalMethod_choose_Traversal_Traversal(final GremlinParser.TraversalMethod_choose_Traversal_TraversalContext ctx) {
        return handleGenerics(ctx);
    }

    @Override
    public Void visitTraversalMethod_choose_Traversal_Traversal_Traversal(final GremlinParser.TraversalMethod_choose_Traversal_Traversal_TraversalContext ctx) {
        return handleGenerics(ctx);
    }

    @Override
    public Void visitTraversalMethod_coalesce(final GremlinParser.TraversalMethod_coalesceContext ctx) {
        return handleGenerics(ctx);
    }

    @Override
    public Void visitTraversalMethod_constant(final GremlinParser.TraversalMethod_constantContext ctx) {
        return handleGenerics(ctx);
    }

    @Override
    public Void visitTraversalMethod_elementMap(final GremlinParser.TraversalMethod_elementMapContext ctx) {
        return handleGenerics(ctx);
    }

    @Override
    public Void visitTraversalMethod_flatMap(final GremlinParser.TraversalMethod_flatMapContext ctx) {
        return handleGenerics(ctx);
    }

    @Override
    public Void visitTraversalMethod_fold_Empty(final GremlinParser.TraversalMethod_fold_EmptyContext ctx) {
        // calling __.fold() as a start step needs the generics
        if (isCalledAsFirstStepInAnonymousTraversal(ctx))
            return handleGenerics(ctx);
        else
            return super.visitTraversalMethod_fold_Empty(ctx);
    }

    @Override
    public Void visitTraversalMethod_fold_Object_BiFunction(final GremlinParser.TraversalMethod_fold_Object_BiFunctionContext ctx) {
        return handleGenerics(ctx);
    }

    @Override
    public Void visitTraversalMethod_group_Empty(final GremlinParser.TraversalMethod_group_EmptyContext ctx) {
        return handleGenerics(ctx);
    }

    @Override
    public Void visitTraversalMethod_groupCount_Empty(final GremlinParser.TraversalMethod_groupCount_EmptyContext ctx) {
        return handleGenerics(ctx);
    }

    @Override
    public Void visitTraversalMethod_has_String_Object(final GremlinParser.TraversalMethod_has_String_ObjectContext ctx) {
        final String step = ctx.getChild(0).getText();
        sb.append(convertToPascalCase(step));
        sb.append("(");
        tryAppendCastToString(ctx.stringNullableLiteral());
        visit(ctx.stringNullableLiteral());
        sb.append(", ");
        tryAppendCastToObject(ctx.genericLiteralArgument());
        visit(ctx.genericLiteralArgument());
        sb.append(")");
        return null;
    }

    @Override
    public Void visitTraversalMethod_has_String_P(GremlinParser.TraversalMethod_has_String_PContext ctx) {
        final String step = ctx.getChild(0).getText();
        sb.append(convertToPascalCase(step));
        sb.append("(");
        tryAppendCastToString(ctx.stringNullableLiteral());
        visit(ctx.stringNullableLiteral());
        sb.append(", ");
        visit(ctx.traversalPredicate());
        sb.append(")");
        return null;
    }

    @Override
    public Void visitTraversalMethod_has_String_String_Object(final GremlinParser.TraversalMethod_has_String_String_ObjectContext ctx) {
        final String step = ctx.getChild(0).getText();
        sb.append(convertToPascalCase(step));
        sb.append("(");
        tryAppendCastToString(ctx.stringNullableArgument());
        visit(ctx.stringNullableArgument());
        sb.append(", ");
        tryAppendCastToString(ctx.stringNullableLiteral());
        visit(ctx.stringNullableLiteral());
        sb.append(", ");
        tryAppendCastToObject(ctx.genericLiteralArgument());
        visit(ctx.genericLiteralArgument());
        sb.append(")");
        return null;
    }

    @Override
    public Void visitTraversalMethod_has_String_String_P(final GremlinParser.TraversalMethod_has_String_String_PContext ctx) {
        final String step = ctx.getChild(0).getText();
        sb.append(convertToPascalCase(step));
        sb.append("(");
        tryAppendCastToString(ctx.stringNullableArgument());
        visit(ctx.stringNullableArgument());
        sb.append(", ");
        tryAppendCastToString(ctx.stringNullableLiteral());
        visit(ctx.stringNullableLiteral());
        sb.append(", ");
        visit(ctx.traversalPredicate());
        sb.append(")");
        return null;
    }

    @Override
    public Void visitTraversalMethod_has_String_Traversal(final GremlinParser.TraversalMethod_has_String_TraversalContext ctx) {
        final String step = ctx.getChild(0).getText();
        sb.append(convertToPascalCase(step));
        sb.append("(");
        tryAppendCastToString(ctx.stringNullableLiteral());
        visit(ctx.stringNullableLiteral());
        sb.append(", ");
        visit(ctx.nestedTraversal());
        sb.append(")");
        return null;
    }

    @Override
    public Void visitTraversalMethod_has_T_Object(final GremlinParser.TraversalMethod_has_T_ObjectContext ctx) {
        final String step = ctx.getChild(0).getText();
        sb.append(convertToPascalCase(step));
        sb.append("(");
        visit(ctx.traversalToken());
        sb.append(", ");
        tryAppendCastToObject(ctx.genericLiteralArgument());
        visit(ctx.genericLiteralArgument());
        sb.append(")");
        return null;
    }

    @Override
    public Void visitTraversalMethod_has_T_P(final GremlinParser.TraversalMethod_has_T_PContext ctx) {
        final String step = ctx.getChild(0).getText();
        sb.append(convertToPascalCase(step));
        sb.append("(");
        visit(ctx.traversalToken());
        sb.append(", ");
        visit(ctx.traversalPredicate());
        sb.append(")");
        return null;
    }

    @Override
    public Void visitTraversalMethod_has_T_Traversal(final GremlinParser.TraversalMethod_has_T_TraversalContext ctx) {
        final String step = ctx.getChild(0).getText();
        sb.append(convertToPascalCase(step));
        sb.append("(");
        visit(ctx.traversalToken());
        sb.append(", ");
        visit(ctx.nestedTraversal());
        sb.append(")");
        return null;
    }

    @Override
    public Void visitTraversalMethod_hasKey_P(final GremlinParser.TraversalMethod_hasKey_PContext ctx) {
        final String step = ctx.getChild(0).getText();
        sb.append(convertToPascalCase(step));
        sb.append("(");
        visit(ctx.traversalPredicate());
        sb.append(")");
        return null;
    }

    @Override
    public Void visitTraversalMethod_hasKey_String_String(final GremlinParser.TraversalMethod_hasKey_String_StringContext ctx) {
        // if there is only one argument then cast to string otherwise it's ambiguous with hasKey(P)
        if (ctx.stringLiteralVarargsLiterals() == null || ctx.stringLiteralVarargsLiterals().getChildCount() == 0) {
            final String step = ctx.getChild(0).getText();
            sb.append(convertToPascalCase(step));
            sb.append("(");
            tryAppendCastToString(ctx.stringNullableLiteral());
            visit(ctx.stringNullableLiteral());
            sb.append(")");
            return null;
        } else {
            return super.visitTraversalMethod_hasKey_String_String(ctx);
        }
    }

    @Override
    public Void visitTraversalMethod_hasValue_Object_Object(final GremlinParser.TraversalMethod_hasValue_Object_ObjectContext ctx) {
        // if there is only one argument then cast to object otherwise it's ambiguous with hasValue(P)
        if (ctx.genericLiteralVarargs() == null || ctx.genericLiteralVarargs().getChildCount() == 0) {
            final String step = ctx.getChild(0).getText();
            sb.append(convertToPascalCase(step));
            sb.append("(");
            tryAppendCastToObject(ctx.genericLiteralArgument());
            visit(ctx.genericLiteralArgument());
            sb.append(")");
            return null;
        } else {
            return super.visitTraversalMethod_hasValue_Object_Object(ctx);
        }
    }

    @Override
    public Void visitTraversalMethod_hasValue_P(final GremlinParser.TraversalMethod_hasValue_PContext ctx) {
        final String step = ctx.getChild(0).getText();
        sb.append(convertToPascalCase(step));
        sb.append("(");
        visit(ctx.traversalPredicate());
        sb.append(")");
        return null;
    }

    @Override
    public Void visitTraversalMethod_hasLabel_P(final GremlinParser.TraversalMethod_hasLabel_PContext ctx) {
        final String step = ctx.getChild(0).getText();
        sb.append(convertToPascalCase(step));
        sb.append("(");
        visit(ctx.traversalPredicate());
        sb.append(")");
        return null;
    }

    @Override
    public Void visitTraversalMethod_hasLabel_String_String(final GremlinParser.TraversalMethod_hasLabel_String_StringContext ctx) {
        // if there is only one argument then cast to string otherwise it's ambiguous with hasLabel(P)
        if (ctx.stringLiteralVarargs() == null || ctx.stringLiteralVarargs().getChildCount() == 0) {
            final String step = ctx.getChild(0).getText();
            sb.append(convertToPascalCase(step));
            sb.append("(");
            tryAppendCastToString(ctx.stringNullableArgument());
            visit(ctx.stringNullableArgument());
            sb.append(")");
            return null;
        } else {
            final String step = ctx.getChild(0).getText();
            sb.append(convertToPascalCase(step));
            sb.append("(");
            tryAppendCastToString(ctx.stringNullableArgument());
            visit(ctx.stringNullableArgument());

            // more arguments to come
            if (!ctx.stringLiteralVarargs().isEmpty())  sb.append(", ");
            visit(ctx.stringLiteralVarargs());

            sb.append(")");
            return null;
        }
    }

    @Override
    public Void visitStringLiteralVarargs(final GremlinParser.StringLiteralVarargsContext ctx) {
        for (int ix = 0; ix < ctx.getChildCount(); ix++) {
            final ParseTree pt = ctx.getChild(ix);
            if (pt instanceof GremlinParser.StringNullableArgumentContext) {
                GremlinParser.StringNullableArgumentContext sna = (GremlinParser.StringNullableArgumentContext) pt;
                tryAppendCastToString(sna);
                visit(sna);
            } else {
                visit(pt);
            }
        };
        return null;
    }

    @Override
    public Void visitTraversalMethod_index(final GremlinParser.TraversalMethod_indexContext ctx) {
        return handleGenerics(ctx);
    }

    @Override
    public Void visitTraversalMethod_length_Scope(final GremlinParser.TraversalMethod_length_ScopeContext ctx) {
        return handleGenerics(ctx);
    }

    @Override
    public Void visitTraversalMethod_limit_Scope_long(final GremlinParser.TraversalMethod_limit_Scope_longContext ctx) {
        return handleGenerics(ctx);
    }

    @Override
    public Void visitTraversalMethod_limit_long(final GremlinParser.TraversalMethod_limit_longContext ctx) {
        return handleGenerics(ctx);
    }

    @Override
    public Void visitTraversalMethod_local(final GremlinParser.TraversalMethod_localContext ctx) {
        return handleGenerics(ctx);
    }

    @Override
    public Void visitTraversalMethod_lTrim_Scope(final GremlinParser.TraversalMethod_lTrim_ScopeContext ctx) {
        return handleGenerics(ctx);
    }

    @Override
    public Void visitTraversalMethod_map(final GremlinParser.TraversalMethod_mapContext ctx) {
        return handleGenerics(ctx);
    }

    @Override
    public Void visitTraversalMethod_match(final GremlinParser.TraversalMethod_matchContext ctx) {
        return handleGenerics(ctx);
    }

    @Override
    public Void visitTraversalMethod_max_Empty(final GremlinParser.TraversalMethod_max_EmptyContext ctx) {
        return handleGenerics(ctx);
    }

    @Override
    public Void visitTraversalMethod_max_Scope(final GremlinParser.TraversalMethod_max_ScopeContext ctx) {
        return handleGenerics(ctx);
    }

    @Override
    public Void visitTraversalMethod_mean_Empty(final GremlinParser.TraversalMethod_mean_EmptyContext ctx) {
        return handleGenerics(ctx);
    }

    @Override
    public Void visitTraversalMethod_mean_Scope(final GremlinParser.TraversalMethod_mean_ScopeContext ctx) {
        return handleGenerics(ctx);
    }

    @Override
    public Void visitTraversalMethod_mergeV_Map(final GremlinParser.TraversalMethod_mergeV_MapContext ctx) {
        // call is ambiguous without an explicit cast
        visit(ctx.getChild(0));
        sb.append("(");
        sb.append("(IDictionary<object, object>) ");
        visit(ctx.genericLiteralMapNullableArgument());
        sb.append(")");
        return null;
    }

    @Override
    public Void visitTraversalMethod_mergeV_Traversal(final GremlinParser.TraversalMethod_mergeV_TraversalContext ctx) {
        // call is ambiguous without an explicit cast
        visit(ctx.getChild(0));
        sb.append("(");
        sb.append("(ITraversal) ");
        visit(ctx.nestedTraversal());
        sb.append(")");
        return null;
    }

    @Override
    public Void visitTraversalMethod_mergeE_Map(final GremlinParser.TraversalMethod_mergeE_MapContext ctx) {
        // call is ambiguous without an explicit cast
        visit(ctx.getChild(0));
        sb.append("(");
        sb.append("(IDictionary<object, object>) ");
        visit(ctx.genericLiteralMapNullableArgument());
        sb.append(")");
        return null;
    }

    @Override
    public Void visitTraversalMethod_mergeE_Traversal(final GremlinParser.TraversalMethod_mergeE_TraversalContext ctx) {
        // call is ambiguous without an explicit cast
        visit(ctx.getChild(0));
        sb.append("(");
        sb.append("(ITraversal) ");
        visit(ctx.nestedTraversal());
        sb.append(")");
        return null;
    }

    @Override
    public Void visitTraversalMethod_min_Empty(final GremlinParser.TraversalMethod_min_EmptyContext ctx) {
        return handleGenerics(ctx);
    }

    @Override
    public Void visitTraversalMethod_min_Scope(final GremlinParser.TraversalMethod_min_ScopeContext ctx) {
        return handleGenerics(ctx);
    }

    @Override
    public Void visitTraversalMethod_option_Merge_Map(final GremlinParser.TraversalMethod_option_Merge_MapContext ctx) {
        // call is ambiguous without an explicit cast
        visit(ctx.getChild(0));
        sb.append("(");
        visit(ctx.traversalMerge());
        sb.append(", ");
        sb.append("(IDictionary<object, object>) ");
        visit(ctx.genericLiteralMapNullableArgument()); // second argument
        sb.append(")");
        return null;
    }

    @Override
    public Void visitTraversalMethod_option_Object_Traversal(final GremlinParser.TraversalMethod_option_Object_TraversalContext ctx) {
        if (ctx.genericLiteralArgument().genericLiteral() != null && ctx.genericLiteralArgument().genericLiteral().traversalMerge() != null) {
            visit(ctx.getChild(0));
            sb.append("(");
            visit(ctx.genericLiteralArgument());
            sb.append(", ");
            sb.append("(ITraversal) ");
            visit(ctx.nestedTraversal());
            sb.append(")");
            return null;
        } else {
            return super.visitTraversalMethod_option_Object_Traversal(ctx);
        }
    }

    @Override
    public Void visitTraversalMethod_option_Merge_Traversal(final GremlinParser.TraversalMethod_option_Merge_TraversalContext ctx) {
        visit(ctx.getChild(0));
        sb.append("(");
        visit(ctx.traversalMerge());
        sb.append(", ");
        sb.append("(ITraversal) ");
        visit(ctx.nestedTraversal());
        sb.append(")");
        return null;
    }

    @Override
    public Void visitTraversalMethod_optional(final GremlinParser.TraversalMethod_optionalContext ctx) {
        return handleGenerics(ctx);
    }

    @Override
    public Void visitTraversalMethod_profile_Empty(final GremlinParser.TraversalMethod_profile_EmptyContext ctx) {
        return handleGenerics(ctx);
    }

    @Override
    public Void visitTraversalMethod_project(final GremlinParser.TraversalMethod_projectContext ctx) {
        return handleGenerics(ctx);
    }

    @Override
    public Void visitTraversalMethod_properties(final GremlinParser.TraversalMethod_propertiesContext ctx) {
        return handleGenerics(ctx);
    }

    @Override
    public Void visitTraversalMethod_property_Cardinality_Object_Object_Object(final GremlinParser.TraversalMethod_property_Cardinality_Object_Object_ObjectContext ctx) {
        if (ctx.genericLiteralVarargs() == null || ctx.genericLiteralVarargs().getChildCount() == 0) {
            final String step = ctx.getChild(0).getText();
            sb.append(convertToPascalCase(step));
            sb.append("(");
            visit(ctx.traversalCardinality());
            sb.append(", ");
            tryAppendCastToObject(ctx.genericLiteralArgument(0));
            visit(ctx.genericLiteralArgument(0));
            sb.append(", ");
            tryAppendCastToObject(ctx.genericLiteralArgument(1));
            visit(ctx.genericLiteralArgument(1));
            sb.append(")");
            return null;
        } else {
            return super.visitTraversalMethod_property_Cardinality_Object_Object_Object(ctx);
        }
    }

    @Override
    public Void visitTraversalMethod_conjoin_String(final GremlinParser.TraversalMethod_conjoin_StringContext ctx) {
        final String step = ctx.getChild(0).getText();
        sb.append(convertToPascalCase(step));
        sb.append("(");
        tryAppendCastToString(ctx.stringArgument());
        visit(ctx.stringArgument());
        sb.append(")");
        return null;
    }

    @Override
    public Void visitTraversalMethod_propertyMap(final GremlinParser.TraversalMethod_propertyMapContext ctx) {
        return handleGenerics(ctx);
    }

    @Override
    public Void visitTraversalMethod_range_Scope_long_long(final GremlinParser.TraversalMethod_range_Scope_long_longContext ctx) {
        return handleGenerics(ctx);
    }

    @Override
    public Void visitTraversalMethod_range_long_long(final GremlinParser.TraversalMethod_range_long_longContext ctx) {
        return handleGenerics(ctx);
    }

    @Override
    public Void visitTraversalMethod_replace_Scope_String_String(final GremlinParser.TraversalMethod_replace_Scope_String_StringContext ctx) {
        return handleGenerics(ctx);
    }

    @Override
    public Void visitTraversalMethod_rTrim_Scope(final GremlinParser.TraversalMethod_rTrim_ScopeContext ctx) {
        return handleGenerics(ctx);
    }

    @Override
    public Void visitTraversalMethod_sack_Empty(final GremlinParser.TraversalMethod_sack_EmptyContext ctx) {
        return handleGenerics(ctx);
    }

    @Override
    public Void visitTraversalMethod_select_Column(final GremlinParser.TraversalMethod_select_ColumnContext ctx) {
        return handleGenerics(ctx);
    }

    @Override
    public Void visitTraversalMethod_select_Pop_String(final GremlinParser.TraversalMethod_select_Pop_StringContext ctx) {
        return handleGenerics(ctx);
    }

    @Override
    public Void visitTraversalMethod_select_Pop_String_String_String(final GremlinParser.TraversalMethod_select_Pop_String_String_StringContext ctx) {
        return handleGenerics(ctx);
    }

    @Override
    public Void visitTraversalMethod_select_Pop_Traversal(final GremlinParser.TraversalMethod_select_Pop_TraversalContext ctx) {
        return handleGenerics(ctx);
    }

    @Override
    public Void visitTraversalMethod_select_String(final GremlinParser.TraversalMethod_select_StringContext ctx) {
        return handleGenerics(ctx);
    }

    @Override
    public Void visitTraversalMethod_select_String_String_String(final GremlinParser.TraversalMethod_select_String_String_StringContext ctx) {
        return handleGenerics(ctx);
    }

    @Override
    public Void visitTraversalMethod_select_Traversal(final GremlinParser.TraversalMethod_select_TraversalContext ctx) {
        return handleGenerics(ctx);
    }

    @Override
    public Void visitTraversalMethod_skip_long(final GremlinParser.TraversalMethod_skip_longContext ctx) {
        return handleGenerics(ctx);
    }

    @Override
    public Void visitTraversalMethod_skip_Scope_long(final GremlinParser.TraversalMethod_skip_Scope_longContext ctx) {
        return handleGenerics(ctx);
    }

    @Override
    public Void visitTraversalMethod_split_Scope_String(final GremlinParser.TraversalMethod_split_Scope_StringContext ctx) {
        return handleGenerics(ctx);
    }

    @Override
    public Void visitTraversalMethod_substring_Scope_int(final GremlinParser.TraversalMethod_substring_Scope_intContext ctx) {
        return handleGenerics(ctx);
    }

    @Override
    public Void visitTraversalMethod_substring_Scope_int_int(final GremlinParser.TraversalMethod_substring_Scope_int_intContext ctx) {
        return handleGenerics(ctx);
    }

    @Override
    public Void visitTraversalMethod_sum_Empty(final GremlinParser.TraversalMethod_sum_EmptyContext ctx) {
        return handleGenerics(ctx);
    }

    @Override
    public Void visitTraversalMethod_sum_Scope(final GremlinParser.TraversalMethod_sum_ScopeContext ctx) {
        return handleGenerics(ctx);
    }

    @Override
    public Void visitTraversalMethod_tail_Empty(final GremlinParser.TraversalMethod_tail_EmptyContext ctx) {
        return handleGenerics(ctx);
    }

    @Override
    public Void visitTraversalMethod_tail_Scope(final GremlinParser.TraversalMethod_tail_ScopeContext ctx) {
        return handleGenerics(ctx);
    }

    @Override
    public Void visitTraversalMethod_tail_Scope_long(final GremlinParser.TraversalMethod_tail_Scope_longContext ctx) {
        return handleGenerics(ctx);
    }

    @Override
    public Void visitTraversalMethod_tail_long(final GremlinParser.TraversalMethod_tail_longContext ctx) {
        return handleGenerics(ctx);
    }

    @Override
    public Void visitTraversalMethod_toUpper_Scope(final GremlinParser.TraversalMethod_toUpper_ScopeContext ctx) {
        return handleGenerics(ctx);
    }

    @Override
    public Void visitTraversalMethod_toLower_Scope(final GremlinParser.TraversalMethod_toLower_ScopeContext ctx) {
        return handleGenerics(ctx);
    }

    @Override
    public Void visitTraversalMethod_tree_Empty(final GremlinParser.TraversalMethod_tree_EmptyContext ctx) {
        return handleGenerics(ctx);
    }

    @Override
    public Void visitTraversalMethod_trim_Scope(final GremlinParser.TraversalMethod_trim_ScopeContext ctx) {
        return handleGenerics(ctx);
    }

    @Override
    public Void visitTraversalMethod_unfold(final GremlinParser.TraversalMethod_unfoldContext ctx) {
        return handleGenerics(ctx);
    }

    @Override
    public Void visitTraversalMethod_union(final GremlinParser.TraversalMethod_unionContext ctx) {
        return handleGenerics(ctx);
    }

    @Override
    public Void visitTraversalMethod_value(final GremlinParser.TraversalMethod_valueContext ctx) {
        return handleGenerics(ctx);
    }

    @Override
    public Void visitTraversalMethod_valueMap_String(final GremlinParser.TraversalMethod_valueMap_StringContext ctx) {
        return handleGenerics(ctx);
    }

    @Override
    public Void visitTraversalMethod_valueMap_boolean_String(final GremlinParser.TraversalMethod_valueMap_boolean_StringContext ctx) {
        return handleGenerics(ctx);
    }

    @Override
    public Void visitTraversalMethod_values(final GremlinParser.TraversalMethod_valuesContext ctx) {
        return handleGenerics(ctx);
    }

    @Override
    public Void visitClassType(final GremlinParser.ClassTypeContext ctx) {
        sb.append("typeof(").append(ctx.getText()).append(")");
        return null;
    }

    /**
     * Steps with a {@code <TNewEnd>} defined need special handling to append generics.
     */
    private Void handleGenerics(final ParseTree ctx) {
        final String step = ctx.getChild(0).getText();
        sb.append(convertToPascalCase(step));

        if (step.equals(GraphTraversal.Symbols.group) || step.equals(GraphTraversal.Symbols.valueMap))
            sb.append("<object, object>");
        else
            sb.append("<object>");

        for (int ix = 1; ix < ctx.getChildCount(); ix++) {
            visit(ctx.getChild(ix));
        }
        return null;
    }

    @Override
    protected String processGremlinSymbol(final String step) {
        return SymbolHelper.toCSharp(step);
    }

    /**
     * The default behavior for this method is to call {@link #processGremlinSymbol(String)} but there really isn't a
     * need to do that for C# because the mappings of the {@link SymbolHelper} don't apply to enums.
     */
    @Override
    protected void appendExplicitNaming(final String txt, final String prefix) {
        if (!txt.startsWith(prefix + ".")) {
            sb.append(prefix).append(".");
            sb.append(convertToPascalCase(txt));
        } else {
            final String[] split = txt.split("\\.");
            sb.append(split[0]).append(".");
            sb.append(convertToPascalCase(split[1]));
        }
    }

    private void tryAppendCastToString(final GremlinParser.StringArgumentContext ctx) {
        if (ctx.variable() != null || ctx.stringLiteral() != null) {
            sb.append("(string) ");
        }
    }

    private void tryAppendCastToString(final GremlinParser.StringNullableArgumentContext ctx) {
        if (ctx.variable() != null || ctx.stringNullableLiteral().NullLiteral() != null) {
            sb.append("(string) ");
        }
    }

    private void tryAppendCastToString(final GremlinParser.StringNullableLiteralContext ctx) {
        if (ctx.NullLiteral() != null) {
            sb.append("(string) ");
        }
    }

    private void tryAppendCastToObject(final GremlinParser.GenericLiteralArgumentContext ctx) {
        if (ctx.variable() != null || ctx.genericLiteral().nullLiteral() != null)
            sb.append("(object) ");
    }

    private boolean isCalledAsFirstStepInAnonymousTraversal(final ParseTree stepToTest) {
        final ParseTree parent = stepToTest.getParent();
        final ParseTree parentParent = parent.getParent();
        final ParseTree firstStepOfNestedTraversal = parentParent.getChild(0).getChild(0);
        final ParseTree parentParentParent = parentParent.getParent();

        // the step is first if it matches the first step of the nested traversal and if the parent of the parent is
        // a nested traversal
        return stepToTest == firstStepOfNestedTraversal && parentParentParent instanceof GremlinParser.NestedTraversalContext;
    }

    private String convertToPascalCase(final String txt) {
        return txt.substring(0,1).toUpperCase() + txt.substring(1);
    }

    static final class SymbolHelper {

        private final static Map<String, String> TO_CS_MAP = new HashMap<>();
        private final static Map<String, String> FROM_CS_MAP = new HashMap<>();

        static {
            TO_CS_MAP.put("graphml", "GraphML");
            TO_CS_MAP.put("graphson", "GraphSON");
            TO_CS_MAP.forEach((k, v) -> FROM_CS_MAP.put(v, k));
        }

        private SymbolHelper() {
            // static methods only, do not instantiate
        }

        public static String toCSharp(final String symbol) {
            return TO_CS_MAP.getOrDefault(symbol, StringUtils.capitalize(symbol));
        }

        public static String toJava(final String symbol) {
            return FROM_CS_MAP.getOrDefault(symbol, StringUtils.uncapitalize(symbol));
        }

    }
}
