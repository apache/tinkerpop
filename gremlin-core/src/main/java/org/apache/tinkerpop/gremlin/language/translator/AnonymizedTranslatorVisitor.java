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
import org.apache.tinkerpop.gremlin.language.grammar.GremlinParser;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A translator that anonymizes Gremlin so that arguments that might contain sensitive information are removed.
 */
public class AnonymizedTranslatorVisitor extends TranslateVisitor {

    private final Map<String, Map<Object, String>> simpleNameToObjectCache = new HashMap<>();

    public AnonymizedTranslatorVisitor() {
        this("g");
    }

    public AnonymizedTranslatorVisitor(final String graphTraversalSourceName) {
        super(graphTraversalSourceName);
    }

    /**
     * Anonymizes the given context by replacing the text with a lower case version of the class name and a number
     * that is incremented for each unique value.
     *
     * @param ctx   the context to anonymize
     * @param clazz the class of the context
     * @return null
     */
    protected Void anonymize(final ParserRuleContext ctx, final Class<?> clazz) {
        final String text = ctx.getText();
        final String type = clazz.getSimpleName();
        Map<Object, String> objectToAnonymizedString = simpleNameToObjectCache.get(type);
        if (objectToAnonymizedString != null){
            // this object type has been handled at least once before
            final String innerValue = objectToAnonymizedString.get(text);
            if (innerValue != null){
                sb.append(innerValue);
            } else {
                final String anonymizedValue = type.toLowerCase() + objectToAnonymizedString.size();
                objectToAnonymizedString.put(text, anonymizedValue);
                sb.append(anonymizedValue);
            }
        } else {
            objectToAnonymizedString = new HashMap<>();
            simpleNameToObjectCache.put(type, objectToAnonymizedString);
            final String anonymizedValue = type.toLowerCase() + objectToAnonymizedString.size();
            objectToAnonymizedString.put(text, anonymizedValue);
            sb.append(anonymizedValue);
        }
        return null;
    }

    @Override
    public Void visitGenericLiteralCollection(final GremlinParser.GenericLiteralCollectionContext ctx) {
        return anonymize(ctx, List.class);
    }

    @Override
    public Void visitGenericLiteralMap(final GremlinParser.GenericLiteralMapContext ctx) {
        return anonymize(ctx, Map.class);
    }

    @Override
    public Void visitGenericLiteralMapNullableArgument(final GremlinParser.GenericLiteralMapNullableArgumentContext ctx) {
        return anonymize(ctx, Map.class);
    }

    @Override
    public Void visitStringLiteral(final GremlinParser.StringLiteralContext ctx) {
        return anonymize(ctx, String.class);
    }

    @Override
    public Void visitStringNullableLiteral(final GremlinParser.StringNullableLiteralContext ctx) {
        return anonymize(ctx, String.class);
    }

    @Override
    public Void visitIntegerLiteral(final GremlinParser.IntegerLiteralContext ctx) {
        final String integerLiteral = ctx.getText().toLowerCase();

        // check suffix
        final int lastCharIndex = integerLiteral.length() - 1;
        final char lastCharacter = integerLiteral.charAt(lastCharIndex);
        switch (lastCharacter) {
            case 'b':
                anonymize(ctx, Byte.class);
                break;
            case 's':
                anonymize(ctx, Short.class);
                break;
            case 'i':
                anonymize(ctx, Integer.class);
                break;
            case 'l':
                anonymize(ctx, Long.class);
                break;
            case 'n':
                anonymize(ctx, BigInteger.class);
                break;
            default:
                anonymize(ctx, Number.class);
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
                anonymize(ctx, Float.class);
                break;
            case 'd':
                anonymize(ctx, Double.class);
                break;
            case 'm':
                anonymize(ctx, BigDecimal.class);
                break;
            default:
                anonymize(ctx, Number.class);
                break;
        }
        return null;
    }

    @Override
    public Void visitBooleanLiteral(final GremlinParser.BooleanLiteralContext ctx) {
        return anonymize(ctx, Boolean.class);
    }

    @Override
    public Void visitDateLiteral(final GremlinParser.DateLiteralContext ctx) {
        return anonymize(ctx, Date.class);
    }

    @Override
    public Void visitNullLiteral(final GremlinParser.NullLiteralContext ctx) {
        return anonymize(ctx, Object.class);
    }

    @Override
    public Void visitNanLiteral(final GremlinParser.NanLiteralContext ctx) {
        return anonymize(ctx, Number.class);
    }

    @Override
    public Void visitInfLiteral(final GremlinParser.InfLiteralContext ctx) {
        return anonymize(ctx, Number.class);
    }
}
