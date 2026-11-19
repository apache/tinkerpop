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
package org.apache.tinkerpop.gremlin.language.translator;

import org.antlr.v4.runtime.ParserRuleContext;
import org.apache.tinkerpop.gremlin.language.antlr.GremlinParser;
import org.apache.tinkerpop.gremlin.structure.geo.Geometry;
import org.apache.tinkerpop.gremlin.structure.geo.GeometryCollection;
import org.apache.tinkerpop.gremlin.structure.geo.LineString;
import org.apache.tinkerpop.gremlin.structure.geo.MultiLineString;
import org.apache.tinkerpop.gremlin.structure.geo.MultiPoint;
import org.apache.tinkerpop.gremlin.structure.geo.MultiPolygon;
import org.apache.tinkerpop.gremlin.structure.geo.Point;
import org.apache.tinkerpop.gremlin.structure.geo.Polygon;
import org.apache.tinkerpop.gremlin.structure.io.pdt.PrimitivePDT;
import org.apache.tinkerpop.gremlin.structure.io.pdt.CompositePDT;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A translator that anonymizes Gremlin so that arguments that might contain sensitive information are removed.
 */
public class AnonymizedTranslatorVisitor extends TranslateVisitor {

    /**
     * The geometry classes named by the WKT type keywords.
     */
    private static final Map<String, Class<? extends Geometry>> GEOMETRY_KINDS = Map.of(
            "POINT", Point.class,
            "LINESTRING", LineString.class,
            "POLYGON", Polygon.class,
            "MULTIPOINT", MultiPoint.class,
            "MULTILINESTRING", MultiLineString.class,
            "MULTIPOLYGON", MultiPolygon.class,
            "GEOMETRYCOLLECTION", GeometryCollection.class);

    private static final Pattern EWKT_PREFIX = Pattern.compile("^srid=[^;]*;", Pattern.CASE_INSENSITIVE);

    private static final Pattern GEOMETRY_KEYWORD = Pattern.compile("[a-z]+", Pattern.CASE_INSENSITIVE);

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
    public Void visitGenericSetLiteral(final GremlinParser.GenericSetLiteralContext ctx) {
        return anonymize(ctx, Set.class);
    }

    @Override
    public Void visitGenericCollectionLiteral(final GremlinParser.GenericCollectionLiteralContext ctx) {
        return anonymize(ctx, List.class);
    }

    @Override
    public Void visitGenericMapLiteral(final GremlinParser.GenericMapLiteralContext ctx) {
        return anonymize(ctx, Map.class);
    }

    @Override
    public Void visitGenericMapNullableArgument(final GremlinParser.GenericMapNullableArgumentContext ctx) {
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
        return anonymize(ctx, OffsetDateTime.class);
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

    @Override
    public Void visitUuidLiteral(final GremlinParser.UuidLiteralContext ctx) { return anonymize(ctx, String.class); }

    @Override
    public Void visitCharacterLiteral(final GremlinParser.CharacterLiteralContext ctx) {
        return anonymize(ctx, Character.class);
    }

    @Override
    public Void visitDurationLiteral(final GremlinParser.DurationLiteralContext ctx) {
        return anonymize(ctx, Duration.class);
    }

    @Override
    public Void visitBinaryLiteral(final GremlinParser.BinaryLiteralContext ctx) {
        return anonymize(ctx, ByteBuffer.class);
    }

    /**
     * {@inheritDoc}
     * <p>
     * The type keyword that leads the WKT, after an optional {@code SRID=} prefix, names the value, so a literal
     * anonymizes by its kind (for example {@code point0}) whether or not the rest of it is valid. A keyword that
     * names none of the kinds falls back to the general geometry name.
     */
    @Override
    public Void visitGeometryLiteral(final GremlinParser.GeometryLiteralContext ctx) {
        final String text = (String) literalVisitor.visitStringLiteral(ctx.stringLiteral());
        final Matcher keyword = GEOMETRY_KEYWORD.matcher(EWKT_PREFIX.matcher(text.trim()).replaceFirst("").trim());
        return anonymize(ctx, keyword.lookingAt()
                ? GEOMETRY_KINDS.getOrDefault(keyword.group().toUpperCase(Locale.ROOT), Geometry.class)
                : Geometry.class);
    }

    @Override
    public Void visitPdtLiteral(final GremlinParser.PdtLiteralContext ctx) {
        if (ctx.genericMapLiteral() != null) {
            return anonymize(ctx, CompositePDT.class);
        } else {
            return anonymize(ctx, PrimitivePDT.class);
        }
    }
}
