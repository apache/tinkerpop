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

import org.apache.tinkerpop.gremlin.language.grammar.GremlinParser;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.tinkerpop.gremlin.util.DatetimeHelper;

import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Translates Gremlin traversals to Dart source.
 */
public class DartTranslateVisitor extends AbstractTranslateVisitor {

    /**
     * Maps Gremlin step names and enum class names to their Dart equivalents.
     * Step names that clash with Dart reserved words are suffixed with '_'.
     * Enum class prefixes are lowercased to match the Dart record-based enum pattern.
     */
    private static final Map<String, String> TO_DART_MAP = new HashMap<>();
    static {
        TO_DART_MAP.put("and", "and_");
        TO_DART_MAP.put("as", "as_");
        TO_DART_MAP.put("clone", "clone_");
        TO_DART_MAP.put("filter", "filter_");
        TO_DART_MAP.put("format", "format_");
        TO_DART_MAP.put("from", "from_");
        TO_DART_MAP.put("in", "in_");
        TO_DART_MAP.put("is", "is_");
        TO_DART_MAP.put("key", "key_");
        TO_DART_MAP.put("map", "map_");
        TO_DART_MAP.put("match", "match_");
        TO_DART_MAP.put("math", "math_");
        TO_DART_MAP.put("merge", "merge_");
        TO_DART_MAP.put("not", "not_");
        TO_DART_MAP.put("or", "or_");
        TO_DART_MAP.put("value", "value_");
        TO_DART_MAP.put("with", "with_");
        TO_DART_MAP.put("Barrier", "barrier");
        TO_DART_MAP.put("Cardinality", "cardinality");
        TO_DART_MAP.put("Column", "column");
        TO_DART_MAP.put("Direction", "direction");
        TO_DART_MAP.put("DT", "dt");
        TO_DART_MAP.put("GType", "gtype");
        TO_DART_MAP.put("Merge", "merge");
        TO_DART_MAP.put("Operator", "operator_");
        TO_DART_MAP.put("Order", "order");
        TO_DART_MAP.put("Pick", "pick");
        TO_DART_MAP.put("Pop", "pop");
        TO_DART_MAP.put("Scope", "scope");
        TO_DART_MAP.put("T", "t");
        TO_DART_MAP.put("IN", "in_");
        TO_DART_MAP.put("set", "set_");
    }

    public DartTranslateVisitor() {
        super("g");
    }

    public DartTranslateVisitor(final String graphTraversalSourceName) {
        super(graphTraversalSourceName);
    }

    @Override
    protected String processGremlinSymbol(final String step) {
        return TO_DART_MAP.getOrDefault(step, step);
    }

    @Override
    protected void appendAnonymousSpawn() {
        sb.append("Anon.");
    }

    @Override
    public Void visitNestedTraversal(final GremlinParser.NestedTraversalContext ctx) {
        appendAnonymousSpawn();
        return visit(ctx.chainedTraversal());
    }

    @Override
    public Void visitIntegerLiteral(final GremlinParser.IntegerLiteralContext ctx) {
        final String literal = ctx.getText().toLowerCase();
        final int lastCharIndex = literal.length() - 1;
        final char suffix = literal.charAt(lastCharIndex);
        final String value = Character.isAlphabetic(suffix) ? literal.substring(0, lastCharIndex) : literal;

        switch (suffix) {
            case 'b':
                sb.append("GByte(").append(value).append(")");
                return null;
            case 's':
                sb.append("GShort(").append(value).append(")");
                return null;
            case 'l':
                sb.append("GLong(").append(value).append(")");
                return null;
            case 'n':
                sb.append("BigInt.parse('").append(value).append("')");
                return null;
            case 'i':
            default:
                sb.append("GInt(").append(value).append(")");
                return null;
        }
    }

    @Override
    public Void visitFloatLiteral(final GremlinParser.FloatLiteralContext ctx) {
        if (ctx.infLiteral() != null) return visit(ctx.infLiteral());
        if (ctx.nanLiteral() != null) return visit(ctx.nanLiteral());

        final String literal = ctx.getText().toLowerCase();
        final int lastCharIndex = literal.length() - 1;
        final char suffix = literal.charAt(lastCharIndex);
        final String value = Character.isAlphabetic(suffix) ? literal.substring(0, lastCharIndex) : literal;

        sb.append(suffix == 'f' ? "GFloat(" : "GDouble(");
        sb.append(value).append(")");
        return null;
    }

    @Override
    public Void visitDateLiteral(final GremlinParser.DateLiteralContext ctx) {
        final String dtString = ctx.getChild(2).getText();
        final OffsetDateTime dt = DatetimeHelper.parse(removeFirstAndLastCharacters(dtString));
        sb.append("DateTime.parse('").append(dt).append("')");
        return null;
    }

    @Override
    public Void visitNanLiteral(final GremlinParser.NanLiteralContext ctx) {
        sb.append("double.nan");
        return null;
    }

    @Override
    public Void visitInfLiteral(final GremlinParser.InfLiteralContext ctx) {
        if (ctx.SignedInfLiteral() != null && ctx.SignedInfLiteral().getText().equals("-Infinity"))
            sb.append("double.negativeInfinity");
        else
            sb.append("double.infinity");
        return null;
    }

    @Override
    public Void visitUuidLiteral(final GremlinParser.UuidLiteralContext ctx) {
        if (ctx.stringLiteral() == null) {
            // UUID() with no argument is a random UUID, as in the other translators
            sb.append("UuidValue.fromString(Uuid().v4())");
            return null;
        }
        sb.append("UuidValue.fromString(");
        visitStringLiteral(ctx.stringLiteral());
        sb.append(")");
        return null;
    }

    @Override
    public Void visitCharacterLiteral(final GremlinParser.CharacterLiteralContext ctx) {
        final String text = ctx.getText();
        final String literal = removeFirstAndLastCharacters(text.substring(0, text.length() - 1));
        sb.append("GChar('");
        appendDartEscaped(sb, decodeGremlinEscapes(literal));
        sb.append("'.runes.single)");
        return null;
    }

    @Override
    public Void visitDurationLiteral(final GremlinParser.DurationLiteralContext ctx) {
        final String[] parts = ctx.getText()
                .replace("Duration(", "")
                .replace(")", "")
                .split(",");
        sb.append("Duration(seconds: ").append(parts[0].trim());
        if (parts.length > 1) {
            sb.append(", microseconds: ").append(parts[1].trim()).append(" ~/ 1000");
        }
        sb.append(")");
        return null;
    }

    @Override
    public Void visitBinaryLiteral(final GremlinParser.BinaryLiteralContext ctx) {
        sb.append("base64Decode(");
        visitStringLiteral(ctx.stringLiteral());
        sb.append(")");
        return null;
    }

    @Override
    public Void visitGenericCollectionLiteral(final GremlinParser.GenericCollectionLiteralContext ctx) {
        sb.append("[");
        for (int i = 0; i < ctx.genericLiteral().size(); i++) {
            visit(ctx.genericLiteral(i));
            if (i < ctx.genericLiteral().size() - 1) appendArgumentSeparator();
        }
        sb.append("]");
        return null;
    }

    @Override
    public Void visitGenericSetLiteral(final GremlinParser.GenericSetLiteralContext ctx) {
        sb.append("<dynamic>{");
        for (int i = 0; i < ctx.genericLiteral().size(); i++) {
            visit(ctx.genericLiteral(i));
            if (i < ctx.genericLiteral().size() - 1) appendArgumentSeparator();
        }
        sb.append("}");
        return null;
    }

    @Override
    public Void visitGenericMapLiteral(final GremlinParser.GenericMapLiteralContext ctx) {
        sb.append("{");
        for (int i = 0; i < ctx.mapEntry().size(); i++) {
            visit(ctx.mapEntry(i));
            if (i < ctx.mapEntry().size() - 1) appendArgumentSeparator();
        }
        sb.append("}");
        return null;
    }

    @Override
    public Void visitMapEntry(final GremlinParser.MapEntryContext ctx) {
        visit(ctx.mapKey());
        sb.append(": ");
        visit(ctx.genericLiteral());
        return null;
    }

    @Override
    public Void visitTraversalStrategy(final GremlinParser.TraversalStrategyContext ctx) {
        // "new SubgraphStrategy(vertices: ...)" and the bare "ReadOnlyStrategy" both become Dart constructor calls.
        final String strategyName = ctx.getChild(0).getText().equals("new") ? ctx.getChild(1).getText() : ctx.getChild(0).getText();
        if (strategyName.equals("OptionsStrategy")) {
            // OptionsStrategy accepts arbitrary keys, so it is built from a map rather than named arguments
            sb.append("OptionsStrategy({");
            final List<ParseTree> options = ctx.children.stream().
                    filter(c -> c instanceof GremlinParser.ConfigurationContext).collect(Collectors.toList());
            for (int ix = 0; ix < options.size(); ix++) {
                String key = options.get(ix).getChild(0).getText();
                if (key.length() > 1 && (key.startsWith("\"") || key.startsWith("'"))) key = key.substring(1, key.length() - 1);
                sb.append("'").append(key.replace("\\", "\\\\").replace("'", "\\'").replace("$", "\\$")).append("': ");
                visit(options.get(ix).getChild(2));
                if (ix < options.size() - 1) appendArgumentSeparator();
            }
            sb.append("})");
            return null;
        }

        if (ctx.getChildCount() == 1) {
            sb.append(ctx.getText()).append("()");
            return null;
        }

        sb.append(ctx.getChild(0).getText().equals("new") ? ctx.getChild(1).getText() : ctx.getChild(0).getText()).append("(");
        final List<ParseTree> configs = ctx.children.stream().
                filter(c -> c instanceof GremlinParser.ConfigurationContext).collect(Collectors.toList());
        for (int ix = 0; ix < configs.size(); ix++) {
            visit(configs.get(ix));
            if (ix < configs.size() - 1) appendArgumentSeparator();
        }
        sb.append(")");
        return null;
    }

    @Override
    public Void visitConfiguration(final GremlinParser.ConfigurationContext ctx) {
        // key:value becomes the Dart named argument key: value
        String key = ctx.getChild(0).getText();
        if (key.length() > 1 && (key.startsWith("\"") || key.startsWith("'"))) key = key.substring(1, key.length() - 1);
        if (!key.matches("[A-Za-z_$][A-Za-z0-9_$]*") || isDartKeyword(key)) {
            throw new IllegalArgumentException("Strategy configuration key is not a Dart named argument: " + key);
        }
        sb.append(key).append(": ");
        visit(ctx.getChild(2));
        return null;
    }

    private boolean isDartKeyword(final String value) {
        return value.equals("abstract") || value.equals("as") || value.equals("assert") ||
                value.equals("async") || value.equals("await") || value.equals("break") ||
                value.equals("case") || value.equals("catch") || value.equals("class") ||
                value.equals("const") || value.equals("continue") || value.equals("covariant") ||
                value.equals("default") || value.equals("deferred") || value.equals("do") ||
                value.equals("dynamic") || value.equals("else") || value.equals("enum") ||
                value.equals("export") || value.equals("extends") || value.equals("extension") ||
                value.equals("external") || value.equals("factory") || value.equals("false") ||
                value.equals("final") || value.equals("finally") || value.equals("for") ||
                value.equals("Function") || value.equals("get") || value.equals("hide") ||
                value.equals("if") || value.equals("implements") || value.equals("import") ||
                value.equals("in") || value.equals("interface") || value.equals("is") ||
                value.equals("late") || value.equals("library") || value.equals("mixin") ||
                value.equals("new") || value.equals("null") || value.equals("on") ||
                value.equals("operator") || value.equals("part") || value.equals("required") ||
                value.equals("rethrow") || value.equals("return") || value.equals("set") ||
                value.equals("show") || value.equals("static") || value.equals("super") ||
                value.equals("switch") || value.equals("sync") || value.equals("this") ||
                value.equals("throw") || value.equals("true") || value.equals("try") ||
                value.equals("typedef") || value.equals("var") || value.equals("void") ||
                value.equals("while") || value.equals("with") || value.equals("yield");
    }

    @Override
    public Void visitNullLiteral(final GremlinParser.NullLiteralContext ctx) {
        sb.append("null");
        return null;
    }

    @Override
    protected void handleStringLiteralText(final String text) {
        sb.append("'");
        appendDartEscaped(sb, decodeGremlinEscapes(text));
        sb.append("'");
    }

    /**
     * Decodes Gremlin's string/character escape sequences (single-char escapes, octal, unicode, and
     * the line-join escape) into the actual characters they represent. The raw ANTLR token text still
     * holds these escapes unevaluated, so this must run before re-escaping for Dart: Gremlin's escapes
     * and Dart's are not byte-for-byte identical (Dart has no octal escape, and '$' needs escaping in
     * Dart but is not a Gremlin escape character at all).
     */
    private static String decodeGremlinEscapes(final String text) {
        final StringBuilder decoded = new StringBuilder();
        final int len = text.length();
        int i = 0;
        while (i < len) {
            final char c = text.charAt(i);
            if (c != '\\') {
                decoded.append(c);
                i++;
                continue;
            }

            // A backslash always starts a recognized escape in grammar-valid input.
            if (i + 1 >= len) {
                decoded.append(c);
                i++;
                continue;
            }
            final char next = text.charAt(i + 1);

            // Line-join escape ('\' '\r'? '\n'): contributes nothing to the string's content.
            if (next == '\r' || next == '\n') {
                int j2 = i + 1;
                if (text.charAt(j2) == '\r' && j2 + 1 < len && text.charAt(j2 + 1) == '\n') j2++;
                i = j2 + 1;
                continue;
            }

            switch (next) {
                case 'b': decoded.append('\b'); i += 2; continue;
                case 't': decoded.append('\t'); i += 2; continue;
                case 'n': decoded.append('\n'); i += 2; continue;
                case 'f': decoded.append('\f'); i += 2; continue;
                case 'r': decoded.append('\r'); i += 2; continue;
                case '"': decoded.append('"'); i += 2; continue;
                case '\'': decoded.append('\''); i += 2; continue;
                case '\\': decoded.append('\\'); i += 2; continue;
                default: break;
            }

            if (next == 'u') {
                // backslash, one or more 'u', then exactly four hex digits
                int j2 = i + 1;
                while (j2 < len && text.charAt(j2) == 'u') j2++;
                final String hex = text.substring(j2, Math.min(j2 + 4, len));
                decoded.append((char) Integer.parseInt(hex, 16));
                i = j2 + 4;
                continue;
            }

            if (next >= '0' && next <= '7') {
                // Octal escape: up to 3 digits (only when the first is 0-3), else up to 2.
                final int maxDigits = (next <= '3') ? 3 : 2;
                int j2 = i + 1;
                int end = j2 + 1;
                while (end < len && end < j2 + maxDigits && isOctalDigit(text.charAt(end))) end++;
                decoded.appendCodePoint(Integer.parseInt(text.substring(j2, end), 8));
                i = end;
                continue;
            }

            // Not reachable for grammar-valid input; pass the backslash through defensively.
            decoded.append(c);
            i++;
        }
        return decoded.toString();
    }

    /** Appends {@code decoded} to {@code target} as a Dart string body (no surrounding quotes). */
    private static void appendDartEscaped(final StringBuilder target, final String decoded) {
        for (int i = 0; i < decoded.length(); i++) {
            final char c = decoded.charAt(i);
            switch (c) {
                case '\\': target.append("\\\\"); break;
                case '\'': target.append("\\'"); break;
                case '$': target.append("\\$"); break;
                case '\n': target.append("\\n"); break;
                case '\r': target.append("\\r"); break;
                case '\t': target.append("\\t"); break;
                case '\b': target.append("\\b"); break;
                case '\f': target.append("\\f"); break;
                default: target.append(c);
            }
        }
    }

    private static boolean isOctalDigit(final char c) {
        return c >= '0' && c <= '7';
    }
}