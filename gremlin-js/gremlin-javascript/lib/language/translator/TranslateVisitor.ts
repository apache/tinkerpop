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

import { ErrorNode, TerminalNode } from 'antlr4ng';
import { GremlinLexer } from '../grammar/GremlinLexer.js';

/**
 * Base visitor for translating Gremlin queries. Mirrors the Java TranslateVisitor.
 * Makes no changes to input except:
 * - Normalizes whitespace
 * - Normalizes numeric suffixes to lower case
 * - Makes anonymous traversals explicit with double underscore
 * - Makes enums explicit with their proper name
 */
export default class TranslateVisitor {

    /**
     * The names of the `GType` entries that identify a geometry, which includes the model based
     * `CARTESIAN` and `GEOGRAPHIC` entries. Kept in step with the geometry entries of
     * org.apache.tinkerpop.gremlin.process.traversal.GType.
     */
    protected static readonly GEOMETRY_GTYPE_NAMES: ReadonlySet<string> = new Set([
        'CARTESIAN', 'GEOGRAPHIC', 'GEOMETRY', 'GEOMETRYCOLLECTION', 'LINESTRING',
        'MULTILINESTRING', 'MULTIPOINT', 'MULTIPOLYGON', 'POINT', 'POLYGON',
    ]);

    protected readonly graphTraversalSourceName: string;
    protected readonly sb: string[] = [];
    protected readonly parameters: Set<string> = new Set();

    constructor(graphTraversalSourceName: string = 'g') {
        this.graphTraversalSourceName = graphTraversalSourceName;
    }

    getTranslated(): string {
        return this.sb.join('');
    }

    getParameters(): ReadonlySet<string> {
        return this.parameters;
    }

    protected processGremlinSymbol(step: string): string {
        return step;
    }

    protected appendArgumentSeparator(): void {
        this.sb.push(', ');
    }

    protected appendStepSeparator(): void {
        this.sb.push('.');
    }

    protected appendStepOpen(): void {
        this.sb.push('(');
    }

    protected appendStepClose(): void {
        this.sb.push(')');
    }

    protected appendAnonymousSpawn(): void {
        this.sb.push('__.');
    }

    visit(tree: any): void {
        if (tree == null) return;

        // antlr-ng does not generate accept() overrides per context class —
        // ParserRuleContext.accept() always calls visitChildren, never dispatching to
        // the specific visitXxx method. We perform the dispatch ourselves here by
        // mapping the context class name (e.g. StringLiteralContext) to the
        // corresponding visitor method (e.g. visitStringLiteral).
        const className: string = tree?.constructor?.name ?? '';
        if (className.endsWith('Context')) {
            const methodName = 'visit' + className.slice(0, -'Context'.length);
            const method = (this as any)[methodName];
            if (typeof method === 'function') {
                method.call(this, tree);
                return;
            }
        }

        // No specific visitor method found — fall through to accept() which calls
        // visitChildren for rule contexts, visitTerminal for terminals.
        (tree as any).accept(this);
    }

    visitChildren(node: any): void {
        const n = node.getChildCount();
        for (let i = 0; i < n; i++) {
            this.visit(node.getChild(i));
        }
    }

    protected getCardinalityFunctionClass(): string {
        return 'Cardinality';
    }

    protected static removeFirstAndLastCharacters(text: string): string {
        return text != null && text.length > 0 ? text.substring(1, text.length - 1) : '';
    }

    protected handleStringLiteralText(text: string): void {
        this.sb.push('"');
        this.sb.push(text);
        this.sb.push('"');
    }

    protected appendExplicitNaming(txt: string, prefix: string): void {
        if (!txt.startsWith(prefix + '.')) {
            this.sb.push(this.processGremlinSymbol(prefix));
            this.sb.push('.');
            this.sb.push(this.processGremlinSymbol(txt));
        } else {
            const parts = txt.split('.');
            this.sb.push(this.processGremlinSymbol(parts[0]));
            this.sb.push('.');
            this.sb.push(this.processGremlinSymbol(parts[1]));
        }
    }

    visitTerminal(node: TerminalNode): void {
        // skip EOF node
        if (node == null || node.getSymbol().type === -1) return;

        // TRAVERSAL_ROOT is the traversal source token (e.g. 'g') — always output as-is,
        // never subject to processGremlinSymbol (which may capitalize it in some dialects).
        if (node.getSymbol().type === GremlinLexer.TRAVERSAL_ROOT) {
            this.sb.push(this.graphTraversalSourceName);
            return;
        }

        const terminal = node.getSymbol().text ?? '';
        switch (terminal) {
            case '(':
                this.appendStepOpen();
                break;
            case ')':
                this.appendStepClose();
                break;
            case ',':
                this.appendArgumentSeparator();
                break;
            case '.':
                this.appendStepSeparator();
                break;
            case 'new':
                this.sb.push('new');
                if (!(node.parent?.constructor?.name === 'MapKeyContext')) {
                    this.sb.push(' ');
                }
                break;
            default:
                this.sb.push(this.processGremlinSymbol(terminal));
        }
    }

    visitErrorNode(_node: ErrorNode): void {
        // no-op
    }

    visitTraversalSource(ctx: any): void {
        this.sb.push(this.graphTraversalSourceName);
        // child counts more than 1 means there is a step separator and a traversal source method
        if (ctx.getChildCount() > 1) {
            if (ctx.getChild(0).getChildCount() > 1) {
                this.appendStepSeparator();
                this.visitTraversalSourceSelfMethod(ctx.getChild(0).getChild(2));
            }
            this.appendStepSeparator();
            this.visitTraversalSourceSelfMethod(ctx.getChild(2));
        }
    }

    visitTraversalSourceSelfMethod(ctx: any): void {
        this.visitChildren(ctx);
    }

    visitNestedTraversal(ctx: any): void {
        if (ctx.ANON_TRAVERSAL_ROOT() == null) {
            this.appendAnonymousSpawn();
        }
        this.visitChildren(ctx);
    }

    visitTraversalScope(ctx: any): void {
        this.appendExplicitNaming(ctx.getText(), 'Scope');
    }

    visitTraversalT(ctx: any): void {
        this.appendExplicitNaming(ctx.getText(), 'T');
    }

    visitTraversalTShort(ctx: any): void {
        this.appendExplicitNaming(ctx.getText(), 'T');
    }

    visitTraversalTLong(ctx: any): void {
        this.appendExplicitNaming(ctx.getText(), 'T');
    }

    visitTraversalMerge(ctx: any): void {
        this.appendExplicitNaming(ctx.getText(), 'Merge');
    }

    visitTraversalOrder(ctx: any): void {
        this.appendExplicitNaming(ctx.getText(), 'Order');
    }

    visitTraversalBarrier(ctx: any): void {
        this.appendExplicitNaming(ctx.getText(), 'Barrier');
    }

    visitTraversalDirection(ctx: any): void {
        this.appendExplicitNaming(ctx.getText(), 'Direction');
    }

    visitTraversalDirectionShort(ctx: any): void {
        this.appendExplicitNaming(ctx.getText(), 'Direction');
    }

    visitTraversalDirectionLong(ctx: any): void {
        this.appendExplicitNaming(ctx.getText(), 'Direction');
    }

    visitTraversalCardinality(ctx: any): void {
        if (ctx.LPAREN() != null && ctx.RPAREN() != null) {
            const idx = ctx.K_CARDINALITY() != null ? 2 : 0;
            const txt = ctx.getChild(idx).getText();
            this.appendExplicitNaming(txt, this.getCardinalityFunctionClass());
            this.appendStepOpen();
            this.visit(ctx.genericLiteral());
            this.appendStepClose();
        } else {
            this.appendExplicitNaming(ctx.getText(), 'Cardinality');
        }
    }

    visitTraversalColumn(ctx: any): void {
        this.appendExplicitNaming(ctx.getText(), 'Column');
    }

    visitTraversalPop(ctx: any): void {
        this.appendExplicitNaming(ctx.getText(), 'Pop');
    }

    visitTraversalOperator(ctx: any): void {
        this.appendExplicitNaming(ctx.getText(), 'Operator');
    }

    visitTraversalPick(ctx: any): void {
        this.appendExplicitNaming(ctx.getText(), 'Pick');
    }

    visitTraversalDT(ctx: any): void {
        this.appendExplicitNaming(ctx.getText(), 'DT');
    }

    visitTraversalGType(ctx: any): void {
        this.appendExplicitNaming(ctx.getText(), 'GType');
    }

    /**
     * Extracts the name of the type from a `GType` which may be written with or without the `GType`
     * prefix and in either the camel case or the upper case form, as in `lineString` and `LINESTRING`.
     */
    protected static extractGTypeName(ctx: any): string {
        const split: string[] = ctx.getText().split('.');
        return split[split.length - 1];
    }

    /**
     * Determines if a `GType` names a geometry type, which the translators that have no geometry support use
     * to reject the type.
     */
    protected static isGeometryGType(ctx: any): boolean {
        return TranslateVisitor.GEOMETRY_GTYPE_NAMES.has(TranslateVisitor.extractGTypeName(ctx).toUpperCase());
    }

    /**
     * Appends `Geometry.fromWkt("...")`, the form the JVM languages use since they have a WKT reader, with
     * `Model.GEOGRAPHIC` or the SRID as a second argument where the geometry carries one. The WKT is written
     * as the Gremlin spells it, since this translator has no WKT reader of its own to normalize it through.
     * WKT holds no quote or backslash, so the text needs no escaping.
     */
    protected appendGeometryFromWkt(ctx: any): void {
        this.sb.push('Geometry.fromWkt("');
        this.sb.push(TranslateVisitor.removeFirstAndLastCharacters(ctx.stringLiteral().getText()));
        this.sb.push('"');

        const modelCtx = ctx.traversalModel();
        if (modelCtx != null) {
            const modelName: string = modelCtx.getText().split('.').pop().toUpperCase();
            // CARTESIAN is the default model and so goes unwritten
            if (modelName === 'GEOGRAPHIC') {
                this.sb.push(', Model.GEOGRAPHIC');
            }
        } else if (ctx.integerLiteral() != null) {
            const srid = parseSridLiteral(ctx.integerLiteral().getText());
            // SRID 0 is CARTESIAN and SRID 4326 is GEOGRAPHIC, so those name their model instead
            if (srid === GEOGRAPHIC_SRID) {
                this.sb.push(', Model.GEOGRAPHIC');
            } else if (srid !== CARTESIAN_SRID) {
                this.sb.push(', ');
                this.sb.push(String(srid));
            }
        }

        this.sb.push(')');
    }

    visitTraversalPredicate(ctx: any): void {
        switch (ctx.getChildCount()) {
            case 1:
                this.visit(ctx.getChild(0));
                break;
            case 5:
                // handle negate of P
                this.visit(ctx.getChild(0));
                this.sb.push('.');
                this.sb.push(this.processGremlinSymbol('negate'));
                this.sb.push('()');
                break;
            case 6:
                // handle and/or predicates
                this.visit(ctx.getChild(0));
                this.sb.push('.');
                this.sb.push(this.processGremlinSymbol(ctx.getChild(2).getText()));
                this.sb.push('(');
                this.visit(ctx.getChild(4));
                this.sb.push(')');
                break;
        }
    }

    protected visitP(ctx: any, pClass: string, methodName: string): void {
        this.sb.push(pClass);
        this.appendStepSeparator();
        this.sb.push(this.processGremlinSymbol(methodName));
        this.appendStepOpen();

        const children: any[] = [];
        for (let i = 0; i < ctx.getChildCount(); i++) {
            const child = ctx.getChild(i);
            const name = child.constructor?.name ?? '';
            if (name === 'GenericArgumentContext' ||
                name === 'GenericArgumentVarargsContext' ||
                name === 'StringArgumentContext' ||
                name === 'StringLiteralContext' ||
                name === 'GeometryArgumentContext' ||
                name === 'NumericArgumentContext' ||
                name === 'NestedTraversalContext' ||
                name === 'TraversalGTypeContext' ||
                name === 'TraversalPredicateContext') {
                children.push(child);
            }
        }

        for (let ix = 0; ix < children.length; ix++) {
            this.visit(children[ix]);
            if (ix < children.length - 1) {
                this.appendArgumentSeparator();
            }
        }

        this.appendStepClose();
    }

    visitTraversalPredicate_eq(ctx: any): void { this.visitP(ctx, 'P', 'eq'); }
    visitTraversalPredicate_neq(ctx: any): void { this.visitP(ctx, 'P', 'neq'); }
    visitTraversalPredicate_lt(ctx: any): void { this.visitP(ctx, 'P', 'lt'); }
    visitTraversalPredicate_lte(ctx: any): void { this.visitP(ctx, 'P', 'lte'); }
    visitTraversalPredicate_gt(ctx: any): void { this.visitP(ctx, 'P', 'gt'); }
    visitTraversalPredicate_gte(ctx: any): void { this.visitP(ctx, 'P', 'gte'); }
    visitTraversalPredicate_inside(ctx: any): void { this.visitP(ctx, 'P', 'inside'); }
    visitTraversalPredicate_outside(ctx: any): void { this.visitP(ctx, 'P', 'outside'); }
    visitTraversalPredicate_between(ctx: any): void { this.visitP(ctx, 'P', 'between'); }
    visitTraversalPredicate_within(ctx: any): void { this.visitP(ctx, 'P', 'within'); }
    visitTraversalPredicate_without(ctx: any): void { this.visitP(ctx, 'P', 'without'); }
    visitTraversalPredicate_typeOf(ctx: any): void { this.visitP(ctx, 'P', 'typeOf'); }
    visitTraversalPredicate_not(ctx: any): void { this.visitP(ctx, 'P', 'not'); }
    visitTraversalPredicate_containing(ctx: any): void { this.visitP(ctx, 'TextP', 'containing'); }
    visitTraversalPredicate_notContaining(ctx: any): void { this.visitP(ctx, 'TextP', 'notContaining'); }
    visitTraversalPredicate_startingWith(ctx: any): void { this.visitP(ctx, 'TextP', 'startingWith'); }
    visitTraversalPredicate_notStartingWith(ctx: any): void { this.visitP(ctx, 'TextP', 'notStartingWith'); }
    visitTraversalPredicate_endingWith(ctx: any): void { this.visitP(ctx, 'TextP', 'endingWith'); }
    visitTraversalPredicate_notEndingWith(ctx: any): void { this.visitP(ctx, 'TextP', 'notEndingWith'); }
    visitTraversalPredicate_regex(ctx: any): void { this.visitP(ctx, 'TextP', 'regex'); }
    visitTraversalPredicate_notRegex(ctx: any): void { this.visitP(ctx, 'TextP', 'notRegex'); }
    visitTraversalPredicate_geoWithin(ctx: any): void { this.visitP(ctx, 'GeoP', 'geoWithin'); }
    visitTraversalPredicate_geoContains(ctx: any): void { this.visitP(ctx, 'GeoP', 'geoContains'); }
    visitTraversalPredicate_geoWithinDistance(ctx: any): void { this.visitP(ctx, 'GeoP', 'geoWithinDistance'); }

    visitBooleanArgument(ctx: any): void {
        if (ctx.booleanLiteral() != null) {
            this.visitBooleanLiteral(ctx.booleanLiteral());
        } else {
            this.visitVariable(ctx.variable());
        }
    }

    visitGenericArgument(ctx: any): void {
        if (ctx.genericLiteral() != null) {
            this.visitGenericLiteral(ctx.genericLiteral());
        } else {
            this.visitVariable(ctx.variable());
        }
    }

    visitGeometryArgument(ctx: any): void {
        if (ctx.geometryLiteral() != null) {
            this.visitGeometryLiteral(ctx.geometryLiteral());
        } else {
            this.visitVariable(ctx.variable());
        }
    }

    visitGenericLiteral(ctx: any): void {
        this.visitChildren(ctx);
    }

    visitIntegerLiteral(ctx: any): void {
        this.sb.push(ctx.getText().toLowerCase());
    }

    visitFloatLiteral(ctx: any): void {
        if (ctx.infLiteral() != null) { this.visit(ctx.infLiteral()); return; }
        if (ctx.nanLiteral() != null) { this.visit(ctx.nanLiteral()); return; }
        this.sb.push(ctx.getText().toLowerCase());
    }

    visitBooleanLiteral(ctx: any): void {
        this.sb.push(ctx.getText());
    }

    visitNullLiteral(ctx: any): void {
        this.sb.push(ctx.getText());
    }

    visitNanLiteral(ctx: any): void {
        this.sb.push(ctx.getText());
    }

    visitInfLiteral(ctx: any): void {
        this.sb.push(ctx.getText());
    }

    visitUuidLiteral(ctx: any): void {
        this.sb.push(ctx.getText());
    }

    visitCharacterLiteral(ctx: any): void {
        this.sb.push(ctx.getText());
    }

    visitDurationLiteral(ctx: any): void {
        this.sb.push(ctx.getText());
    }

    visitBinaryLiteral(ctx: any): void {
        this.sb.push(ctx.getText());
    }

    visitGeometryLiteral(ctx: any): void {
        // the keyword and the arguments are echoed as written, which keeps the "Geometry" and "geometry"
        // spellings apart, while the arguments are separated the way every other argument list is
        this.sb.push(ctx.getChild(0).getText());
        this.appendStepOpen();
        this.sb.push(ctx.stringLiteral().getText());
        if (ctx.COMMA() != null) {
            this.appendArgumentSeparator();
            this.sb.push(ctx.traversalModel() != null ? ctx.traversalModel().getText() : ctx.integerLiteral().getText());
        }
        this.appendStepClose();
    }

    visitStringLiteral(ctx: any): void {
        const text = TranslateVisitor.removeFirstAndLastCharacters(ctx.getText());
        this.handleStringLiteralText(text);
    }

    visitStringNullableLiteral(ctx: any): void {
        if (ctx.getText() === 'null') {
            this.sb.push('null');
        } else {
            const text = TranslateVisitor.removeFirstAndLastCharacters(ctx.getText());
            this.handleStringLiteralText(text);
        }
    }

    visitNakedKey(ctx: any): void {
        this.handleStringLiteralText(ctx.getText());
    }

    visitVariable(ctx: any): void {
        const varName: string = ctx.getText();
        this.sb.push(varName);
        this.parameters.add(varName);
    }

    visitKeyword(ctx: any): void {
        const keyword: string = ctx.getText();
        if (ctx.parent?.constructor?.name === 'MapKeyContext' ||
            ctx.parent?.constructor?.name === 'ConfigurationContext') {
            this.handleStringLiteralText(keyword);
        } else {
            this.sb.push(keyword);
            this.sb.push(' ');
        }
    }

    visitMapKey(ctx: any): void {
        const keyIndex = (ctx.LPAREN() != null && ctx.RPAREN() != null) ? 1 : 0;
        this.visit(ctx.getChild(keyIndex));
    }

    visitMapEntry(ctx: any): void {
        this.visitChildren(ctx);
    }
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

/** The SRID of the CARTESIAN model. */
const CARTESIAN_SRID = 0;

/** The SRID of the GEOGRAPHIC model, which is WGS84. */
const GEOGRAPHIC_SRID = 4326;

/**
 * Reads the SRID of a geometry literal, which the grammar writes in the decimal, hexadecimal or octal
 * form, with underscore separators and an optional integer type suffix.
 */
function parseSridLiteral(text: string): number {
    let digits = text.replace(/_/g, '');

    let sign = 1;
    if (digits.startsWith('-')) {
        sign = -1;
        digits = digits.slice(1);
    } else if (digits.startsWith('+')) {
        digits = digits.slice(1);
    }

    // a hexadecimal literal keeps a trailing b or s, which the lexer reads as a digit rather than a suffix
    if (/^0[xX]/.test(digits)) {
        return sign * parseInt(digits.slice(2).replace(/[ilnILN]$/, ''), 16);
    }

    digits = digits.replace(/[bsilnBSILN]$/, '');
    if (/^0[0-7]+$/.test(digits)) {
        return sign * parseInt(digits.slice(1), 8);
    }
    return sign * parseInt(digits, 10);
}
