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

// Context parameters are typed as `any` because the generated grammar files
// require accepting arbitrary context types at runtime.

import TranslateVisitor from './TranslateVisitor.js';
import { TranslatorException } from './TranslatorException.js';

const GO_PACKAGE_NAME = 'gremlingo.';

const STRATEGY_WITH_MAP_OPTS = new Set([
    'OptionsStrategy',
    'ReferenceElementStrategy', 'ComputerFinalizationStrategy', 'ProfileStrategy',
    'ComputerVerificationStrategy', 'StandardVerificationStrategy', 'VertexProgramRestrictionStrategy',
    'MessagePassingReductionStrategy',
]);

const STRATEGY_WITH_STRING_SLICE = new Set([
    'ReservedKeysVerificationStrategy', 'ProductiveByStrategy',
]);

/**
 * Converts a Gremlin traversal string into a Go source code representation.
 * Assumes use of the gremlingo, math, time, and uuid packages.
 */
export default class GoTranslateVisitor extends TranslateVisitor {

    constructor(graphTraversalSourceName: string = 'g') {
        super(graphTraversalSourceName);
    }

    protected override processGremlinSymbol(step: string): string {
        return SymbolHelper.toGo(step);
    }

    protected override getCardinalityFunctionClass(): string {
        return 'CardinalityValue';
    }

    protected override appendExplicitNaming(txt: string, prefix: string): void {
        this.sb.push(GO_PACKAGE_NAME);
        super.appendExplicitNaming(txt, prefix);
    }

    protected override appendAnonymousSpawn(): void {
        this.sb.push(GO_PACKAGE_NAME + 'T__.');
    }

    protected override visitP(ctx: any, pClass: string, methodName: string): void {
        this.sb.push(GO_PACKAGE_NAME);
        super.visitP(ctx, pClass, methodName);
    }

    visitTraversalGType(ctx: any): void {
        const parts: string[] = ctx.getText().split('.');
        this.sb.push(GO_PACKAGE_NAME);
        this.sb.push(this.processGremlinSymbol(parts[0]));
        this.sb.push('.');
        this.sb.push(this.processGremlinSymbol(parts[1].toLowerCase()));
    }

    visitNanLiteral(_ctx: any): void {
        this.sb.push('math.NaN()');
    }

    visitInfLiteral(ctx: any): void {
        if (ctx.SignedInfLiteral() != null && ctx.SignedInfLiteral().getText() === '-Infinity') {
            this.sb.push('math.Inf(-1)');
        } else {
            this.sb.push('math.Inf(1)');
        }
    }

    visitNullLiteral(_ctx: any): void {
        this.sb.push('nil');
    }

    visitStringNullableLiteral(ctx: any): void {
        if (ctx.getText() === 'null') {
            this.sb.push('nil');
        } else {
            const text = TranslateVisitor.removeFirstAndLastCharacters(ctx.getText());
            this.handleStringLiteralText(text);
        }
    }

    visitIntegerLiteral(ctx: any): void {
        const integerLiteral: string = ctx.getText().toLowerCase();
        const lastChar = integerLiteral[integerLiteral.length - 1];
        const num = integerLiteral.slice(0, -1);

        if (!/[a-z]/.test(lastChar)) {
            this.sb.push(integerLiteral);
            return;
        }

        switch (lastChar) {
            case 'b':
                this.sb.push('int8(');
                this.sb.push(num);
                this.sb.push(')');
                break;
            case 's':
                this.sb.push('int16(');
                this.sb.push(num);
                this.sb.push(')');
                break;
            case 'i':
                this.sb.push('int32(');
                this.sb.push(num);
                this.sb.push(')');
                break;
            case 'l':
                this.sb.push('int64(');
                this.sb.push(num);
                this.sb.push(')');
                break;
            case 'n':
                this.sb.push(GO_PACKAGE_NAME + 'ParseBigInt("');
                this.sb.push(num);
                this.sb.push('")');
                break;
            default:
                this.sb.push(integerLiteral);
        }
    }

    visitFloatLiteral(ctx: any): void {
        if (ctx.infLiteral() != null) { this.visit(ctx.infLiteral()); return; }
        if (ctx.nanLiteral() != null) { this.visit(ctx.nanLiteral()); return; }

        const floatLiteral: string = ctx.getText().toLowerCase();
        const lastChar = floatLiteral[floatLiteral.length - 1];
        const num = floatLiteral.slice(0, -1);

        if (!/[a-z]/.test(lastChar)) {
            this.sb.push(floatLiteral);
            return;
        }

        switch (lastChar) {
            case 'f':
                this.sb.push('float32(');
                this.sb.push(num);
                this.sb.push(')');
                break;
            case 'd':
                this.sb.push(num);
                break;
            case 'm':
                this.sb.push(GO_PACKAGE_NAME + 'ParseBigDecimal("');
                this.sb.push(num);
                this.sb.push('")');
                break;
            default:
                this.sb.push(floatLiteral);
        }
    }

    visitDateLiteral(ctx: any): void {
        const dtString: string = ctx.getChild(2).getText();
        const inner = TranslateVisitor.removeFirstAndLastCharacters(dtString);
        const d = new Date(inner);
        if (isNaN(d.getTime())) {
            throw new TranslatorException(`Invalid datetime: ${inner}`);
        }

        const year = d.getUTCFullYear();
        const month = d.getUTCMonth() + 1;
        const day = d.getUTCDate();
        const hours = d.getUTCHours();
        const minutes = d.getUTCMinutes();
        const seconds = d.getUTCSeconds();
        const nanos = d.getUTCMilliseconds() * 1_000_000;
        const totalSeconds = 0; // assume UTC when no offset specified
        const zoneInfo = 'UTC+00:00';

        this.sb.push(`time.Date(${year}, ${month}, ${day}, ${hours}, ${minutes}, ${seconds}, ${nanos}, time.FixedZone("${zoneInfo}", ${totalSeconds}))`);
    }

    visitGenericRangeLiteral(_ctx: any): void {
        throw new TranslatorException('Go does not support range literals');
    }

    visitGenericSetLiteral(ctx: any): void {
        this.sb.push(GO_PACKAGE_NAME + 'NewSimpleSet(');
        const literals: any[] = ctx.genericLiteral();
        for (let i = 0; i < literals.length; i++) {
            this.visit(literals[i]);
            if (i < literals.length - 1) {
                this.sb.push(', ');
            }
        }
        this.sb.push(')');
    }

    visitGenericCollectionLiteral(ctx: any): void {
        this.sb.push('[]interface{}{');
        const literals: any[] = ctx.genericLiteral();
        for (let i = 0; i < literals.length; i++) {
            this.visit(literals[i]);
            if (i < literals.length - 1) {
                this.sb.push(', ');
            }
        }
        this.sb.push('}');
    }

    visitGenericMapLiteral(ctx: any): void {
        this.sb.push('map[interface{}]interface{}{');
        const entries: any[] = ctx.mapEntry();
        for (let i = 0; i < entries.length; i++) {
            this.visit(entries[i]);
            if (i < entries.length - 1) {
                this.sb.push(', ');
            }
        }
        this.sb.push(' }');
    }

    visitMapEntry(ctx: any): void {
        this.visit(ctx.mapKey());
        this.sb.push(': ');
        this.visit(ctx.genericLiteral());
    }

    visitMapKey(ctx: any): void {
        const keyIndex = (ctx.LPAREN() != null && ctx.RPAREN() != null) ? 1 : 0;
        this.visit(ctx.getChild(keyIndex));
    }

    visitUuidLiteral(ctx: any): void {
        if (ctx.stringLiteral() == null) {
            this.sb.push('uuid.New()');
            return;
        }
        this.sb.push('uuid.MustParse(');
        this.visitStringLiteral(ctx.stringLiteral());
        this.sb.push(')');
    }

    visitTraversalStrategy(ctx: any): void {
        if (ctx.getChildCount() === 1) {
            this.sb.push(GO_PACKAGE_NAME);
            this.sb.push(ctx.getText());
            this.sb.push('()');
            return;
        }

        const firstText = ctx.getChild(0).getText();
        const strategyName = firstText === 'new' ? ctx.getChild(1).getText() : firstText;

        this.sb.push(GO_PACKAGE_NAME);
        this.sb.push(strategyName);
        this.sb.push('(');

        if (!STRATEGY_WITH_MAP_OPTS.has(strategyName)) {
            this.sb.push(GO_PACKAGE_NAME);
            this.sb.push(strategyName);
            this.sb.push('Config{');
        }

        const configs: any[] = ctx.configuration();
        for (let ix = 0; ix < configs.length; ix++) {
            this.visit(configs[ix]);
            if (ix < configs.length - 1) {
                this.sb.push(', ');
            }
        }

        if (strategyName !== 'OptionsStrategy') {
            this.sb.push('}');
        }
        this.sb.push(')');
    }

    visitConfiguration(ctx: any): void {
        const parent: string = ctx.parent.getText();
        const parenIdx = parent.indexOf('(');
        const rawName = parenIdx > -1 ? parent.substring(0, parenIdx) : parent;
        const parentName = rawName.startsWith('new') ? rawName.substring(3) : rawName;

        if (STRATEGY_WITH_MAP_OPTS.has(parentName)) {
            this.sb.push('map[string]interface{}{"');
            this.sb.push(ctx.getChild(0).getText());
            this.sb.push('": ');
            this.visit(ctx.getChild(2));
            this.sb.push('}');
            return;
        }

        const key: string = ctx.getChild(0).getText();
        this.sb.push(SymbolHelper.toGo(key));
        this.sb.push(': ');

        const startIdx = this.sb.length;
        this.visit(ctx.genericArgument());

        if (STRATEGY_WITH_STRING_SLICE.has(parentName)) {
            // Replace []interface{} with []string if present
            const idx = this.sb.indexOf('[]interface{}', startIdx);
            if (idx > -1) {
                this.sb[idx] = '[]string';
            }
        }

        if (key === 'readPartitions') {
            // Replace []interface{}{items} with gremlingo.NewSimpleSet(items)
            const added = this.sb.splice(startIdx);
            const joined = added.join('');
            const transformed = joined.replace(/^\[\]interface\{\}\{([\s\S]*)\}$/, GO_PACKAGE_NAME + 'NewSimpleSet($1)');
            this.sb.push(transformed);
        }
    }

    visitTraversalSourceSelfMethod_withoutStrategies(ctx: any): void {
        this.sb.push('WithoutStrategies(');
        this.sb.push(GO_PACKAGE_NAME);
        this.sb.push(ctx.classType().getText());
        this.sb.push('()');

        if (ctx.classTypeList() != null && ctx.classTypeList().classTypeExpr() != null) {
            for (const classTypeCtx of ctx.classTypeList().classTypeExpr().classType()) {
                this.sb.push(', ');
                this.sb.push(GO_PACKAGE_NAME);
                this.sb.push(classTypeCtx.getText());
                this.sb.push('()');
            }
        }

        this.sb.push(')');
    }
}

class SymbolHelper {
    private static readonly TO_GO_MAP: Record<string, string> = {
        'OUT': 'Out',
        'IN': 'In',
        'BOTH': 'Both',
        'bigdecimal': 'BigDecimal',
        'bigint': 'BigInt',
        'datetime': 'DateTime',
        'uuid': 'UUID',
        'vproperty': 'VProperty',
        'WithOptions': GO_PACKAGE_NAME + 'WithOptions',
        'IO': GO_PACKAGE_NAME + 'IO',
        '__': GO_PACKAGE_NAME + 'T__',
    };

    static toGo(symbol: string): string {
        if (symbol in SymbolHelper.TO_GO_MAP) {
            return SymbolHelper.TO_GO_MAP[symbol];
        }
        // Capitalize first letter (mirrors Java StringUtils.capitalize)
        if (!symbol) return symbol;
        return symbol.charAt(0).toUpperCase() + symbol.slice(1);
    }
}
