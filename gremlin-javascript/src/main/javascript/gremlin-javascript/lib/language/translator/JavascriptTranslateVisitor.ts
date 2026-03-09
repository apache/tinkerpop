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
// use @ts-nocheck (the standard antlr4 npm package doesn't fully re-export
// its TypeScript types from the main module entry point).

import TranslateVisitor from './TranslateVisitor.js';
import { TranslatorException } from './TranslatorException.js';

/**
 * Converts a Gremlin traversal string into a JavaScript source code representation.
 * Assumes use of https://www.npmjs.com/package/uuid library for UUID handling.
 */
export default class JavascriptTranslateVisitor extends TranslateVisitor {

    constructor(graphTraversalSourceName: string = 'g') {
        super(graphTraversalSourceName);
    }

    protected override processGremlinSymbol(step: string): string {
        return SymbolHelper.toJavascript(step);
    }

    protected override getCardinalityFunctionClass(): string {
        return 'CardinalityValue';
    }

    visitTraversalStrategy(ctx: any): void {
        this.sb.push('new ');

        if (ctx.getChildCount() === 1) {
            this.sb.push(ctx.getText());
            this.sb.push('()');
        } else {
            const firstText = ctx.getChild(0).getText();
            const className = firstText === 'new' ? ctx.getChild(1).getText() : firstText;
            this.sb.push(className);
            this.sb.push('({');

            const configs: any[] = ctx.configuration();
            for (let ix = 0; ix < configs.length; ix++) {
                this.visit(configs[ix]);
                if (ix < configs.length - 1) {
                    this.sb.push(', ');
                }
            }

            this.sb.push('})');
        }
    }

    visitConfiguration(ctx: any): void {
        const keyText: string = ctx.keyword() != null ? ctx.keyword().getText() : ctx.nakedKey().getText();
        this.sb.push(SymbolHelper.toJavascript(keyText));
        this.sb.push(': ');
        this.visit(ctx.genericArgument());
    }

    visitTraversalGType(ctx: any): void {
        const parts: string[] = ctx.getText().split('.');
        this.sb.push(this.processGremlinSymbol(parts[0]));
        this.sb.push('.');
        this.sb.push(this.processGremlinSymbol(parts[1].toLowerCase()));
    }

    visitGenericMapLiteral(ctx: any): void {
        this.sb.push('new Map([');
        const entries: any[] = ctx.mapEntry();
        for (let i = 0; i < entries.length; i++) {
            this.visit(entries[i]);
            if (i < entries.length - 1) {
                this.sb.push(', ');
            }
        }
        this.sb.push('])');
    }

    visitMapEntry(ctx: any): void {
        this.sb.push('[');
        this.visit(ctx.mapKey());
        this.sb.push(', ');
        this.visit(ctx.genericLiteral());
        this.sb.push(']');
    }

    visitMapKey(ctx: any): void {
        const keyIndex = (ctx.LPAREN() != null && ctx.RPAREN() != null) ? 1 : 0;
        this.visit(ctx.getChild(keyIndex));
    }

    visitDateLiteral(ctx: any): void {
        // child at index 2 is the string argument to datetime() — comes enclosed in quotes
        const dtString: string = ctx.getChild(2).getText();
        const inner = TranslateVisitor.removeFirstAndLastCharacters(dtString);
        this.sb.push("new Date('");
        this.sb.push(formatDatetimeJavaStyle(inner));
        this.sb.push("')");
    }

    visitNanLiteral(_ctx: any): void {
        this.sb.push('Number.NaN');
    }

    visitInfLiteral(ctx: any): void {
        if (ctx.SignedInfLiteral() != null && ctx.SignedInfLiteral().getText() === '-Infinity') {
            this.sb.push('Number.NEGATIVE_INFINITY');
        } else {
            this.sb.push('Number.POSITIVE_INFINITY');
        }
    }

    visitIntegerLiteral(ctx: any): void {
        const integerLiteral: string = ctx.getText().toLowerCase();
        const lastChar = integerLiteral[integerLiteral.length - 1];
        if (/[a-z]/.test(lastChar)) {
            this.sb.push(integerLiteral.slice(0, -1));
        } else {
            this.sb.push(integerLiteral);
        }
    }

    visitFloatLiteral(ctx: any): void {
        if (ctx.infLiteral() != null) { this.visit(ctx.infLiteral()); return; }
        if (ctx.nanLiteral() != null) { this.visit(ctx.nanLiteral()); return; }
        const floatLiteral: string = ctx.getText().toLowerCase();
        const lastChar = floatLiteral[floatLiteral.length - 1];
        if (/[a-z]/.test(lastChar)) {
            this.sb.push(floatLiteral.slice(0, -1));
        } else {
            this.sb.push(floatLiteral);
        }
    }

    visitGenericRangeLiteral(_ctx: any): void {
        throw new TranslatorException('Javascript does not support range literals');
    }

    visitGenericSetLiteral(ctx: any): void {
        this.sb.push('new Set([');
        const literals: any[] = ctx.genericLiteral();
        for (let i = 0; i < literals.length; i++) {
            this.visit(literals[i]);
            if (i < literals.length - 1) {
                this.sb.push(', ');
            }
        }
        this.sb.push('])');
    }

    visitGenericCollectionLiteral(ctx: any): void {
        this.sb.push('[');
        const literals: any[] = ctx.genericLiteral();
        for (let i = 0; i < literals.length; i++) {
            this.visit(literals[i]);
            if (i < literals.length - 1) {
                this.sb.push(', ');
            }
        }
        this.sb.push(']');
    }

    visitUuidLiteral(ctx: any): void {
        if (ctx.stringLiteral() == null) {
            this.sb.push('uuid.v4()');
            return;
        }
        this.sb.push('uuid.parse(');
        this.visitStringLiteral(ctx.stringLiteral());
        this.sb.push(')');
    }
}

/**
 * Formats a datetime string the same way Java's OffsetDateTime.toString() does:
 * - Truncates seconds if both seconds and milliseconds are 0
 * - Uses 'Z' for UTC offset
 * - Assumes UTC when no offset is specified
 */
function formatDatetimeJavaStyle(s: string): string {
    const d = new Date(s);
    if (isNaN(d.getTime())) {
        throw new TranslatorException(`Invalid datetime: ${s}`);
    }
    const year = d.getUTCFullYear().toString().padStart(4, '0');
    const month = (d.getUTCMonth() + 1).toString().padStart(2, '0');
    const day = d.getUTCDate().toString().padStart(2, '0');
    const hours = d.getUTCHours().toString().padStart(2, '0');
    const minutes = d.getUTCMinutes().toString().padStart(2, '0');
    const seconds = d.getUTCSeconds();
    const millis = d.getUTCMilliseconds();

    let result = `${year}-${month}-${day}T${hours}:${minutes}`;
    if (seconds !== 0 || millis !== 0) {
        result += ':' + seconds.toString().padStart(2, '0');
        if (millis !== 0) {
            result += '.' + millis.toString().padStart(3, '0').replace(/0+$/, '');
        }
    }
    result += 'Z';
    return result;
}

class SymbolHelper {
    private static readonly TO_JS_MAP: Record<string, string> = {
        'from': 'from_',
        'in': 'in_',
        'with': 'with_',
        'bigdecimal': 'bigDecimal',
        'bigint': 'bigInt',
    };

    static toJavascript(symbol: string): string {
        return SymbolHelper.TO_JS_MAP[symbol] ?? symbol;
    }
}
