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

/**
 * Converts a Gremlin traversal string into a Python source code representation.
 * Assumes use of the `uuid` and `datetime` standard library modules.
 */
export default class PythonTranslateVisitor extends TranslateVisitor {

    constructor(graphTraversalSourceName: string = 'g') {
        super(graphTraversalSourceName);
    }

    protected override processGremlinSymbol(step: string): string {
        return SymbolHelper.toPython(step);
    }

    protected override getCardinalityFunctionClass(): string {
        return 'CardinalityValue';
    }

    protected override handleStringLiteralText(text: string): void {
        this.sb.push("'");
        this.sb.push(text);
        this.sb.push("'");
    }

    visitBooleanLiteral(ctx: any): void {
        const text: string = ctx.getText();
        this.sb.push(text.charAt(0).toUpperCase() + text.slice(1));
    }

    visitNullLiteral(_ctx: any): void {
        this.sb.push('None');
    }

    visitStringNullableLiteral(ctx: any): void {
        if (ctx.getText() === 'null') {
            this.sb.push('None');
        } else {
            const text = TranslateVisitor.removeFirstAndLastCharacters(ctx.getText());
            this.handleStringLiteralText(text);
        }
    }

    visitNanLiteral(_ctx: any): void {
        this.sb.push("float('nan')");
    }

    visitInfLiteral(ctx: any): void {
        if (ctx.SignedInfLiteral() != null && ctx.SignedInfLiteral().getText() === '-Infinity') {
            this.sb.push("float('-inf')");
        } else {
            this.sb.push("float('inf')");
        }
    }

    visitIntegerLiteral(ctx: any): void {
        const integerLiteral: string = ctx.getText().toLowerCase();
        const lastChar = integerLiteral[integerLiteral.length - 1];

        if (!/[a-z]/.test(lastChar)) {
            // No type suffix — check if the value exceeds 32-bit signed integer range
            const value = BigInt(integerLiteral);
            if (value > BigInt(2147483647) || value < BigInt(-2147483648)) {
                this.sb.push('long(');
                this.sb.push(integerLiteral);
                this.sb.push(')');
            } else {
                this.sb.push(integerLiteral);
            }
            return;
        }

        const num = integerLiteral.slice(0, -1);
        switch (lastChar) {
            case 'b':
            case 's':
            case 'i':
                this.sb.push(num);
                break;
            case 'n':
                this.sb.push('bigint(');
                this.sb.push(num);
                this.sb.push(')');
                break;
            case 'l':
                this.sb.push('long(');
                this.sb.push(num);
                this.sb.push(')');
                break;
            default:
                this.sb.push(integerLiteral);
                break;
        }
    }

    visitFloatLiteral(ctx: any): void {
        if (ctx.infLiteral() != null) { this.visit(ctx.infLiteral()); return; }
        if (ctx.nanLiteral() != null) { this.visit(ctx.nanLiteral()); return; }

        const floatLiteral: string = ctx.getText().toLowerCase();
        const lastChar = floatLiteral[floatLiteral.length - 1];
        const num = floatLiteral.slice(0, -1);

        switch (lastChar) {
            case 'f':
            case 'd':
                this.sb.push(num);
                break;
            case 'm':
                this.sb.push('bigdecimal(');
                this.sb.push(num);
                this.sb.push(')');
                break;
            default:
                this.sb.push(floatLiteral);
                break;
        }
    }

    visitDateLiteral(ctx: any): void {
        const dtString: string = ctx.getChild(2).getText();
        const inner = TranslateVisitor.removeFirstAndLastCharacters(dtString);
        this.sb.push("datetime.datetime.fromisoformat('");
        this.sb.push(formatDatetimePythonStyle(inner));
        this.sb.push("')");
    }

    visitGenericRangeLiteral(_ctx: any): void {
        throw new TranslatorException('Python does not support range literals');
    }

    visitGenericSetLiteral(ctx: any): void {
        const literals: any[] = ctx.genericLiteral();
        if (literals.length === 0) {
            this.sb.push('set()');
            return;
        }
        this.sb.push('{');
        for (let i = 0; i < literals.length; i++) {
            this.visit(literals[i]);
            if (i < literals.length - 1) {
                this.sb.push(', ');
            }
        }
        this.sb.push('}');
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

    visitGenericMapLiteral(ctx: any): void {
        this.sb.push('{ ');
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
            this.sb.push('uuid.uuid4()');
            return;
        }
        this.sb.push('uuid.UUID(');
        this.visitStringLiteral(ctx.stringLiteral());
        this.sb.push(')');
    }

    visitTraversalStrategy(ctx: any): void {
        if (ctx.getChildCount() === 1) {
            this.sb.push(ctx.getText());
            this.sb.push('()');
        } else {
            const firstText = ctx.getChild(0).getText();
            const className = firstText === 'new' ? ctx.getChild(1).getText() : firstText;
            this.sb.push(className);
            this.sb.push('(');

            const configs: any[] = ctx.configuration();
            for (let ix = 0; ix < configs.length; ix++) {
                this.visit(configs[ix]);
                if (ix < configs.length - 1) {
                    this.sb.push(', ');
                }
            }

            this.sb.push(')');
        }
    }

    visitConfiguration(ctx: any): void {
        const keyText: string = ctx.keyword() != null ? ctx.keyword().getText() : ctx.nakedKey().getText();
        this.sb.push(SymbolHelper.toPython(keyText));
        this.sb.push('=');
        this.visit(ctx.genericArgument());
    }
}

/**
 * Formats a datetime string for Python's datetime.fromisoformat():
 * - Truncates seconds if both seconds and milliseconds are 0
 * - Uses '+00:00' for UTC (Python < 3.11 does not support 'Z')
 * - Assumes UTC when no offset is specified
 */
function formatDatetimePythonStyle(s: string): string {
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
    result += '+00:00';
    return result;
}

class SymbolHelper {
    private static readonly TO_PYTHON_MAP: Record<string, string> = {
        'global': 'global_',
        'all': 'all_',
        'and': 'and_',
        'any': 'any_',
        'as': 'as_',
        'filter': 'filter_',
        'format': 'format_',
        'from': 'from_',
        'id': 'id_',
        'in': 'in_',
        'is': 'is_',
        'list': 'list_',
        'max': 'max_',
        'min': 'min_',
        'or': 'or_',
        'not': 'not_',
        'range': 'range_',
        'set': 'set_',
        'sum': 'sum_',
        'with': 'with_',
    };

    static toPython(symbol: string): string {
        return SymbolHelper.TO_PYTHON_MAP[symbol] ?? convertCamelCaseToSnakeCase(symbol);
    }
}

function convertCamelCaseToSnakeCase(camelCase: string): string {
    if (!camelCase) return camelCase;
    // skip if this is a class/enum indicated by the first letter being upper case
    if (camelCase.charAt(0) === camelCase.charAt(0).toUpperCase() && /[A-Z]/.test(camelCase.charAt(0))) {
        return camelCase;
    }
    let result = '';
    for (const c of camelCase) {
        if (c >= 'A' && c <= 'Z') {
            result += '_' + c.toLowerCase();
        } else {
            result += c;
        }
    }
    return result;
}
