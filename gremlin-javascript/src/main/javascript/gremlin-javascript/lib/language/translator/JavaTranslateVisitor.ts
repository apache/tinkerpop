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
 * Converts a Gremlin traversal string into a Java source code representation.
 * Mirrors the Java JavaTranslateVisitor.
 */
export default class JavaTranslateVisitor extends TranslateVisitor {

    constructor(graphTraversalSourceName: string = 'g') {
        super(graphTraversalSourceName);
    }

    visitTraversalStrategy(ctx: any): void {
        if (ctx.getChildCount() === 1) {
            this.sb.push(ctx.getText());
            this.sb.push('.instance()');
            return;
        }

        this.visit(ctx.classType());
        this.sb.push('.build()');

        const configs: any[] = ctx.configuration();
        // mirrors Java: ctx.getChild(1) is "OptionsStrategy" only when form is `new OptionsStrategy(...)`
        const isNewOptionsStrategy: boolean = configs.length > 0 &&
            ctx.getChild(1)?.getText() === 'OptionsStrategy';

        if (isNewOptionsStrategy) {
            for (const cfg of configs) {
                this.sb.push('.with("');
                this.sb.push(cfg.getChild(0).getText());
                this.sb.push('", ');
                this.visit(cfg.getChild(2));
                this.sb.push(')');
            }
        } else {
            for (const cfg of configs) {
                this.sb.push('.');
                this.visit(cfg);
            }
        }

        this.sb.push('.create()');
    }

    visitConfiguration(ctx: any): void {
        this.sb.push(ctx.getChild(0).getText());
        this.sb.push('(');
        this.visit(ctx.getChild(2));
        this.sb.push(')');
    }

    visitClassType(ctx: any): void {
        const parentName = ctx.parent?.constructor?.name ?? '';
        this.sb.push(ctx.getText());
        if (parentName === 'TraversalSourceSelfMethod_withoutStrategiesContext' ||
            parentName === 'ClassTypeExprContext') {
            this.sb.push('.class');
        }
    }

    visitGenericMapLiteral(ctx: any): void {
        this.sb.push('new LinkedHashMap<Object, Object>() {{ ');
        const entries: any[] = ctx.mapEntry();
        for (let i = 0; i < entries.length; i++) {
            this.visit(entries[i]);
            if (i < entries.length - 1) this.sb.push(' ');
        }
        this.sb.push(' }}');
    }

    visitMapEntry(ctx: any): void {
        this.sb.push('put(');
        this.visit(ctx.mapKey());
        this.sb.push(', ');
        this.visit(ctx.genericLiteral());
        this.sb.push(');');
    }

    visitMapKey(ctx: any): void {
        const keyIndex = (ctx.LPAREN() != null && ctx.RPAREN() != null) ? 1 : 0;
        this.visit(ctx.getChild(keyIndex));
    }

    visitDateLiteral(ctx: any): void {
        const dtString: string = ctx.getChild(2).getText();
        const inner = TranslateVisitor.removeFirstAndLastCharacters(dtString);
        this.sb.push('OffsetDateTime.parse("');
        this.sb.push(formatDatetimeJavaStyle(inner));
        this.sb.push('")');
    }

    visitNanLiteral(_ctx: any): void {
        this.sb.push('Double.NaN');
    }

    visitInfLiteral(ctx: any): void {
        if (ctx.SignedInfLiteral() != null && ctx.SignedInfLiteral().getText() === '-Infinity') {
            this.sb.push('Double.NEGATIVE_INFINITY');
        } else {
            this.sb.push('Double.POSITIVE_INFINITY');
        }
    }

    visitIntegerLiteral(ctx: any): void {
        const integerLiteral: string = ctx.getText().toLowerCase();
        const lastChar = integerLiteral[integerLiteral.length - 1];

        if (!/[a-z]/.test(lastChar)) {
            this.sb.push(integerLiteral);
            return;
        }

        const num = integerLiteral.slice(0, -1);
        switch (lastChar) {
            case 'b':
                this.sb.push('new Byte(');
                this.sb.push(num);
                this.sb.push(')');
                break;
            case 's':
                this.sb.push('new Short(');
                this.sb.push(num);
                this.sb.push(')');
                break;
            case 'i':
                this.sb.push(num);
                break;
            case 'l':
                this.sb.push(integerLiteral); // keep the 'l' suffix
                break;
            case 'n':
                this.sb.push('new BigInteger("');
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

        if (!/[a-z]/.test(lastChar)) {
            this.sb.push(floatLiteral);
            return;
        }

        const num = floatLiteral.slice(0, -1);
        switch (lastChar) {
            case 'f':
            case 'd':
                this.sb.push(floatLiteral); // keep f/d suffix
                break;
            case 'm':
                this.sb.push('new BigDecimal("');
                this.sb.push(num);
                this.sb.push('")');
                break;
            default:
                this.sb.push(floatLiteral);
        }
    }

    visitUuidLiteral(ctx: any): void {
        if (ctx.stringLiteral() == null) {
            this.sb.push('UUID.randomUUID()');
            return;
        }
        this.sb.push('UUID.fromString(');
        this.sb.push(ctx.stringLiteral().getText());
        this.sb.push(')');
    }

    visitGenericRangeLiteral(_ctx: any): void {
        throw new TranslatorException('Java does not support range literals');
    }

    visitGenericSetLiteral(ctx: any): void {
        this.sb.push('new HashSet<Object>() {{ ');
        const literals: any[] = ctx.genericLiteral();
        for (let i = 0; i < literals.length; i++) {
            this.sb.push('add(');
            this.visit(literals[i]);
            this.sb.push(');');
            if (i < literals.length - 1) this.sb.push(' ');
        }
        this.sb.push(' }}');
    }

    visitGenericCollectionLiteral(ctx: any): void {
        this.sb.push('new ArrayList<Object>() {{ ');
        const literals: any[] = ctx.genericLiteral();
        for (let i = 0; i < literals.length; i++) {
            this.sb.push('add(');
            this.visit(literals[i]);
            this.sb.push(');');
            if (i < literals.length - 1) this.sb.push(' ');
        }
        this.sb.push(' }}');
    }
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

/**
 * Formats a datetime string the same way Java's OffsetDateTime.toString() does:
 * - Truncates seconds if both seconds and milliseconds are 0
 * - Uses 'Z' for UTC offset
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
