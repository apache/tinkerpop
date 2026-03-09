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

/**
 * Anonymizes a Gremlin traversal by replacing literal values with type-based
 * placeholders (e.g. `string0`, `number1`).  Identical values within the same
 * traversal reuse the same placeholder.  Mirrors the Java AnonymizedTranslatorVisitor.
 */
export default class AnonymizedTranslateVisitor extends TranslateVisitor {

    /** outer key: Java simple class name (e.g. "String"); inner key: original text → placeholder */
    private readonly cache: Map<string, Map<string, string>> = new Map();

    constructor(graphTraversalSourceName: string = 'g') {
        super(graphTraversalSourceName);
    }

    /**
     * Replaces the text of `ctx` with a type-tagged placeholder.
     * The first unique value of each type gets index 0, the second index 1, etc.
     * Repeated occurrences of the same text reuse the same placeholder.
     *
     * @param ctx  ANTLR parse-tree context whose getText() is used as the cache key
     * @param type Java simple class name (e.g. "String", "Integer", "Number")
     */
    protected anonymize(ctx: any, type: string): void {
        const text: string = ctx.getText();

        let inner = this.cache.get(type);
        if (inner === undefined) {
            inner = new Map<string, string>();
            this.cache.set(type, inner);
        }

        let placeholder = inner.get(text);
        if (placeholder === undefined) {
            placeholder = type.toLowerCase() + inner.size;
            inner.set(text, placeholder);
        }

        this.sb.push(placeholder);
    }

    // ─── Collection / map literals ───────────────────────────────────────────

    visitGenericSetLiteral(ctx: any): void {
        this.anonymize(ctx, 'Set');
    }

    visitGenericCollectionLiteral(ctx: any): void {
        this.anonymize(ctx, 'List');
    }

    visitGenericMapLiteral(ctx: any): void {
        this.anonymize(ctx, 'Map');
    }

    visitGenericMapNullableArgument(ctx: any): void {
        this.anonymize(ctx, 'Map');
    }

    // ─── String literals ─────────────────────────────────────────────────────

    visitStringLiteral(ctx: any): void {
        this.anonymize(ctx, 'String');
    }

    visitStringNullableLiteral(ctx: any): void {
        this.anonymize(ctx, 'String');
    }

    // ─── Numeric literals ────────────────────────────────────────────────────

    visitIntegerLiteral(ctx: any): void {
        const lastChar = ctx.getText().toLowerCase().slice(-1);
        switch (lastChar) {
            case 'b': this.anonymize(ctx, 'Byte');       break;
            case 's': this.anonymize(ctx, 'Short');      break;
            case 'i': this.anonymize(ctx, 'Integer');    break;
            case 'l': this.anonymize(ctx, 'Long');       break;
            case 'n': this.anonymize(ctx, 'BigInteger'); break;
            default:  this.anonymize(ctx, 'Number');     break;
        }
    }

    visitFloatLiteral(ctx: any): void {
        // Do NOT delegate to infLiteral/nanLiteral — anonymize the FloatLiteralContext directly
        const lastChar = ctx.getText().toLowerCase().slice(-1);
        switch (lastChar) {
            case 'f': this.anonymize(ctx, 'Float');      break;
            case 'd': this.anonymize(ctx, 'Double');     break;
            case 'm': this.anonymize(ctx, 'BigDecimal'); break;
            default:  this.anonymize(ctx, 'Number');     break;
        }
    }

    // ─── Other literals ──────────────────────────────────────────────────────

    visitBooleanLiteral(ctx: any): void {
        this.anonymize(ctx, 'Boolean');
    }

    visitDateLiteral(ctx: any): void {
        this.anonymize(ctx, 'OffsetDateTime');
    }

    visitNullLiteral(ctx: any): void {
        this.anonymize(ctx, 'Object');
    }

    visitNanLiteral(ctx: any): void {
        this.anonymize(ctx, 'Number');
    }

    visitInfLiteral(ctx: any): void {
        this.anonymize(ctx, 'Number');
    }

    visitUuidLiteral(ctx: any): void {
        this.anonymize(ctx, 'String');
    }

    // ─── Keys — output unquoted (only values are anonymized) ─────────────────

    visitNakedKey(ctx: any): void {
        this.sb.push(ctx.getText());
    }

    visitKeyword(ctx: any): void {
        const keyword: string = ctx.getText();
        const parentName = ctx.parent?.constructor?.name ?? '';
        if (parentName === 'MapKeyContext' || parentName === 'ConfigurationContext') {
            this.sb.push(keyword);
        } else {
            this.sb.push(keyword);
            this.sb.push(' ');
        }
    }
}
