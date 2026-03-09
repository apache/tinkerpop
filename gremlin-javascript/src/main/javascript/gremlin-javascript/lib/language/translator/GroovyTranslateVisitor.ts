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
 * Converts a Gremlin traversal string into a Groovy source code representation.
 * Mirrors the Java GroovyTranslateVisitor.
 */
export default class GroovyTranslateVisitor extends TranslateVisitor {

    constructor(graphTraversalSourceName: string = 'g') {
        super(graphTraversalSourceName);
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
                this.sb.push('(byte)');
                this.sb.push(num);
                break;
            case 's':
                this.sb.push('(short)');
                this.sb.push(num);
                break;
            case 'n':
                // Groovy BigInteger uses the 'g' suffix
                this.sb.push(num);
                this.sb.push('g');
                break;
            default:
                // i and l suffixes and anything else kept as-is
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
                // keep f/d suffix as-is
                this.sb.push(floatLiteral);
                break;
            case 'm':
                // Groovy BigDecimal: if no decimal point use new BigDecimal(NL), else just the number
                if (!floatLiteral.includes('.')) {
                    this.sb.push('new BigDecimal(');
                    this.sb.push(num);
                    this.sb.push('L)');
                } else {
                    this.sb.push(num);
                }
                break;
            default:
                this.sb.push(floatLiteral);
        }
    }

    visitInfLiteral(ctx: any): void {
        if (ctx.SignedInfLiteral() != null && ctx.SignedInfLiteral().getText() === '-Infinity') {
            this.sb.push('Double.NEGATIVE_INFINITY');
        } else {
            this.sb.push('Double.POSITIVE_INFINITY');
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

    visitNullLiteral(ctx: any): void {
        if (ctx.parent?.constructor?.name === 'GenericMapNullableArgumentContext') {
            this.sb.push('null as Map');
        } else {
            this.sb.push(ctx.getText());
        }
    }

    visitGenericSetLiteral(ctx: any): void {
        this.sb.push('[');
        const literals: any[] = ctx.genericLiteral();
        for (let i = 0; i < literals.length; i++) {
            this.visit(literals[i]);
            if (i < literals.length - 1) this.sb.push(', ');
        }
        this.sb.push('] as Set');
    }

    visitStringLiteral(ctx: any): void {
        // Preserve original quote style and escape Groovy GString interpolation
        this.sb.push(ctx.getText().replace(/\$/g, '\\$'));
    }

    visitStringNullableLiteral(ctx: any): void {
        if (ctx.getText() === 'null') {
            this.sb.push('null');
        } else {
            // Preserve original quote style and escape Groovy GString interpolation
            this.sb.push(ctx.getText().replace(/\$/g, '\\$'));
        }
    }

    visitMapKey(ctx: any): void {
        if (ctx.LPAREN() != null && ctx.RPAREN() != null) {
            // Groovy map syntax: expression keys must be parenthesized — preserve the parens
            this.sb.push('(');
            this.visit(ctx.getChild(1));
            this.sb.push(')');
        } else {
            this.visit(ctx.getChild(0));
        }
    }

    visitNakedKey(ctx: any): void {
        // Groovy map/config syntax uses unquoted identifier keys
        this.sb.push(ctx.getText());
    }

    visitKeyword(ctx: any): void {
        const keyword: string = ctx.getText();
        const parentName = ctx.parent?.constructor?.name ?? '';
        if (parentName === 'MapKeyContext' || parentName === 'ConfigurationContext') {
            // Groovy uses unquoted keys
            this.sb.push(keyword);
        } else {
            this.sb.push(keyword);
            this.sb.push(' ');
        }
    }

    visitTraversalStrategy(ctx: any): void {
        if (ctx.getChildCount() === 1) {
            // No-arg strategy: output name as-is (no .instance())
            this.sb.push(ctx.getText());
        } else {
            // Ensure 'new' keyword is present
            if (ctx.getChild(0).getText() !== 'new') {
                this.sb.push('new ');
            }
            this.visitChildren(ctx);
        }
    }

    visitTraversalSourceSpawnMethod_inject(ctx: any): void {
        this.handleInject(ctx);
    }

    visitTraversalMethod_inject(ctx: any): void {
        this.handleInject(ctx);
    }

    visitTraversalMethod_hasLabel_String_String(ctx: any): void {
        if (ctx.getChildCount() > 4 && ctx.getChild(2).getText() === 'null') {
            const varArgs = ctx.getChild(4);
            this.sb.push(ctx.getChild(0).getText());
            this.sb.push('((String) null, ');
            for (let i = 0; i < varArgs.getChildCount(); i += 2) {
                if (varArgs.getChild(i).getText() === 'null') {
                    this.sb.push('(String) null');
                } else {
                    this.visit(varArgs.getChild(i));
                }
                if (i < varArgs.getChildCount() - 1) {
                    this.sb.push(', ');
                }
            }
            this.sb.push(')');
            return;
        }
        this.visitChildren(ctx);
    }

    visitTraversalMethod_option_Merge_Map(ctx: any): void {
        this.sb.push('option(');
        this.visit(ctx.traversalMerge());
        this.sb.push(', ');
        this.tryCastMapNullableArgument(ctx.genericMapNullableArgument());
        this.sb.push(')');
    }

    visitTraversalMethod_mergeV_Map(ctx: any): void {
        this.sb.push('mergeV(');
        this.tryCastMapNullableArgument(ctx.genericMapNullableArgument());
        this.sb.push(')');
    }

    visitTraversalSourceSpawnMethod_mergeV_Map(ctx: any): void {
        this.sb.push('mergeV(');
        this.tryCastMapNullableArgument(ctx.genericMapNullableArgument());
        this.sb.push(')');
    }

    visitTraversalMethod_mergeE_Map(ctx: any): void {
        this.sb.push('mergeE(');
        this.tryCastMapNullableArgument(ctx.genericMapNullableArgument());
        this.sb.push(')');
    }

    visitTraversalSourceSpawnMethod_mergeE_Map(ctx: any): void {
        this.sb.push('mergeE(');
        this.tryCastMapNullableArgument(ctx.genericMapNullableArgument());
        this.sb.push(')');
    }

    private tryCastMapNullableArgument(ctx: any): void {
        const nullableLit = ctx?.genericMapNullableLiteral?.();
        if (nullableLit != null && nullableLit?.nullLiteral?.() != null) {
            this.sb.push('(Map) null');
        } else {
            this.visit(ctx);
        }
    }

    /**
     * Special handling for inject with second `null` argument like g.inject(1, null).
     * inject() is ambiguous in Groovy with jdk extension inject(Object, Closure).
     */
    private handleInject(ctx: any): void {
        if (ctx.getChildCount() > 3) {
            const child2 = ctx.getChild(2);
            if (child2?.constructor?.name === 'GenericLiteralVarargsContext') {
                const varArgs = child2;
                if (varArgs.getChildCount() === 1) {
                    const child0 = varArgs.getChild(0);
                    if (child0?.constructor?.name === 'GenericLiteralExprContext') {
                        const injectArgs = child0;
                        if (injectArgs.getChildCount() > 2 && injectArgs.getChild(2).getText() === 'null') {
                            this.sb.push(ctx.getChild(0).getText());
                            this.sb.push('(');
                            for (let i = 0; i < injectArgs.getChildCount(); i += 2) {
                                if (i === 2) {
                                    this.sb.push('(Object) null');
                                } else {
                                    this.visit(injectArgs.getChild(i));
                                }
                                if (i < injectArgs.getChildCount() - 1) {
                                    this.sb.push(', ');
                                }
                            }
                            this.sb.push(')');
                            return;
                        }
                    }
                }
            }
        }
        this.visitChildren(ctx);
    }
}
