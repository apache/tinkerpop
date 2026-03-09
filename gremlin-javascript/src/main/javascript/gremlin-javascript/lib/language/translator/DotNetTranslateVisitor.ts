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

// Steps whose return type needs <object, object>
const DOUBLE_GENERIC_STEPS = new Set(['group', 'valueMap']);

/**
 * Converts a Gremlin traversal string into a C# source code representation.
 * Mirrors the Java DotNetTranslateVisitor.
 */
export default class DotNetTranslateVisitor extends TranslateVisitor {

    constructor(graphTraversalSourceName: string = 'g') {
        super(graphTraversalSourceName);
    }

    protected override processGremlinSymbol(step: string): string {
        return SymbolHelper.toCSharp(step);
    }

    protected override getCardinalityFunctionClass(): string {
        return 'CardinalityValue';
    }

    /**
     * .NET enum values are capitalized but do NOT go through processGremlinSymbol,
     * since the SymbolHelper mappings (bigdecimal→BigDecimal etc.) don't apply to enums.
     */
    protected override appendExplicitNaming(txt: string, prefix: string): void {
        if (!txt.startsWith(prefix + '.')) {
            this.sb.push(prefix);
            this.sb.push('.');
            this.sb.push(capitalize(txt));
        } else {
            const parts = txt.split('.');
            this.sb.push(parts[0]);
            this.sb.push('.');
            this.sb.push(capitalize(parts[1]));
        }
    }

    visitTraversalGType(ctx: any): void {
        const parts: string[] = ctx.getText().split('.');
        this.sb.push(this.processGremlinSymbol(parts[0]));
        this.sb.push('.');
        this.sb.push(this.processGremlinSymbol(parts[1].toLowerCase()));
    }

    visitTraversalDirection(ctx: any): void {
        const direction = ctx.getText().toLowerCase();
        this.sb.push('Direction.');
        if (direction.includes('out')) this.sb.push('Out');
        else if (direction.includes('in')) this.sb.push('In');
        else if (direction.includes('from')) this.sb.push('From');
        else if (direction.includes('to')) this.sb.push('To');
        else this.sb.push('Both');
    }

    visitTraversalDirectionShort(ctx: any): void {
        this.visitTraversalDirection(ctx);
    }

    visitTraversalDirectionLong(ctx: any): void {
        this.visitTraversalDirection(ctx);
    }

    visitNanLiteral(_ctx: any): void {
        this.sb.push('Double.NaN');
    }

    visitInfLiteral(ctx: any): void {
        if (ctx.SignedInfLiteral() != null && ctx.SignedInfLiteral().getText() === '-Infinity') {
            this.sb.push('Double.NegativeInfinity');
        } else {
            this.sb.push('Double.PositiveInfinity');
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
                this.sb.push('(sbyte) ');
                this.sb.push(num);
                break;
            case 's':
                this.sb.push('(short) ');
                this.sb.push(num);
                break;
            case 'i':
                this.sb.push(num);
                break;
            case 'l':
                this.sb.push(integerLiteral); // keep the 'l' suffix for C#
                break;
            case 'n':
                this.sb.push('BigInteger.Parse("');
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
                this.sb.push(floatLiteral); // keep the f/d suffix for C#
                break;
            case 'm':
                this.sb.push('(decimal) ');
                this.sb.push(num);
                break;
            default:
                this.sb.push(floatLiteral);
        }
    }

    visitDateLiteral(ctx: any): void {
        const dtString: string = ctx.getChild(2).getText();
        const inner = TranslateVisitor.removeFirstAndLastCharacters(dtString);
        this.sb.push('DateTimeOffset.Parse("');
        this.sb.push(formatDatetimeJavaStyle(inner));
        this.sb.push('")');
    }

    visitGenericRangeLiteral(_ctx: any): void {
        throw new TranslatorException('.NET does not support range literals');
    }

    visitGenericSetLiteral(ctx: any): void {
        this.sb.push('new HashSet<object> { ');
        const literals: any[] = ctx.genericLiteral();
        for (let i = 0; i < literals.length; i++) {
            this.visit(literals[i]);
            if (i < literals.length - 1) this.sb.push(', ');
        }
        this.sb.push(' }');
    }

    visitGenericCollectionLiteral(ctx: any): void {
        this.sb.push('new List<object> { ');
        const literals: any[] = ctx.genericLiteral();
        for (let i = 0; i < literals.length; i++) {
            this.visit(literals[i]);
            if (i < literals.length - 1) this.sb.push(', ');
        }
        this.sb.push(' }');
    }

    visitGenericMapLiteral(ctx: any): void {
        this.sb.push('new Dictionary<object, object> {');
        const entries: any[] = ctx.mapEntry();
        for (let i = 0; i < entries.length; i++) {
            this.visit(entries[i]);
            if (i < entries.length - 1) this.sb.push(', ');
        }
        this.sb.push('}');
    }

    visitMapEntry(ctx: any): void {
        this.sb.push('{ ');
        this.visit(ctx.mapKey());
        this.sb.push(', ');
        this.visit(ctx.genericLiteral());
        this.sb.push(' }');
    }

    visitMapKey(ctx: any): void {
        const keyIndex = (ctx.LPAREN() != null && ctx.RPAREN() != null) ? 1 : 0;
        this.visit(ctx.getChild(keyIndex));
    }

    visitUuidLiteral(ctx: any): void {
        if (ctx.stringLiteral() == null) {
            this.sb.push('Guid.NewGuid()');
            return;
        }
        this.sb.push('Guid.Parse(');
        this.sb.push(ctx.stringLiteral().getText());
        this.sb.push(')');
    }

    visitClassType(ctx: any): void {
        this.sb.push('typeof(');
        this.sb.push(ctx.getText());
        this.sb.push(')');
    }

    visitTraversalStrategy(ctx: any): void {
        if (ctx.getChildCount() === 1) {
            this.sb.push('new ');
            this.sb.push(ctx.getText());
            this.sb.push('()');
            return;
        }

        const firstText = ctx.getChild(0).getText();
        const strategyName = firstText === 'new' ? ctx.getChild(1).getText() : firstText;
        this.sb.push('new ');
        this.sb.push(strategyName);
        this.sb.push('(');

        const configs: any[] = ctx.configuration();
        const isOptionsStrategy = ctx.children?.some((c: any) => c.getText() === 'OptionsStrategy');

        if (configs.length > 0 && isOptionsStrategy) {
            this.sb.push('new Dictionary<string, object> {');
            for (const cfg of configs) {
                this.sb.push('{"');
                this.sb.push(cfg.getChild(0).getText());
                this.sb.push('",');
                this.visit(cfg.getChild(2));
                this.sb.push('},');
            }
            this.sb.push('}');
        } else {
            for (let ix = 0; ix < configs.length; ix++) {
                this.visit(configs[ix]);
                if (ix < configs.length - 1) this.sb.push(', ');
            }
        }

        this.sb.push(')');
    }

    visitConfiguration(ctx: any): void {
        const key: string = ctx.getChild(0).getText();
        this.sb.push(key);
        this.sb.push(': ');

        const startIdx = this.sb.length;
        this.visit(ctx.getChild(2));

        if (key === 'readPartitions') {
            const added = this.sb.splice(startIdx);
            this.sb.push(added.join('').replace('List<object>', 'HashSet<string>'));
        } else if (key === 'keys') {
            const added = this.sb.splice(startIdx);
            const joined = added.join('');
            this.sb.push(joined.includes('HashSet<object>') ? joined.replace('HashSet<object>', 'HashSet<string>') : joined);
        }
    }

    visitStringNullableArgumentVarargs(ctx: any): void {
        const n = ctx.getChildCount();
        for (let ix = 0; ix < n; ix++) {
            const child = ctx.getChild(ix);
            if (child.constructor?.name === 'StringNullableArgumentContext') {
                this.tryAppendCastToString_StringNullableArg(child);
            }
            this.visit(child);
        }
    }

    // ─── handleGenerics helpers ─────────────────────────────────────────────

    private handleGenerics(ctx: any): void {
        const step: string = ctx.getChild(0).getText();
        this.sb.push(capitalize(step));
        this.sb.push(DOUBLE_GENERIC_STEPS.has(step) ? '<object, object>' : '<object>');
        for (let ix = 1; ix < ctx.getChildCount(); ix++) {
            this.visit(ctx.getChild(ix));
        }
    }

    private handleLongArguments(ctx: any): void {
        const step: string = ctx.getChild(0).getText();
        this.sb.push(capitalize(step));
        this.sb.push('<object>');
        for (let ix = 1; ix < ctx.getChildCount(); ix++) {
            const child = ctx.getChild(ix);
            if (child.constructor?.name === 'IntegerArgumentContext') {
                this.tryAppendCastToLong_IntArg(child);
            }
            this.visit(child);
        }
    }

    // ─── Cast helpers ────────────────────────────────────────────────────────

    private tryAppendCastToLong_IntArg(ctx: any): void {
        if (ctx?.variable() != null) this.sb.push('(long) ');
    }

    private tryAppendCastToString_StringArg(ctx: any): void {
        if (ctx?.variable() != null || ctx?.stringLiteral() != null) this.sb.push('(string) ');
    }

    private tryAppendCastToString_StringNullableLit(ctx: any): void {
        if (ctx?.K_NULL != null && ctx.K_NULL() != null) this.sb.push('(string) ');
    }

    private tryAppendCastToString_StringNullableArg(ctx: any): void {
        if (ctx?.variable() != null ||
            (ctx?.stringNullableLiteral() != null && ctx.stringNullableLiteral()?.K_NULL != null && ctx.stringNullableLiteral().K_NULL() != null)) {
            this.sb.push('(string) ');
        }
    }

    private tryAppendCastToObject_GenericArg(ctx: any): void {
        if (ctx?.variable() != null ||
            (ctx?.genericLiteral() != null && ctx.genericLiteral()?.nullLiteral != null && ctx.genericLiteral().nullLiteral() != null)) {
            this.sb.push('(object) ');
        }
    }

    private tryAppendCastToObject_GenericLit(ctx: any): void {
        if (ctx?.nullLiteral != null && ctx.nullLiteral() != null) this.sb.push('(object) ');
    }

    // ─── isCalledAsFirstStepInAnonymousTraversal ─────────────────────────────

    private isCalledAsFirstStepInAnonymousTraversal(ctx: any): boolean {
        // ctx = traversalMethod_fold_EmptyContext
        // parent = traversalMethodContext → chainedTraversalContext → nestedTraversalContext
        const parent = ctx.parent;
        if (!parent) return false;
        const parentParent = parent.parent; // ChainedTraversalContext
        if (!parentParent) return false;
        const parentParentParent = parentParent.parent; // should be NestedTraversalContext
        if (parentParentParent?.constructor?.name !== 'NestedTraversalContext') return false;
        // Check if ctx is the first step: parentParent.child(0).child(0) === ctx
        const firstChild = parentParent.getChild(0);
        const firstGrandchild = firstChild?.getChild(0);
        return firstGrandchild === ctx;
    }

    // ─── Source spawn method overrides ──────────────────────────────────────

    visitTraversalSourceSpawnMethod_inject(ctx: any): void { this.handleGenerics(ctx); }
    visitTraversalSourceSpawnMethod_io(ctx: any): void { this.handleGenerics(ctx); }
    visitTraversalSourceSpawnMethod_union(ctx: any): void { this.handleGenerics(ctx); }
    visitTraversalSourceSpawnMethod_call_empty(ctx: any): void { this.handleGenerics(ctx); }

    visitTraversalSourceSpawnMethod_addV(ctx: any): void {
        this.sb.push(capitalize(ctx.getChild(0).getText()));
        for (let ix = 1; ix < ctx.getChildCount(); ix++) {
            const child = ctx.getChild(ix);
            if (child.constructor?.name === 'StringArgumentContext') this.tryAppendCastToString_StringArg(child);
            this.visit(child);
        }
    }

    visitTraversalSourceSpawnMethod_addE(ctx: any): void {
        this.sb.push(capitalize(ctx.getChild(0).getText()));
        for (let ix = 1; ix < ctx.getChildCount(); ix++) {
            const child = ctx.getChild(ix);
            if (child.constructor?.name === 'StringArgumentContext') this.tryAppendCastToString_StringArg(child);
            this.visit(child);
        }
    }

    visitTraversalSourceSpawnMethod_call_string(ctx: any): void {
        this.sb.push(capitalize(ctx.getChild(0).getText()));
        this.sb.push('<object>');
        this.sb.push('((string) ');
        this.visit(ctx.stringLiteral());
        this.sb.push(')');
    }

    visitTraversalSourceSpawnMethod_call_string_map(ctx: any): void {
        this.sb.push(capitalize(ctx.getChild(0).getText()));
        this.sb.push('<object>(');
        this.visit(ctx.stringLiteral());
        this.sb.push(', (IDictionary<object, object>) ');
        this.visit(ctx.genericMapArgument());
        this.sb.push(')');
    }

    visitTraversalSourceSpawnMethod_call_string_traversal(ctx: any): void {
        this.sb.push(capitalize(ctx.getChild(0).getText()));
        this.sb.push('<object>(');
        this.visit(ctx.stringLiteral());
        this.sb.push(', (ITraversal) ');
        this.visit(ctx.nestedTraversal());
        this.sb.push(')');
    }

    visitTraversalSourceSpawnMethod_call_string_map_traversal(ctx: any): void {
        this.sb.push(capitalize(ctx.getChild(0).getText()));
        this.sb.push('<object>(');
        this.visit(ctx.stringLiteral());
        this.sb.push(', (IDictionary<object, object>) ');
        this.visit(ctx.genericMapArgument());
        this.sb.push(', (ITraversal) ');
        this.visit(ctx.nestedTraversal());
        this.sb.push(')');
    }

    visitTraversalSourceSpawnMethod_mergeV_Map(ctx: any): void {
        this.visit(ctx.getChild(0));
        this.sb.push('((IDictionary<object, object>) ');
        this.visit(ctx.genericMapNullableArgument());
        this.sb.push(')');
    }

    visitTraversalSourceSpawnMethod_mergeV_Traversal(ctx: any): void {
        this.visit(ctx.getChild(0));
        this.sb.push('((ITraversal) ');
        this.visit(ctx.nestedTraversal());
        this.sb.push(')');
    }

    visitTraversalSourceSpawnMethod_mergeE_Map(ctx: any): void {
        this.visit(ctx.getChild(0));
        this.sb.push('((IDictionary<object, object>) ');
        this.visit(ctx.genericMapNullableArgument());
        this.sb.push(')');
    }

    visitTraversalSourceSpawnMethod_mergeE_Traversal(ctx: any): void {
        this.visit(ctx.getChild(0));
        this.sb.push('((ITraversal) ');
        this.visit(ctx.nestedTraversal());
        this.sb.push(')');
    }

    // ─── Traversal method overrides — handleGenerics ─────────────────────────

    visitTraversalMethod_asString_Scope(ctx: any): void { this.handleGenerics(ctx); }
    visitTraversalMethod_branch(ctx: any): void { this.handleGenerics(ctx); }
    visitTraversalMethod_cap(ctx: any): void { this.handleGenerics(ctx); }
    visitTraversalMethod_choose_Function(ctx: any): void { this.handleGenerics(ctx); }
    visitTraversalMethod_choose_Predicate_Traversal(ctx: any): void { this.handleGenerics(ctx); }
    visitTraversalMethod_choose_Predicate_Traversal_Traversal(ctx: any): void { this.handleGenerics(ctx); }
    visitTraversalMethod_choose_Traversal(ctx: any): void { this.handleGenerics(ctx); }
    visitTraversalMethod_choose_Traversal_Traversal(ctx: any): void { this.handleGenerics(ctx); }
    visitTraversalMethod_choose_Traversal_Traversal_Traversal(ctx: any): void { this.handleGenerics(ctx); }
    visitTraversalMethod_coalesce(ctx: any): void { this.handleGenerics(ctx); }
    visitTraversalMethod_constant(ctx: any): void { this.handleGenerics(ctx); }
    visitTraversalMethod_elementMap(ctx: any): void { this.handleGenerics(ctx); }
    visitTraversalMethod_flatMap(ctx: any): void { this.handleGenerics(ctx); }

    visitTraversalMethod_fold_Empty(ctx: any): void {
        if (this.isCalledAsFirstStepInAnonymousTraversal(ctx)) {
            this.handleGenerics(ctx);
        } else {
            this.visitChildren(ctx); // base: just output Fold()
        }
    }

    visitTraversalMethod_fold_Object_BiFunction(ctx: any): void { this.handleGenerics(ctx); }
    visitTraversalMethod_group_Empty(ctx: any): void { this.handleGenerics(ctx); }
    visitTraversalMethod_groupCount_Empty(ctx: any): void { this.handleGenerics(ctx); }
    visitTraversalMethod_index(ctx: any): void { this.handleGenerics(ctx); }
    visitTraversalMethod_length_Scope(ctx: any): void { this.handleGenerics(ctx); }
    visitTraversalMethod_local(ctx: any): void { this.handleGenerics(ctx); }
    visitTraversalMethod_lTrim_Scope(ctx: any): void { this.handleGenerics(ctx); }
    visitTraversalMethod_map(ctx: any): void { this.handleGenerics(ctx); }
    visitTraversalMethod_match(ctx: any): void { this.handleGenerics(ctx); }
    visitTraversalMethod_max_Empty(ctx: any): void { this.handleGenerics(ctx); }
    visitTraversalMethod_max_Scope(ctx: any): void { this.handleGenerics(ctx); }
    visitTraversalMethod_mean_Empty(ctx: any): void { this.handleGenerics(ctx); }
    visitTraversalMethod_mean_Scope(ctx: any): void { this.handleGenerics(ctx); }
    visitTraversalMethod_min_Empty(ctx: any): void { this.handleGenerics(ctx); }
    visitTraversalMethod_min_Scope(ctx: any): void { this.handleGenerics(ctx); }
    visitTraversalMethod_optional(ctx: any): void { this.handleGenerics(ctx); }
    visitTraversalMethod_profile_Empty(ctx: any): void { this.handleGenerics(ctx); }
    visitTraversalMethod_project(ctx: any): void { this.handleGenerics(ctx); }
    visitTraversalMethod_properties(ctx: any): void { this.handleGenerics(ctx); }
    visitTraversalMethod_propertyMap(ctx: any): void { this.handleGenerics(ctx); }
    visitTraversalMethod_replace_Scope_String_String(ctx: any): void { this.handleGenerics(ctx); }
    visitTraversalMethod_rTrim_Scope(ctx: any): void { this.handleGenerics(ctx); }
    visitTraversalMethod_sack_Empty(ctx: any): void { this.handleGenerics(ctx); }
    visitTraversalMethod_select_Column(ctx: any): void { this.handleGenerics(ctx); }
    visitTraversalMethod_select_Pop_String(ctx: any): void { this.handleGenerics(ctx); }
    visitTraversalMethod_select_Pop_String_String_String(ctx: any): void { this.handleGenerics(ctx); }
    visitTraversalMethod_select_Pop_Traversal(ctx: any): void { this.handleGenerics(ctx); }
    visitTraversalMethod_select_String(ctx: any): void { this.handleGenerics(ctx); }
    visitTraversalMethod_select_String_String_String(ctx: any): void { this.handleGenerics(ctx); }
    visitTraversalMethod_select_Traversal(ctx: any): void { this.handleGenerics(ctx); }
    visitTraversalMethod_split_Scope_String(ctx: any): void { this.handleGenerics(ctx); }
    visitTraversalMethod_substring_Scope_int(ctx: any): void { this.handleGenerics(ctx); }
    visitTraversalMethod_substring_Scope_int_int(ctx: any): void { this.handleGenerics(ctx); }
    visitTraversalMethod_sum_Empty(ctx: any): void { this.handleGenerics(ctx); }
    visitTraversalMethod_sum_Scope(ctx: any): void { this.handleGenerics(ctx); }
    visitTraversalMethod_tail_Empty(ctx: any): void { this.handleGenerics(ctx); }
    visitTraversalMethod_tail_Scope(ctx: any): void { this.handleGenerics(ctx); }
    visitTraversalMethod_toLower_Scope(ctx: any): void { this.handleGenerics(ctx); }
    visitTraversalMethod_toUpper_Scope(ctx: any): void { this.handleGenerics(ctx); }
    visitTraversalMethod_tree_Empty(ctx: any): void { this.handleGenerics(ctx); }
    visitTraversalMethod_trim_Scope(ctx: any): void { this.handleGenerics(ctx); }
    visitTraversalMethod_unfold(ctx: any): void { this.handleGenerics(ctx); }
    visitTraversalMethod_union(ctx: any): void { this.handleGenerics(ctx); }
    visitTraversalMethod_value(ctx: any): void { this.handleGenerics(ctx); }
    visitTraversalMethod_valueMap_String(ctx: any): void { this.handleGenerics(ctx); }
    visitTraversalMethod_valueMap_boolean_String(ctx: any): void { this.handleGenerics(ctx); }
    visitTraversalMethod_values(ctx: any): void { this.handleGenerics(ctx); }

    // ─── Traversal method overrides — handleLongArguments ────────────────────

    visitTraversalMethod_limit_long(ctx: any): void { this.handleLongArguments(ctx); }
    visitTraversalMethod_limit_Scope_long(ctx: any): void { this.handleLongArguments(ctx); }
    visitTraversalMethod_range_long_long(ctx: any): void { this.handleLongArguments(ctx); }
    visitTraversalMethod_range_Scope_long_long(ctx: any): void { this.handleLongArguments(ctx); }
    visitTraversalMethod_skip_long(ctx: any): void { this.handleLongArguments(ctx); }
    visitTraversalMethod_skip_Scope_long(ctx: any): void { this.handleLongArguments(ctx); }
    visitTraversalMethod_tail_long(ctx: any): void { this.handleLongArguments(ctx); }
    visitTraversalMethod_tail_Scope_long(ctx: any): void { this.handleLongArguments(ctx); }

    // ─── addV / addE (string cast) ───────────────────────────────────────────

    visitTraversalMethod_addV_String(ctx: any): void {
        this.sb.push(capitalize(ctx.getChild(0).getText()));
        for (let ix = 1; ix < ctx.getChildCount(); ix++) {
            const child = ctx.getChild(ix);
            if (child.constructor?.name === 'StringArgumentContext') this.tryAppendCastToString_StringArg(child);
            this.visit(child);
        }
    }

    visitTraversalMethod_addE_String(ctx: any): void {
        this.sb.push(capitalize(ctx.getChild(0).getText()));
        for (let ix = 1; ix < ctx.getChildCount(); ix++) {
            const child = ctx.getChild(ix);
            if (child.constructor?.name === 'StringArgumentContext') this.tryAppendCastToString_StringArg(child);
            this.visit(child);
        }
    }

    // ─── call variants ───────────────────────────────────────────────────────

    visitTraversalMethod_call_string(ctx: any): void {
        this.sb.push(capitalize(ctx.getChild(0).getText()));
        this.sb.push('<object>((string) ');
        this.visit(ctx.stringLiteral());
        this.sb.push(')');
    }

    visitTraversalMethod_call_string_map(ctx: any): void {
        this.sb.push(capitalize(ctx.getChild(0).getText()));
        this.sb.push('<object>(');
        this.visit(ctx.stringLiteral());
        this.sb.push(', (IDictionary<object, object>) ');
        this.visit(ctx.genericMapArgument());
        this.sb.push(')');
    }

    visitTraversalMethod_call_string_traversal(ctx: any): void {
        this.sb.push(capitalize(ctx.getChild(0).getText()));
        this.sb.push('<object>(');
        this.visit(ctx.stringLiteral());
        this.sb.push(', (ITraversal) ');
        this.visit(ctx.nestedTraversal());
        this.sb.push(')');
    }

    visitTraversalMethod_call_string_map_traversal(ctx: any): void {
        this.sb.push(capitalize(ctx.getChild(0).getText()));
        this.sb.push('<object>(');
        this.visit(ctx.stringLiteral());
        this.sb.push(', (IDictionary<object, object>) ');
        this.visit(ctx.genericMapArgument());
        this.sb.push(', (ITraversal) ');
        this.visit(ctx.nestedTraversal());
        this.sb.push(')');
    }

    // ─── has family ──────────────────────────────────────────────────────────

    visitTraversalMethod_has_String_Object(ctx: any): void {
        this.sb.push(capitalize(ctx.getChild(0).getText()));
        this.sb.push('(');
        this.tryAppendCastToString_StringNullableLit(ctx.stringNullableLiteral());
        this.visit(ctx.stringNullableLiteral());
        this.sb.push(', ');
        this.tryAppendCastToObject_GenericArg(ctx.genericArgument());
        this.visit(ctx.genericArgument());
        this.sb.push(')');
    }

    visitTraversalMethod_has_String_P(ctx: any): void {
        this.sb.push(capitalize(ctx.getChild(0).getText()));
        this.sb.push('(');
        this.tryAppendCastToString_StringNullableLit(ctx.stringNullableLiteral());
        this.visit(ctx.stringNullableLiteral());
        this.sb.push(', ');
        this.visit(ctx.traversalPredicate());
        this.sb.push(')');
    }

    visitTraversalMethod_has_String_String_Object(ctx: any): void {
        this.sb.push(capitalize(ctx.getChild(0).getText()));
        this.sb.push('(');
        this.tryAppendCastToString_StringNullableArg(ctx.stringNullableArgument());
        this.visit(ctx.stringNullableArgument());
        this.sb.push(', ');
        this.tryAppendCastToString_StringNullableLit(ctx.stringNullableLiteral());
        this.visit(ctx.stringNullableLiteral());
        this.sb.push(', ');
        this.tryAppendCastToObject_GenericArg(ctx.genericArgument());
        this.visit(ctx.genericArgument());
        this.sb.push(')');
    }

    visitTraversalMethod_has_String_String_P(ctx: any): void {
        this.sb.push(capitalize(ctx.getChild(0).getText()));
        this.sb.push('(');
        this.tryAppendCastToString_StringNullableArg(ctx.stringNullableArgument());
        this.visit(ctx.stringNullableArgument());
        this.sb.push(', ');
        this.tryAppendCastToString_StringNullableLit(ctx.stringNullableLiteral());
        this.visit(ctx.stringNullableLiteral());
        this.sb.push(', ');
        this.visit(ctx.traversalPredicate());
        this.sb.push(')');
    }

    visitTraversalMethod_has_T_Object(ctx: any): void {
        this.sb.push(capitalize(ctx.getChild(0).getText()));
        this.sb.push('(');
        this.visit(ctx.traversalT());
        this.sb.push(', ');
        this.tryAppendCastToObject_GenericArg(ctx.genericArgument());
        this.visit(ctx.genericArgument());
        this.sb.push(')');
    }

    visitTraversalMethod_has_T_P(ctx: any): void {
        this.sb.push(capitalize(ctx.getChild(0).getText()));
        this.sb.push('(');
        this.visit(ctx.traversalT());
        this.sb.push(', ');
        this.visit(ctx.traversalPredicate());
        this.sb.push(')');
    }

    visitTraversalMethod_hasKey_P(ctx: any): void {
        this.sb.push(capitalize(ctx.getChild(0).getText()));
        this.sb.push('(');
        this.visit(ctx.traversalPredicate());
        this.sb.push(')');
    }

    visitTraversalMethod_hasKey_String_String(ctx: any): void {
        // single arg: add (string) cast; multiple args: no cast
        if (ctx.stringNullableLiteralVarargs() == null || ctx.stringNullableLiteralVarargs().getChildCount() === 0) {
            this.sb.push(capitalize(ctx.getChild(0).getText()));
            this.sb.push('(');
            this.tryAppendCastToString_StringNullableLit(ctx.stringNullableLiteral());
            this.visit(ctx.stringNullableLiteral());
            this.sb.push(')');
        } else {
            this.visitChildren(ctx);
        }
    }

    visitTraversalMethod_hasValue_Object_Object(ctx: any): void {
        // single arg: add (object) cast; multiple args: no cast
        if (ctx.genericArgumentVarargs() == null || ctx.genericArgumentVarargs().getChildCount() === 0) {
            this.sb.push(capitalize(ctx.getChild(0).getText()));
            this.sb.push('(');
            this.tryAppendCastToObject_GenericArg(ctx.genericArgument());
            this.visit(ctx.genericArgument());
            this.sb.push(')');
        } else {
            this.visitChildren(ctx);
        }
    }

    visitTraversalMethod_hasValue_P(ctx: any): void {
        this.sb.push(capitalize(ctx.getChild(0).getText()));
        this.sb.push('(');
        this.visit(ctx.traversalPredicate());
        this.sb.push(')');
    }

    visitTraversalMethod_hasLabel_P(ctx: any): void {
        this.sb.push(capitalize(ctx.getChild(0).getText()));
        this.sb.push('(');
        this.visit(ctx.traversalPredicate());
        this.sb.push(')');
    }

    visitTraversalMethod_hasLabel_String_String(ctx: any): void {
        const step = capitalize(ctx.getChild(0).getText());
        if (ctx.stringNullableArgumentVarargs() == null || ctx.stringNullableArgumentVarargs().getChildCount() === 0) {
            // single arg: add (string) cast if variable or null
            this.sb.push(step);
            this.sb.push('(');
            this.tryAppendCastToString_StringNullableArg(ctx.stringNullableArgument());
            this.visit(ctx.stringNullableArgument());
            this.sb.push(')');
        } else {
            // multiple args
            this.sb.push(step);
            this.sb.push('(');
            this.tryAppendCastToString_StringNullableArg(ctx.stringNullableArgument());
            this.visit(ctx.stringNullableArgument());
            if (ctx.stringNullableArgumentVarargs().getChildCount() > 0) this.sb.push(', ');
            this.visit(ctx.stringNullableArgumentVarargs());
            this.sb.push(')');
        }
    }

    // ─── mergeV / mergeE ─────────────────────────────────────────────────────

    visitTraversalMethod_mergeV_Map(ctx: any): void {
        this.visit(ctx.getChild(0));
        this.sb.push('((IDictionary<object, object>) ');
        this.visit(ctx.genericMapNullableArgument());
        this.sb.push(')');
    }

    visitTraversalMethod_mergeV_Traversal(ctx: any): void {
        this.visit(ctx.getChild(0));
        this.sb.push('((ITraversal) ');
        this.visit(ctx.nestedTraversal());
        this.sb.push(')');
    }

    visitTraversalMethod_mergeE_Map(ctx: any): void {
        this.visit(ctx.getChild(0));
        this.sb.push('((IDictionary<object, object>) ');
        this.visit(ctx.genericMapNullableArgument());
        this.sb.push(')');
    }

    visitTraversalMethod_mergeE_Traversal(ctx: any): void {
        this.visit(ctx.getChild(0));
        this.sb.push('((ITraversal) ');
        this.visit(ctx.nestedTraversal());
        this.sb.push(')');
    }

    // ─── option ─────────────────────────────────────────────────────────────

    visitTraversalMethod_option_Merge_Map(ctx: any): void {
        this.visit(ctx.getChild(0));
        this.sb.push('(');
        this.visit(ctx.traversalMerge());
        this.sb.push(', (IDictionary<object, object>) ');
        this.visit(ctx.genericMapNullableArgument());
        this.sb.push(')');
    }

    visitTraversalMethod_option_Merge_Traversal(ctx: any): void {
        this.visit(ctx.getChild(0));
        this.sb.push('(');
        this.visit(ctx.traversalMerge());
        this.sb.push(', (ITraversal) ');
        this.visit(ctx.nestedTraversal());
        this.sb.push(')');
    }

    visitTraversalMethod_option_Object_Traversal(ctx: any): void {
        // Only add (ITraversal) cast when the object is a traversalMerge
        const argIsTraversalMerge = ctx.genericArgument()?.genericLiteral()?.traversalMerge != null &&
            ctx.genericArgument().genericLiteral().traversalMerge() != null;
        if (argIsTraversalMerge) {
            this.visit(ctx.getChild(0));
            this.sb.push('(');
            this.visit(ctx.genericArgument());
            this.sb.push(', (ITraversal) ');
            this.visit(ctx.nestedTraversal());
            this.sb.push(')');
        } else {
            this.visitChildren(ctx);
        }
    }

    // ─── property ────────────────────────────────────────────────────────────

    visitTraversalMethod_property_Cardinality_Object_Object_Object(ctx: any): void {
        if (ctx.genericArgumentVarargs() == null || ctx.genericArgumentVarargs().getChildCount() === 0) {
            this.sb.push(capitalize(ctx.getChild(0).getText()));
            this.sb.push('(');
            this.visit(ctx.traversalCardinality());
            this.sb.push(', ');
            this.tryAppendCastToObject_GenericLit(ctx.genericLiteral());
            this.visit(ctx.genericLiteral());
            this.sb.push(', ');
            this.tryAppendCastToObject_GenericArg(ctx.genericArgument());
            this.visit(ctx.genericArgument());
            this.sb.push(')');
        } else {
            this.visitChildren(ctx);
        }
    }

    // ─── conjoin ─────────────────────────────────────────────────────────────

    visitTraversalMethod_conjoin_String(ctx: any): void {
        this.sb.push(capitalize(ctx.getChild(0).getText()));
        this.sb.push('(');
        this.tryAppendCastToString_StringLit(ctx.stringLiteral());
        this.visit(ctx.stringLiteral());
        this.sb.push(')');
    }

    private tryAppendCastToString_StringLit(ctx: any): void {
        if (ctx != null) this.sb.push('(string) ');
    }
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

function capitalize(s: string): string {
    if (!s) return s;
    return s.charAt(0).toUpperCase() + s.slice(1);
}

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

class SymbolHelper {
    private static readonly TO_CS_MAP: Record<string, string> = {
        'graphml': 'GraphML',
        'graphson': 'GraphSON',
        'bigdecimal': 'BigDecimal',
        'bigint': 'BigInt',
        'datetime': 'DateTime',
        'uuid': 'UUID',
        'vproperty': 'VProperty',
    };

    static toCSharp(symbol: string): string {
        return SymbolHelper.TO_CS_MAP[symbol] ?? capitalize(symbol);
    }
}
