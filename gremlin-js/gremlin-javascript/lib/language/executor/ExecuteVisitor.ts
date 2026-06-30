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

import { P, TextP } from '../../process/traversal.js';
import type { Arg, Pipeline, StepDescriptor } from '../../process/local/types.js';

/**
 * Converts an ANTLR parse tree produced by GremlinParser into a StepDescriptor[]
 * (Pipeline) suitable for execution by LocalExecutor.
 *
 * Uses the same className→visitXxx dispatch mechanism as TranslateVisitor but
 * accumulates typed step descriptors rather than output strings.
 */
export class ExecuteVisitor {
  private readonly steps: StepDescriptor[] = [];

  getPipeline(): Pipeline {
    return this.steps;
  }

  // ── Dispatch ──────────────────────────────────────────────────────────────

  visit(tree: any): void {
    if (tree == null) return;
    const className: string = tree?.constructor?.name ?? '';
    if (className.endsWith('Context')) {
      const methodName = 'visit' + className.slice(0, -'Context'.length);
      const method = (this as any)[methodName];
      if (typeof method === 'function') {
        method.call(this, tree);
        return;
      }
    }
    // Fall back to visiting children for unhandled intermediate nodes
    this.visitChildren(tree);
  }

  private visitChildren(node: any): void {
    const n = node.getChildCount?.() ?? 0;
    for (let i = 0; i < n; i++) {
      this.visit(node.getChild(i));
    }
  }

  // ── Top-level structure ───────────────────────────────────────────────────

  visitQueryList(ctx: any): void {
    // queryList → query → rootTraversal | terminatedTraversal
    const n = ctx.getChildCount();
    for (let i = 0; i < n; i++) {
      const child = ctx.getChild(i);
      const name = child?.constructor?.name ?? '';
      if (name === 'QueryContext') {
        this.visit(child);
        return;
      }
    }
    this.visitChildren(ctx);
  }

  visitQuery(ctx: any): void {
    this.visitChildren(ctx);
  }

  visitRootTraversal(ctx: any): void {
    // traversalSource DOT traversalSourceSpawnMethod (DOT chainedTraversal)?
    this.visit(ctx.traversalSourceSpawnMethod());
    const chained = ctx.chainedTraversal?.();
    if (chained) this.visitChainedTraversal(chained);
  }

  visitTerminatedTraversal(ctx: any): void {
    this.visit(ctx.rootTraversal?.() ?? ctx.getChild(0));
  }

  visitChainedTraversal(ctx: any): void {
    // Grammar is left-recursive: outermost node holds the LAST step, innermost holds the FIRST.
    // Recurse into the left subtree first, then append the current (rightmost) step.
    const next = ctx.chainedTraversal?.();
    if (next) this.visitChainedTraversal(next);
    this.visit(ctx.traversalMethod());
  }

  visitTraversalSourceSpawnMethod(ctx: any): void {
    this.visitChildren(ctx);
  }

  visitTraversalMethod(ctx: any): void {
    // Find the specific TraversalMethod_* child and dispatch to it.
    // If no specific visitor exists, push a placeholder so validation can reject it.
    const n = ctx.getChildCount?.() ?? 0;
    for (let i = 0; i < n; i++) {
      const child = ctx.getChild(i);
      const name: string = child?.constructor?.name ?? '';
      if (name.startsWith('TraversalMethod_') && name.endsWith('Context')) {
        const visitorName = 'visit' + name.slice(0, -'Context'.length);
        if (typeof (this as any)[visitorName] === 'function') {
          (this as any)[visitorName](child);
        } else {
          // Unknown step — extract name (e.g. 'TraversalMethod_repeatContext' → 'repeat')
          const raw = name.slice('TraversalMethod_'.length, -'Context'.length).split('_')[0];
          this.push(raw, []);
        }
        return;
      }
    }
  }

  // ── Nested traversal → sub-pipeline ──────────────────────────────────────

  private visitNestedTraversalAsSubPipeline(ctx: any): Pipeline {
    const sub = new ExecuteVisitor();
    // nestedTraversal has a chainedTraversal child
    const chain = ctx.chainedTraversal?.();
    if (chain) sub.visitChainedTraversal(chain);
    return sub.getPipeline();
  }

  // ── Mid-traversal V/E (used inside anonymous traversals, e.g. __.V(1)) ──────

  visitTraversalMethod_V(ctx: any): void {
    const args = this.recoverGenericVarargs(ctx.genericArgumentVarargs());
    this.push('V', args);
  }

  visitTraversalMethod_E(ctx: any): void {
    const args = this.recoverGenericVarargs(ctx.genericArgumentVarargs());
    this.push('E', args);
  }

  // ── Source spawn methods (g.V(), g.E(), g.addV(), g.addE()) ──────────────

  visitTraversalSourceSpawnMethod_V(ctx: any): void {
    const args = this.recoverGenericVarargs(ctx.genericArgumentVarargs());
    this.push('V', args);
  }

  visitTraversalSourceSpawnMethod_E(ctx: any): void {
    const args = this.recoverGenericVarargs(ctx.genericArgumentVarargs());
    this.push('E', args);
  }

  visitTraversalSourceSpawnMethod_addV(ctx: any): void {
    const strArg = ctx.stringArgument?.();
    if (strArg) {
      this.push('addV', [this.recoverStringArg(strArg)]);
    } else {
      this.push('addV', []);
    }
  }

  visitTraversalSourceSpawnMethod_addE(ctx: any): void {
    const strArg = ctx.stringArgument?.();
    if (strArg) {
      this.push('addE', [this.recoverStringArg(strArg)]);
    } else {
      // Traversal label form — not supported in Tiny Gremlin
      this.push('addE(Traversal)', []);
    }
  }

  // ── Navigation steps ──────────────────────────────────────────────────────

  visitTraversalMethod_out(ctx: any): void {
    this.push('out', this.recoverStringNullableVarargs(ctx.stringNullableArgumentVarargs()));
  }

  visitTraversalMethod_in(ctx: any): void {
    this.push('in', this.recoverStringNullableVarargs(ctx.stringNullableArgumentVarargs()));
  }

  visitTraversalMethod_both(ctx: any): void {
    this.push('both', this.recoverStringNullableVarargs(ctx.stringNullableArgumentVarargs()));
  }

  visitTraversalMethod_outE(ctx: any): void {
    this.push('outE', this.recoverStringNullableVarargs(ctx.stringNullableArgumentVarargs()));
  }

  visitTraversalMethod_inE(ctx: any): void {
    this.push('inE', this.recoverStringNullableVarargs(ctx.stringNullableArgumentVarargs()));
  }

  visitTraversalMethod_bothE(ctx: any): void {
    this.push('bothE', this.recoverStringNullableVarargs(ctx.stringNullableArgumentVarargs()));
  }

  visitTraversalMethod_outV(ctx: any): void {
    this.push('outV', []);
  }

  visitTraversalMethod_inV(ctx: any): void {
    this.push('inV', []);
  }

  visitTraversalMethod_otherV(ctx: any): void {
    this.push('otherV', []);
  }

  // ── Filter steps ──────────────────────────────────────────────────────────

  // has(key) — existence check
  visitTraversalMethod_has_String(ctx: any): void {
    this.push('has', [this.recoverStringNullableLiteral(ctx.stringNullableLiteral())]);
  }

  // has(key, value)
  visitTraversalMethod_has_String_Object(ctx: any): void {
    this.push('has', [
      this.recoverStringNullableLiteral(ctx.stringNullableLiteral()),
      this.recoverGenericArg(ctx.genericArgument()),
    ]);
  }

  // has(key, P)
  visitTraversalMethod_has_String_P(ctx: any): void {
    this.push('has', [
      this.recoverStringNullableLiteral(ctx.stringNullableLiteral()),
      this.recoverPredicate(ctx.traversalPredicate()),
    ]);
  }

  // has(label, key, value) — label is stringNullableArgument, key is stringNullableLiteral
  visitTraversalMethod_has_String_String_Object(ctx: any): void {
    this.push('has', [
      this.recoverStringNullableArgument(ctx.stringNullableArgument()),
      this.recoverStringNullableLiteral(ctx.stringNullableLiteral()),
      this.recoverGenericArg(ctx.genericArgument()),
    ]);
  }

  // has(label, key, P) — label is stringNullableArgument, key is stringNullableLiteral
  visitTraversalMethod_has_String_String_P(ctx: any): void {
    this.push('has', [
      this.recoverStringNullableArgument(ctx.stringNullableArgument()),
      this.recoverStringNullableLiteral(ctx.stringNullableLiteral()),
      this.recoverPredicate(ctx.traversalPredicate()),
    ]);
  }

  // has(T, value)
  visitTraversalMethod_has_T_Object(ctx: any): void {
    this.push('has', [
      ctx.traversalT().getText(),
      this.recoverGenericArg(ctx.genericArgument()),
    ]);
  }

  // has(T, P)
  visitTraversalMethod_has_T_P(ctx: any): void {
    this.push('has', [
      ctx.traversalT().getText(),
      this.recoverPredicate(ctx.traversalPredicate()),
    ]);
  }

  visitTraversalMethod_hasId_Object_Object(ctx: any): void {
    const first = this.recoverGenericArg(ctx.genericArgument());
    const rest = this.recoverGenericVarargs(ctx.genericArgumentVarargs?.());
    this.push('hasId', [first, ...rest]);
  }

  visitTraversalMethod_hasId_P(ctx: any): void {
    this.push('hasId', [this.recoverPredicate(ctx.traversalPredicate())]);
  }

  visitTraversalMethod_hasLabel_String_String(ctx: any): void {
    const first = this.recoverStringNullableArgument(ctx.stringNullableArgument());
    const rest = this.recoverStringNullableVarargs(ctx.stringNullableArgumentVarargs?.());
    this.push('hasLabel', [first, ...rest]);
  }

  visitTraversalMethod_hasLabel_P(ctx: any): void {
    this.push('hasLabel', [this.recoverPredicate(ctx.traversalPredicate())]);
  }

  visitTraversalMethod_hasNot(ctx: any): void {
    this.push('hasNot', [this.recoverStringNullableLiteral(ctx.stringNullableLiteral())]);
  }

  visitTraversalMethod_hasKey_String_String(ctx: any): void {
    const first = this.recoverStringNullableArgument(ctx.stringNullableArgument());
    const rest = this.recoverStringNullableVarargs(ctx.stringNullableArgumentVarargs?.());
    this.push('hasKey', [first, ...rest]);
  }

  visitTraversalMethod_hasKey_P(ctx: any): void {
    this.push('hasKey', [this.recoverPredicate(ctx.traversalPredicate())]);
  }

  visitTraversalMethod_hasValue_Object_Object(ctx: any): void {
    const first = this.recoverGenericArg(ctx.genericArgument());
    const rest = this.recoverGenericVarargs(ctx.genericArgumentVarargs?.());
    this.push('hasValue', [first, ...rest]);
  }

  visitTraversalMethod_hasValue_P(ctx: any): void {
    this.push('hasValue', [this.recoverPredicate(ctx.traversalPredicate())]);
  }

  // ── Value extraction steps ────────────────────────────────────────────────

  visitTraversalMethod_id(ctx: any): void {
    this.push('id', []);
  }

  visitTraversalMethod_label(ctx: any): void {
    this.push('label', []);
  }

  visitTraversalMethod_key(ctx: any): void {
    this.push('key', []);
  }

  visitTraversalMethod_value(ctx: any): void {
    this.push('value', []);
  }

  visitTraversalMethod_values(ctx: any): void {
    this.push('values', this.recoverStringNullableLiteralVarargs(ctx.stringNullableLiteralVarargs()));
  }

  visitTraversalMethod_valueMap(ctx: any): void {
    // valueMap() or valueMap(key, key...)
    const varargs = ctx.stringNullableArgumentVarargs?.();
    this.push('valueMap', varargs ? this.recoverStringNullableVarargs(varargs) : []);
  }

  visitTraversalMethod_valueMap_String(ctx: any): void {
    this.push('valueMap', this.recoverStringNullableLiteralVarargs(ctx.stringNullableLiteralVarargs()));
  }

  visitTraversalMethod_valueMap_boolean_String(ctx: any): void {
    const includeTokens = ctx.booleanLiteral().getText() === 'true';
    const keys = this.recoverStringNullableLiteralVarargs(ctx.stringNullableLiteralVarargs?.());
    // True sentinel at position 0 signals token inclusion to stepValueMap
    this.push('valueMap', includeTokens ? [true, ...keys] : keys);
  }

  visitTraversalMethod_elementMap(ctx: any): void {
    this.push('elementMap', this.recoverStringNullableLiteralVarargs(ctx.stringNullableLiteralVarargs()));
  }

  // ── Path step ─────────────────────────────────────────────────────────────

  visitTraversalMethod_path(ctx: any): void {
    this.push('path', []);
  }

  // ── Range steps ───────────────────────────────────────────────────────────

  visitTraversalMethod_limit_long(ctx: any): void {
    this.push('limit', [this.recoverIntegerArg(ctx.integerArgument())]);
  }

  visitTraversalMethod_limit_Scope_long(_ctx: any): void {
    this.push('limit(Scope)', []);
  }

  visitTraversalMethod_range_long_long(ctx: any): void {
    const args = ctx.integerArgument();
    this.push('range', [this.recoverIntegerArg(args[0]), this.recoverIntegerArg(args[1])]);
  }

  visitTraversalMethod_range_Scope_long_long(_ctx: any): void {
    this.push('range(Scope)', []);
  }

  visitTraversalMethod_skip_long(ctx: any): void {
    this.push('skip', [this.recoverIntegerArg(ctx.integerArgument())]);
  }

  visitTraversalMethod_skip_Scope_long(_ctx: any): void {
    this.push('skip(Scope)', []);
  }

  visitTraversalMethod_tail_Empty(ctx: any): void {
    this.push('tail', [1]);
  }

  visitTraversalMethod_tail_long(ctx: any): void {
    this.push('tail', [this.recoverIntegerArg(ctx.integerArgument())]);
  }

  visitTraversalMethod_tail_Scope(_ctx: any): void {
    this.push('tail(Scope)', []);
  }

  visitTraversalMethod_tail_Scope_long(_ctx: any): void {
    this.push('tail(Scope)', []);
  }

  // ── Ordering step and by() modulator ─────────────────────────────────────

  visitTraversalMethod_order_Empty(ctx: any): void {
    this.push('order', []);
  }

  visitTraversalMethod_order_Scope(_ctx: any): void {
    this.push('order(Scope)', []);
  }

  /** by(String key) — sort by property value ascending */
  visitTraversalMethod_by_String(ctx: any): void {
    this.push('by', [this.recoverStringLiteral(ctx.stringLiteral()), 'asc']);
  }

  /** by(String key, Order) — sort by property value with direction */
  visitTraversalMethod_by_String_Comparator(ctx: any): void {
    const dir = ctx.traversalComparator().traversalOrder().getText();
    this.push('by', [this.recoverStringLiteral(ctx.stringLiteral()), dir]);
  }

  /** by(T token) — sort by T.id or T.label */
  visitTraversalMethod_by_T(ctx: any): void {
    this.push('by', [ctx.traversalT().getText(), 'asc']);
  }

  /** by() — natural ascending sort */
  visitTraversalMethod_by_Empty(_ctx: any): void {
    this.push('by', [null, 'asc']);
  }

  /** by(Order) — natural sort with explicit direction */
  visitTraversalMethod_by_Comparator(ctx: any): void {
    const dir = ctx.traversalComparator().traversalOrder().getText();
    this.push('by', [null, dir]);
  }

  /** by(Order) using traversalOrder directly */
  visitTraversalMethod_by_Order(ctx: any): void {
    const dir = ctx.traversalOrder().getText();
    this.push('by', [null, dir]);
  }

  // by(Traversal) — emit the parsed sub-pipeline; the local executor restricts it to a
  // single value-extraction step (e.g. elementMap()) when used as a path() projection.
  visitTraversalMethod_by_Traversal(ctx: any): void {
    this.push('by', [this.visitNestedTraversalAsSubPipeline(ctx.nestedTraversal())]);
  }

  // by(Traversal, Comparator) — Order is irrelevant for path() projection and is dropped.
  visitTraversalMethod_by_Traversal_Comparator(ctx: any): void {
    this.push('by', [this.visitNestedTraversalAsSubPipeline(ctx.nestedTraversal())]);
  }

  /** by(T, Order) — T.id, T.label etc. with a direction, routed through traversalFunction */
  visitTraversalMethod_by_Function(ctx: any): void {
    const tCtx = ctx.traversalFunction().traversalT?.();
    if (tCtx) {
      this.push('by', [tCtx.getText(), 'asc']);
    } else {
      this.push('by(Function)', []);
    }
  }

  visitTraversalMethod_by_Function_Comparator(ctx: any): void {
    const tCtx = ctx.traversalFunction().traversalT?.();
    if (tCtx) {
      const dir = ctx.traversalComparator().traversalOrder().getText();
      this.push('by', [tCtx.getText(), dir]);
    } else {
      this.push('by(Function)', []);
    }
  }

  // ── Repeat / loop steps ───────────────────────────────────────────────────
  // repeat() and its times()/until()/emit() modulators are pushed as separate
  // descriptors here; the local executor folds them into a single repeat spec,
  // tracking whether each modulator was declared before or after the repeat().

  visitTraversalMethod_repeat_Traversal(ctx: any): void {
    this.push('repeat', [this.visitNestedTraversalAsSubPipeline(ctx.nestedTraversal())]);
  }

  // repeat(String, Traversal) — the loop-label form. Tiny Gremlin has no step labels,
  // so this is left without a body so the executor can reject it with a clear message.

  visitTraversalMethod_times(ctx: any): void {
    this.push('times', [this.recoverIntegerLiteral(ctx.integerLiteral())]);
  }

  visitTraversalMethod_until_Traversal(ctx: any): void {
    this.push('until', [this.visitNestedTraversalAsSubPipeline(ctx.nestedTraversal())]);
  }

  visitTraversalMethod_until_Predicate(ctx: any): void {
    this.push('until', [this.recoverPredicate(ctx.traversalPredicate())]);
  }

  visitTraversalMethod_emit_Empty(_ctx: any): void {
    this.push('emit', []);
  }

  visitTraversalMethod_emit_Traversal(ctx: any): void {
    this.push('emit', [this.visitNestedTraversalAsSubPipeline(ctx.nestedTraversal())]);
  }

  visitTraversalMethod_emit_Predicate(ctx: any): void {
    this.push('emit', [this.recoverPredicate(ctx.traversalPredicate())]);
  }

  visitTraversalMethod_loops_Empty(_ctx: any): void {
    this.push('loops', []);
  }

  // loops(String) — the loop-label reference. Tiny Gremlin has no step labels, so the
  // label is carried through for the executor to reject with a clear message.
  visitTraversalMethod_loops_String(ctx: any): void {
    this.push('loops', [this.recoverStringLiteral(ctx.stringLiteral())]);
  }

  visitTraversalMethod_is_Object(ctx: any): void {
    this.push('is', [this.recoverGenericArg(ctx.genericArgument())]);
  }

  visitTraversalMethod_is_P(ctx: any): void {
    this.push('is', [this.recoverPredicate(ctx.traversalPredicate())]);
  }

  // ── Mutation steps ────────────────────────────────────────────────────────

  visitTraversalMethod_addV_Empty(ctx: any): void {
    this.push('addV', []);
  }

  visitTraversalMethod_addV_String(ctx: any): void {
    this.push('addV', [this.recoverStringArg(ctx.stringArgument())]);
  }

  visitTraversalMethod_addV_Traversal(ctx: any): void {
    this.push('addV', [this.visitNestedTraversalAsSubPipeline(ctx.nestedTraversal())]);
  }

  visitTraversalMethod_addE_String(ctx: any): void {
    this.push('addE', [this.recoverStringArg(ctx.stringArgument())]);
  }

  visitTraversalMethod_addE_Traversal(_ctx: any): void {
    // addE(Traversal) is not supported in Tiny Gremlin — push an unsupported step name
    // so the validation pass rejects it with a clear LocalExecutionError.
    this.push('addE(Traversal)', []);
  }

  // property(key, value) — the supported form
  visitTraversalMethod_property_Object_Object_Object(ctx: any): void {
    this.push('property', [
      this.recoverGenericLiteral(ctx.genericLiteral()),
      this.recoverGenericArg(ctx.genericArgument()),
    ]);
  }

  // from(traversal) modulator
  visitTraversalMethod_from_Traversal(ctx: any): void {
    this.push('from', [this.visitNestedTraversalAsSubPipeline(ctx.nestedTraversal())]);
  }

  // from(string) modulator — label reference, treated as-is
  visitTraversalMethod_from_String(ctx: any): void {
    this.push('from', [this.recoverStringLiteral(ctx.stringLiteral())]);
  }

  // to(traversal) modulator
  visitTraversalMethod_to_Traversal(ctx: any): void {
    this.push('to', [this.visitNestedTraversalAsSubPipeline(ctx.nestedTraversal())]);
  }

  // to(string) modulator
  visitTraversalMethod_to_String(ctx: any): void {
    this.push('to', [this.recoverStringLiteral(ctx.stringLiteral())]);
  }

  // ── Argument recovery helpers ─────────────────────────────────────────────

  private recoverStringLiteral(ctx: any): string {
    if (ctx == null) return '';
    const text: string = ctx.getText();
    // Strip surrounding quotes (' or ") and unescape escaped quotes
    if ((text.startsWith("'") && text.endsWith("'")) ||
        (text.startsWith('"') && text.endsWith('"'))) {
      return text.slice(1, -1)
        .replace(/\\'/g, "'")
        .replace(/\\"/g, '"')
        .replace(/\\\\/g, '\\');
    }
    return text;
  }

  private recoverStringNullableLiteral(ctx: any): string | null {
    if (ctx == null) return null;
    const text: string = ctx.getText();
    if (text === 'null') return null;
    return this.recoverStringLiteral(ctx.stringLiteral?.() ?? ctx);
  }

  private recoverStringArg(ctx: any): string {
    if (ctx == null) return '';
    const lit = ctx.stringLiteral?.();
    return lit ? this.recoverStringLiteral(lit) : ctx.getText();
  }

  private recoverStringNullableArgument(ctx: any): string | null {
    if (ctx == null) return null;
    const lit = ctx.stringNullableLiteral?.();
    return lit ? this.recoverStringNullableLiteral(lit) : ctx.getText();
  }

  private recoverStringNullableVarargs(ctx: any): Arg[] {
    if (ctx == null) return [];
    const items = ctx.stringNullableArgument?.() ?? [];
    return (Array.isArray(items) ? items : [items])
      .map((a: any) => this.recoverStringNullableArgument(a));
  }

  /** Recover args from a stringNullableLiteralVarargs context (used by values, valueMap, elementMap). */
  private recoverStringNullableLiteralVarargs(ctx: any): Arg[] {
    if (ctx == null) return [];
    const items = ctx.stringNullableLiteral?.() ?? [];
    return (Array.isArray(items) ? items : [items])
      .map((a: any) => this.recoverStringNullableLiteral(a));
  }

  private recoverIntegerArg(ctx: any): number {
    if (ctx == null) return 0;
    const lit = ctx.integerLiteral?.();
    if (lit) return this.recoverIntegerLiteral(lit);
    return parseInt(ctx.getText(), 10);
  }

  private recoverIntegerLiteral(ctx: any): number {
    const text: string = ctx.getText().toLowerCase();
    // Strip type suffix: L, S, B
    const stripped = text.replace(/[lsb]$/, '');
    return parseInt(stripped, 10);
  }

  private recoverFloatLiteral(ctx: any): number {
    const text: string = ctx.getText().toLowerCase();
    if (text === '+infinity') return Infinity;
    if (text === '-infinity') return -Infinity;
    if (text === 'nan') return NaN;
    return parseFloat(text.replace(/[fd]$/, ''));
  }

  private recoverGenericLiteral(ctx: any): Arg {
    if (ctx == null) return null;
    const numeric = ctx.numericLiteral?.();
    if (numeric) {
      const int = numeric.integerLiteral?.();
      if (int) return this.recoverIntegerLiteral(int);
      const float = numeric.floatLiteral?.();
      if (float) return this.recoverFloatLiteral(float);
    }
    const bool = ctx.booleanLiteral?.();
    if (bool) return bool.getText() === 'true';
    const str = ctx.stringLiteral?.();
    if (str) return this.recoverStringLiteral(str);
    const nil = ctx.nullLiteral?.();
    if (nil) return null;
    // Nested traversal as a generic literal
    const nested = ctx.nestedTraversal?.();
    if (nested) return this.visitNestedTraversalAsSubPipeline(nested);
    // Collection literal [a, b, c]
    const coll = ctx.genericCollectionLiteral?.();
    if (coll) return coll.genericLiteral().map((gl: any) => this.recoverGenericLiteral(gl));
    return ctx.getText();
  }

  private recoverGenericArg(ctx: any): Arg {
    if (ctx == null) return null;
    const lit = ctx.genericLiteral?.();
    if (lit) return this.recoverGenericLiteral(lit);
    return ctx.getText();
  }

  private recoverGenericVarargs(ctx: any): Arg[] {
    if (ctx == null) return [];
    const items = ctx.genericArgument?.() ?? [];
    return (Array.isArray(items) ? items : [items])
      .map((a: any) => this.recoverGenericArg(a));
  }

  private recoverPredicate(ctx: any): P | TextP {
    if (ctx == null) return P.eq(null);
    const name: string = ctx.constructor?.name ?? '';

    // Compound predicates: predicate DOT and/or LPAREN predicate RPAREN (6 children)
    if (ctx.getChildCount?.() === 6) {
      const op = ctx.getChild(2)?.getText();
      if (op === 'and' || op === 'or') {
        const left = this.recoverPredicate(ctx.getChild(0));
        const right = this.recoverPredicate(ctx.getChild(4));
        return op === 'and' ? left.and(right) : left.or(right);
      }
    }

    // TextP string predicates — checked BEFORE 'not' to avoid prefix collision
    if (name.includes('Predicate_notContaining'))
      return TextP.notContaining(this.recoverStringArg(ctx.stringArgument()) as string);
    if (name.includes('Predicate_notStartingWith'))
      return TextP.notStartingWith(this.recoverStringArg(ctx.stringArgument()) as string);
    if (name.includes('Predicate_notEndingWith'))
      return TextP.notEndingWith(this.recoverStringArg(ctx.stringArgument()) as string);
    if (name.includes('Predicate_notRegex'))
      return (TextP as any).notRegex(this.recoverStringArg(ctx.stringArgument()) as string);
    if (name.includes('Predicate_containing'))
      return TextP.containing(this.recoverStringArg(ctx.stringArgument()) as string);
    if (name.includes('Predicate_startingWith'))
      return TextP.startingWith(this.recoverStringArg(ctx.stringArgument()) as string);
    if (name.includes('Predicate_endingWith'))
      return TextP.endingWith(this.recoverStringArg(ctx.stringArgument()) as string);
    if (name.includes('Predicate_regex'))
      return (TextP as any).regex(this.recoverStringArg(ctx.stringArgument()) as string);
    // P numeric predicates
    if (name.includes('Predicate_eq')) return P.eq(this.recoverGenericArg(ctx.genericArgument()));
    if (name.includes('Predicate_neq')) return P.neq(this.recoverGenericArg(ctx.genericArgument()));
    if (name.includes('Predicate_lte')) return P.lte(this.recoverGenericArg(ctx.genericArgument()));
    if (name.includes('Predicate_lt')) return P.lt(this.recoverGenericArg(ctx.genericArgument()));
    if (name.includes('Predicate_gte')) return P.gte(this.recoverGenericArg(ctx.genericArgument()));
    if (name.includes('Predicate_gt')) return P.gt(this.recoverGenericArg(ctx.genericArgument()));
    if (name.includes('Predicate_between')) {
      const args = ctx.genericArgument();
      return P.between(this.recoverGenericArg(args[0]), this.recoverGenericArg(args[1]));
    }
    if (name.includes('Predicate_within')) {
      const vals = this.recoverGenericVarargs(ctx.genericArgumentVarargs?.());
      return (P as any).within(...vals);
    }
    if (name.includes('Predicate_without')) {
      const vals = this.recoverGenericVarargs(ctx.genericArgumentVarargs?.());
      return (P as any).without(...vals);
    }
    if (name.includes('Predicate_inside')) {
      const args = ctx.genericArgument();
      return (P as any).inside(this.recoverGenericArg(args[0]), this.recoverGenericArg(args[1]));
    }
    if (name.includes('Predicate_outside')) {
      const args = ctx.genericArgument();
      return (P as any).outside(this.recoverGenericArg(args[0]), this.recoverGenericArg(args[1]));
    }
    if (name.includes('Predicate_not')) return P.not(this.recoverPredicate(ctx.traversalPredicate()));

    // Single-child wrapper: visit the child
    if (ctx.getChildCount?.() === 1) return this.recoverPredicate(ctx.getChild(0));

    return P.eq(null);
  }

  // ── Step accumulator ──────────────────────────────────────────────────────

  private push(name: string, args: Arg[]): void {
    this.steps.push({ name, args });
  }
}
