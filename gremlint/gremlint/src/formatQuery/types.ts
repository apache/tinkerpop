/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

export type GremlintUserConfig = {
  indentation: number;
  maxLineLength: number;
  shouldPlaceDotsAfterLineBreaks: boolean;
};

export type GremlintInternalConfig = {
  globalIndentation: number;
  localIndentation: number;
  maxLineLength: number;
  shouldPlaceDotsAfterLineBreaks: boolean;
  shouldStartWithDot: boolean;
  shouldEndWithDot: boolean;
  horizontalPosition: number; // Will be used by child syntax trees and is the horizontal position its child content starts, so a non-indented hasLabel(...) has a horizontal position of 9
};

export enum TokenType {
  NonGremlinCode = 'NON_GREMLIN_CODE',
  Traversal = 'TRAVERSAL',
  Method = 'METHOD',
  Closure = 'CLOSURE',
  String = 'STRING',
  Word = 'WORD',
}

export type UnformattedNonGremlinSyntaxTree = {
  type: TokenType.NonGremlinCode;
  code: string;
};

export type UnformattedTraversalSyntaxTree = {
  type: TokenType.Traversal;
  steps: UnformattedSyntaxTree[];
  // Initial horizontal position of the first line of the query. This is needed in order to be able to preserve relative
  // indentation between lines inside a non-Gremlin code block that starts on the first line of the query.
  initialHorizontalPosition: number;
};

export type UnformattedMethodSyntaxTree = {
  type: TokenType.Method;
  method: UnformattedSyntaxTree;
  arguments: UnformattedSyntaxTree[];
};

export type UnformattedClosureLineOfCode = {
  lineOfCode: string;
  // Relative indentation compared to the opening curly bracket, so relativeIndentation of In {it.get} is 0.
  relativeIndentation: number;
};

export type UnformattedClosureCodeBlock = UnformattedClosureLineOfCode[];

export type UnformattedClosureSyntaxTree = {
  type: TokenType.Closure;
  method: UnformattedSyntaxTree;
  closureCodeBlock: UnformattedClosureCodeBlock;
};

export type UnformattedStringSyntaxTree = {
  type: TokenType.String;
  string: string;
};

export type UnformattedWordSyntaxTree = {
  type: TokenType.Word;
  word: string;
};

export type UnformattedSyntaxTree =
  | UnformattedMethodSyntaxTree
  | UnformattedClosureSyntaxTree
  | UnformattedStringSyntaxTree
  | UnformattedWordSyntaxTree
  | UnformattedTraversalSyntaxTree
  | UnformattedNonGremlinSyntaxTree;

export type FormattedNonGremlinSyntaxTree = UnformattedNonGremlinSyntaxTree & {
  width: number;
};

export type GremlinStepGroup = {
  steps: FormattedSyntaxTree[];
};

export type FormattedTraversalSyntaxTree = {
  type: TokenType.Traversal;
  steps: UnformattedSyntaxTree[];
  stepGroups: GremlinStepGroup[];
  initialHorizontalPosition: number;
  localIndentation: number;
  width: number;
};

export type FormattedMethodSyntaxTree = {
  type: TokenType.Method;
  method: FormattedSyntaxTree;
  arguments: UnformattedSyntaxTree[];
  argumentGroups: FormattedSyntaxTree[][];
  argumentsShouldStartOnNewLine: boolean;
  localIndentation: number;
  width: number;
  shouldStartWithDot: boolean;
  shouldEndWithDot: boolean;
};

type FormattedClosureLineOfCode = {
  lineOfCode: string;
  relativeIndentation: number;
  localIndentation: number;
};

type FormattedClosureCodeBlock = FormattedClosureLineOfCode[];

export type FormattedClosureSyntaxTree = {
  type: TokenType.Closure;
  method: FormattedSyntaxTree;
  closureCodeBlock: FormattedClosureCodeBlock;
  localIndentation: number;
  width: number;
  shouldStartWithDot: boolean;
  shouldEndWithDot: boolean;
};

export type FormattedStringSyntaxTree = {
  type: TokenType.String;
  string: string;
  width: number;
  localIndentation: number;
};

export type FormattedWordSyntaxTree = {
  type: TokenType.Word;
  word: string;
  localIndentation: number;
  width: number;
  shouldStartWithDot: boolean;
  shouldEndWithDot: boolean;
};

export type FormattedSyntaxTree =
  | FormattedTraversalSyntaxTree
  | FormattedMethodSyntaxTree
  | FormattedClosureSyntaxTree
  | FormattedStringSyntaxTree
  | FormattedWordSyntaxTree
  | FormattedNonGremlinSyntaxTree;

export type GremlinSyntaxTreeFormatter = (
  config: GremlintInternalConfig,
) => (syntaxTree: UnformattedSyntaxTree) => FormattedSyntaxTree;
