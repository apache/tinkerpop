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

import {
  TokenType,
  UnformattedClosureSyntaxTree,
  UnformattedMethodSyntaxTree,
  UnformattedNonGremlinSyntaxTree,
  UnformattedStringSyntaxTree,
  UnformattedTraversalSyntaxTree,
  UnformattedWordSyntaxTree,
} from './types';
import { last, spaces } from './utils';

type GremlinOnelinerSyntaxTree =
  | UnformattedNonGremlinSyntaxTree
  | Pick<UnformattedTraversalSyntaxTree, 'type' | 'steps'>
  | Pick<UnformattedMethodSyntaxTree, 'type' | 'method' | 'arguments'>
  | Pick<UnformattedClosureSyntaxTree, 'type' | 'method' | 'closureCodeBlock'>
  | Pick<UnformattedStringSyntaxTree, 'type' | 'string'>
  | Pick<UnformattedWordSyntaxTree, 'type' | 'word'>;

const recreateQueryOnelinerFromSyntaxTree = (localIndentation: number = 0) => (
  syntaxTree: GremlinOnelinerSyntaxTree,
): string => {
  switch (syntaxTree.type) {
    // This case will never occur
    case TokenType.NonGremlinCode:
      return syntaxTree.code;
    case TokenType.Traversal:
      return spaces(localIndentation) + syntaxTree.steps.map(recreateQueryOnelinerFromSyntaxTree()).join('.');
    case TokenType.Method:
      return (
        spaces(localIndentation) +
        recreateQueryOnelinerFromSyntaxTree()(syntaxTree.method) +
        '(' +
        syntaxTree.arguments.map(recreateQueryOnelinerFromSyntaxTree()).join(', ') +
        ')'
      );
    case TokenType.Closure:
      return (
        spaces(localIndentation) +
        recreateQueryOnelinerFromSyntaxTree()(syntaxTree.method) +
        '{' +
        last(
          syntaxTree.closureCodeBlock.map(
            ({ lineOfCode, relativeIndentation }) => `${spaces(Math.max(relativeIndentation, 0))}${lineOfCode}`,
          ),
        ) +
        '}'
      );
    case TokenType.String:
      return spaces(localIndentation) + syntaxTree.string;
    case TokenType.Word:
      return spaces(localIndentation) + syntaxTree.word;
  }
};

export default recreateQueryOnelinerFromSyntaxTree;
