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
  GremlintInternalConfig,
} from './types';
import { last, spaces } from './utils';

type GremlinOnelinerSyntaxTree =
  | UnformattedNonGremlinSyntaxTree
  | Pick<UnformattedTraversalSyntaxTree, 'type' | 'steps'>
  | Pick<UnformattedMethodSyntaxTree, 'type' | 'method' | 'arguments'>
  | Pick<UnformattedClosureSyntaxTree, 'type' | 'method' | 'closureCodeBlock'>
  | Pick<UnformattedStringSyntaxTree, 'type' | 'string'>
  | Pick<UnformattedWordSyntaxTree, 'type' | 'word'>;

const recreateQueryOnelinerFromSyntaxTree = (config: Pick<GremlintInternalConfig, 'globalIndentation'>, localIndentation: number = 0) => (
  syntaxTree: GremlinOnelinerSyntaxTree,
): string => {
  const indentation = spaces(config.globalIndentation + localIndentation);
  switch (syntaxTree.type) {
    // This case will never occur
    case TokenType.NonGremlinCode:
      return syntaxTree.code;
    case TokenType.Traversal:
      return (
        indentation +
        syntaxTree.steps.map(recreateQueryOnelinerFromSyntaxTree({ globalIndentation: 0 })).join('.')
      );
    case TokenType.Method:
      return (
        indentation +
        recreateQueryOnelinerFromSyntaxTree({ globalIndentation: 0 })(syntaxTree.method) +
        '(' +
        syntaxTree.arguments.map(recreateQueryOnelinerFromSyntaxTree({ globalIndentation: 0 })).join(', ') +
        ')'
      );
    case TokenType.Closure:
      return (
        indentation +
        recreateQueryOnelinerFromSyntaxTree({ globalIndentation: 0 })(syntaxTree.method) +
        '{' +
        last(
          syntaxTree.closureCodeBlock.map(
            ({ lineOfCode, relativeIndentation }) => `${spaces(Math.max(relativeIndentation, 0))}${lineOfCode}`,
          ),
        ) +
        '}'
      );
    case TokenType.String:
      return indentation + syntaxTree.string;
    case TokenType.Word:
      return indentation + syntaxTree.word;
  }
};

export default recreateQueryOnelinerFromSyntaxTree;
