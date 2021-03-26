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

import { FormattedSyntaxTree, GremlintInternalConfig, TokenType, UnformattedSyntaxTree } from '../types';
import { formatClosure } from './formatClosure';
import { formatMethod } from './formatMethod';
import { formatNonGremlin } from './formatNonGremlin';
import { formatString } from './formatString';
import { formatTraversal } from './formatTraversal';
import { formatWord } from './formatWord';

const formatSyntaxTree = (config: GremlintInternalConfig) => (
  syntaxTree: UnformattedSyntaxTree,
): FormattedSyntaxTree => {
  switch (syntaxTree.type) {
    case TokenType.NonGremlinCode:
      return formatNonGremlin(config)(syntaxTree);
    case TokenType.Traversal:
      return formatTraversal(formatSyntaxTree)(config)(syntaxTree);
    case TokenType.Method:
      return formatMethod(formatSyntaxTree)(config)(syntaxTree);
    case TokenType.Closure:
      return formatClosure(formatSyntaxTree)(config)(syntaxTree);
    case TokenType.String:
      return formatString(config)(syntaxTree);
    case TokenType.Word:
      return formatWord(config)(syntaxTree);
  }
};

export const formatSyntaxTrees = (config: GremlintInternalConfig) => (syntaxTrees: UnformattedSyntaxTree[]) => {
  return syntaxTrees.map(formatSyntaxTree(config));
};
