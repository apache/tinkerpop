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

import { FormattedSyntaxTree, GremlintInternalConfig, TokenType } from './types';
import { eq, spaces } from './utils';

const recreateQueryStringFromFormattedSyntaxTree = (syntaxTree: FormattedSyntaxTree): string => {
  if (syntaxTree.type === TokenType.NonGremlinCode) {
    return syntaxTree.code;
  }
  if (syntaxTree.type === TokenType.Traversal) {
    return syntaxTree.stepGroups
      .map((stepGroup) => stepGroup.steps.map(recreateQueryStringFromFormattedSyntaxTree).join('.'))
      .join('\n');
  }
  if (syntaxTree.type === TokenType.Method) {
    return (
      (syntaxTree.shouldStartWithDot ? '.' : '') +
      [
        recreateQueryStringFromFormattedSyntaxTree(syntaxTree.method) + '(',
        syntaxTree.argumentGroups
          .map((args) => args.map(recreateQueryStringFromFormattedSyntaxTree).join(', '))
          .join(',\n') +
          ')' +
          (syntaxTree.shouldEndWithDot ? '.' : ''),
      ].join(syntaxTree.argumentsShouldStartOnNewLine ? '\n' : '')
    );
  }
  if (syntaxTree.type === TokenType.Closure) {
    return (
      (syntaxTree.shouldStartWithDot ? '.' : '') +
      recreateQueryStringFromFormattedSyntaxTree(syntaxTree.method) +
      '{' +
      syntaxTree.closureCodeBlock
        .map(({ lineOfCode, localIndentation }, i) => `${spaces(localIndentation)}${lineOfCode}`)
        .join('\n') +
      '}' +
      (syntaxTree.shouldEndWithDot ? '.' : '')
    );
  }
  if (syntaxTree.type === TokenType.String) {
    return spaces(syntaxTree.localIndentation) + syntaxTree.string;
  }
  if (syntaxTree.type === TokenType.Word) {
    return (
      spaces(syntaxTree.localIndentation) +
      (syntaxTree.shouldStartWithDot ? '.' : '') +
      syntaxTree.word +
      (syntaxTree.shouldEndWithDot ? '.' : '')
    );
  }
  // The following line is just here to convince TypeScript that the return type
  // is string and not string | undefined.
  return '';
};

const withIndentationIfNotEmpty = (indentation: number) => (lineOfCode: string): string => {
  if (!lineOfCode) return lineOfCode;
  return spaces(indentation) + lineOfCode;
};

const lineIsEmpty = (lineOfCode: string): boolean => {
  return lineOfCode.split('').every(eq(' '));
};

const removeIndentationFromEmptyLines = (lineOfCode: string): string => {
  if (lineIsEmpty(lineOfCode)) return '';
  return lineOfCode;
};

export const recreateQueryStringFromFormattedSyntaxTrees = ({ globalIndentation }: GremlintInternalConfig) => (
  syntaxTrees: FormattedSyntaxTree[],
): string => {
  return syntaxTrees
    .map(recreateQueryStringFromFormattedSyntaxTree)
    .join('')
    .split('\n')
    .map(withIndentationIfNotEmpty(globalIndentation))
    .map(removeIndentationFromEmptyLines)
    .join('\n');
};
