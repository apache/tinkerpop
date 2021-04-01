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

import recreateQueryOnelinerFromSyntaxTree from '../recreateQueryOnelinerFromSyntaxTree';
import {
  FormattedClosureSyntaxTree,
  GremlinSyntaxTreeFormatter,
  GremlintInternalConfig,
  TokenType,
  UnformattedClosureCodeBlock,
  UnformattedClosureLineOfCode,
  UnformattedClosureSyntaxTree,
} from '../types';
import { withNoEndDotInfo } from './utils';

const getClosureLineOfCodeIndentation = (
  relativeIndentation: number,
  horizontalPosition: number,
  methodWidth: number,
  lineNumber: number,
) => {
  if (lineNumber === 0) return Math.max(relativeIndentation, 0);
  return Math.max(relativeIndentation + horizontalPosition + methodWidth + 1, 0);
};

const getFormattedClosureLineOfCode = (horizontalPosition: number, methodWidth: number) => (
  { lineOfCode, relativeIndentation }: UnformattedClosureLineOfCode,
  lineNumber: number,
) => ({
  lineOfCode,
  relativeIndentation,
  localIndentation: getClosureLineOfCodeIndentation(relativeIndentation, horizontalPosition, methodWidth, lineNumber),
});

const getFormattedClosureCodeBlock = (
  unformattedClosureCodeBlock: UnformattedClosureCodeBlock,
  horizontalPosition: number,
  methodWidth: number,
) => {
  return unformattedClosureCodeBlock.map(getFormattedClosureLineOfCode(horizontalPosition, methodWidth));
};

export const formatClosure = (formatSyntaxTree: GremlinSyntaxTreeFormatter) => (config: GremlintInternalConfig) => (
  syntaxTree: UnformattedClosureSyntaxTree,
): FormattedClosureSyntaxTree => {
  const { closureCodeBlock: unformattedClosureCodeBlock, method: unformattedMethod } = syntaxTree;
  const { localIndentation, horizontalPosition, maxLineLength, shouldEndWithDot } = config;
  const recreatedQuery = recreateQueryOnelinerFromSyntaxTree(localIndentation)(syntaxTree);
  const formattedMethod = formatSyntaxTree(withNoEndDotInfo(config))(unformattedMethod);
  const methodWidth = formattedMethod.width;

  if (recreatedQuery.length <= maxLineLength) {
    return {
      type: TokenType.Closure,
      method: formattedMethod,
      closureCodeBlock: getFormattedClosureCodeBlock(unformattedClosureCodeBlock, horizontalPosition, methodWidth),
      localIndentation,
      width: recreatedQuery.trim().length,
      shouldStartWithDot: false,
      shouldEndWithDot: Boolean(shouldEndWithDot),
    };
  }

  return {
    type: TokenType.Closure,
    method: formattedMethod,
    closureCodeBlock: getFormattedClosureCodeBlock(unformattedClosureCodeBlock, horizontalPosition, methodWidth),
    localIndentation: 0,
    width: 0,
    shouldStartWithDot: false,
    shouldEndWithDot: Boolean(shouldEndWithDot),
  };
};
