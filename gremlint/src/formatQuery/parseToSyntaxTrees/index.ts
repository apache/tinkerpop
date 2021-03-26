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
  UnformattedClosureCodeBlock,
  TokenType,
  UnformattedSyntaxTree,
  UnformattedNonGremlinSyntaxTree,
} from '../types';
import { last, neq, pipe } from '../utils';
import { extractGremlinQueries } from './extractGremlinQueries';

const tokenizeOnTopLevelPunctuation = (query: string): string[] => {
  let word = '';
  let parenthesesCount = 0;
  let squareBracketCount = 0;
  let curlyBracketCount = 0;
  let isInsideSingleQuoteString = false;
  query.split('').forEach((char) => {
    if (char === '(' && !isInsideSingleQuoteString) {
      parenthesesCount++;
      word += '(';
      return;
    }
    if (char === '[' && !isInsideSingleQuoteString) {
      squareBracketCount++;
      word += '[';
      return;
    }
    if (char === '{' && !isInsideSingleQuoteString) {
      curlyBracketCount++;
      word += '{';
      return;
    }
    if (char === ')' && !isInsideSingleQuoteString) {
      parenthesesCount--;
      word += ')';
      return;
    }
    if (char === ']' && !isInsideSingleQuoteString) {
      squareBracketCount--;
      word += ']';
      return;
    }
    if (char === '}' && !isInsideSingleQuoteString) {
      curlyBracketCount--;
      word += '}';
      return;
    }
    if (char === "'") {
      isInsideSingleQuoteString = !isInsideSingleQuoteString;
      word += "'";
      return;
    }
    if (char === '.') {
      word +=
        isInsideSingleQuoteString || parenthesesCount || squareBracketCount || curlyBracketCount
          ? '.'
          : String.fromCharCode(28);
      return;
    }
    word += char;
  });
  return word
    .split(String.fromCharCode(28))
    .filter((token) => token !== '')
    .map((token) => token.trim());
};

const tokenizeOnTopLevelComma = (query: string): string[] => {
  let word = '';
  let parenthesesCount = 0;
  let squareBracketsCount = 0;
  let curlyBracketsCount = 0;
  let isInsideSingleQuoteString = false;
  query.split('').forEach((char) => {
    if (char === '(' && !isInsideSingleQuoteString) {
      parenthesesCount++;
      word += '(';
      return;
    }
    if (char === '[' && !isInsideSingleQuoteString) {
      squareBracketsCount++;
      word += '[';
      return;
    }
    if (char === '{' && !isInsideSingleQuoteString) {
      curlyBracketsCount++;
      word += '{';
      return;
    }
    if (char === ')' && !isInsideSingleQuoteString) {
      parenthesesCount--;
      word += ')';
      return;
    }
    if (char === ']' && !isInsideSingleQuoteString) {
      squareBracketsCount--;
      word += ']';
      return;
    }
    if (char === '}' && !isInsideSingleQuoteString) {
      curlyBracketsCount--;
      word += '}';
      return;
    }
    if (char === "'") {
      isInsideSingleQuoteString = !isInsideSingleQuoteString;
      word += "'";
      return;
    }
    if (char === ',') {
      word +=
        isInsideSingleQuoteString || parenthesesCount || squareBracketsCount || curlyBracketsCount
          ? ','
          : String.fromCharCode(28);
      return;
    }
    word += char;
  });
  return word
    .split(String.fromCharCode(28))
    .filter((token) => token !== '')
    .map((token) => token.trim());
};

const tokenizeOnTopLevelParentheses = (query: string): string[] => {
  let word = '';
  let parenthesesCount = 0;
  let squareBracketsCount = 0;
  let curlyBracketsCount = 0;
  let isInsideSingleQuoteString = false;
  query.split('').forEach((char) => {
    if (char === '(' && !isInsideSingleQuoteString) {
      if (parenthesesCount === 0) {
        word += String.fromCharCode(28);
      }
      parenthesesCount++;
      word += '(';
      return;
    }
    if (char === '[' && !isInsideSingleQuoteString) {
      squareBracketsCount++;
      word += '[';
      return;
    }
    if (char === '{' && !isInsideSingleQuoteString) {
      curlyBracketsCount++;
      word += '{';
      return;
    }
    if (char === ')' && !isInsideSingleQuoteString) {
      parenthesesCount--;
      word += ')';
      return;
    }
    if (char === ']' && !isInsideSingleQuoteString) {
      squareBracketsCount--;
      word += ']';
      return;
    }
    if (char === '}' && !isInsideSingleQuoteString) {
      curlyBracketsCount--;
      word += '}';
      return;
    }
    if (char === "'") {
      isInsideSingleQuoteString = !isInsideSingleQuoteString;
      word += "'";
      return;
    }
    word += char;
  });
  return word
    .split(String.fromCharCode(28))
    .filter((token) => token !== '')
    .map((token) => token.trim());
};

const tokenizeOnTopLevelCurlyBrackets = (query: string): string[] => {
  let word = '';
  let parenthesesCount = 0;
  let squareBracketsCount = 0;
  let curlyBracketsCount = 0;
  let isInsideSingleQuoteString = false;
  query.split('').forEach((char) => {
    if (char === '(' && !isInsideSingleQuoteString) {
      parenthesesCount++;
      word += '(';
      return;
    }
    if (char === '[' && !isInsideSingleQuoteString) {
      squareBracketsCount++;
      word += '[';
      return;
    }
    if (char === '{' && !isInsideSingleQuoteString) {
      if (curlyBracketsCount === 0) {
        word += String.fromCharCode(28);
      }
      curlyBracketsCount++;
      word += '{';
      return;
    }
    if (char === ')' && !isInsideSingleQuoteString) {
      parenthesesCount--;
      word += ')';
      return;
    }
    if (char === ']' && !isInsideSingleQuoteString) {
      squareBracketsCount--;
      word += ']';
      return;
    }
    if (char === '}' && !isInsideSingleQuoteString) {
      curlyBracketsCount--;
      word += '}';
      return;
    }
    if (char === "'") {
      isInsideSingleQuoteString = !isInsideSingleQuoteString;
      word += "'";
      return;
    }
    word += char;
  });
  return word
    .split(String.fromCharCode(28))
    .filter((token) => token !== '')
    .map((token) => token.trim());
};

const isWrappedInParentheses = (token: string): boolean => {
  if (token.length < 2) return false;
  if (token.charAt(0) !== '(') return false;
  if (token.slice(-1) !== ')') return false;
  return true;
};

const isWrappedInCurlyBrackets = (token: string): boolean => {
  if (token.length < 2) return false;
  if (token.charAt(0) !== '{') return false;
  if (token.slice(-1) !== '}') return false;
  return true;
};

const isString = (token: string): boolean => {
  if (token.length < 2) return false;
  if (token.charAt(0) !== token.substr(-1)) return false;
  if (['"', "'"].includes(token.charAt(0))) return true;
  return false;
};

const isMethodInvocation = (token: string): boolean => {
  return pipe(tokenizeOnTopLevelParentheses, last, isWrappedInParentheses)(token);
};

const isClosureInvocation = (token: string): boolean => {
  return pipe(tokenizeOnTopLevelCurlyBrackets, last, isWrappedInCurlyBrackets)(token);
};

const trimParentheses = (expression: string): string => expression.slice(1, -1);

const trimCurlyBrackets = (expression: string): string => expression.slice(1, -1);

const getMethodTokenAndArgumentTokensFromMethodInvocation = (
  token: string,
): { methodToken: string; argumentTokens: string[] } => {
  // The word before the first parenthesis is the method name
  // The token may be a double application of a curried function, so we cannot
  // assume that the first opening parenthesis is closed by the last closing
  // parenthesis
  const tokens = tokenizeOnTopLevelParentheses(token);
  return {
    methodToken: tokens.slice(0, -1).join(''),
    argumentTokens: pipe(trimParentheses, tokenizeOnTopLevelComma)(tokens.slice(-1)[0]),
  };
};

const getIndentation = (lineOfCode: string): number => lineOfCode.split('').findIndex(neq(' '));

const getMethodTokenAndClosureCodeBlockFromClosureInvocation = (
  token: string,
  fullQuery: string,
): { methodToken: string; closureCodeBlock: UnformattedClosureCodeBlock } => {
  // The word before the first curly bracket is the method name
  // The token may be a double application of a curried function, so we cannot
  // assume that the first opening curly bracket is closed by the last closing
  // curly bracket
  const tokens = tokenizeOnTopLevelCurlyBrackets(token);
  const methodToken = tokens.slice(0, -1).join('');
  const closureCodeBlockToken = trimCurlyBrackets(tokens.slice(-1)[0]);
  const initialClosureCodeBlockIndentation = fullQuery
    .substr(0, fullQuery.indexOf(closureCodeBlockToken))
    .split('\n')
    .slice(-1)[0].length;
  return {
    methodToken,
    closureCodeBlock: trimCurlyBrackets(tokens.slice(-1)[0])
      .split('\n')
      .map((lineOfCode, i) => ({
        lineOfCode: lineOfCode.trimStart(),
        relativeIndentation:
          i === 0 ? getIndentation(lineOfCode) : getIndentation(lineOfCode) - initialClosureCodeBlockIndentation,
      })),
  };
};

const parseCodeBlockToSyntaxTree = (fullCode: string, shouldCalculateInitialHorizontalPosition?: boolean) => (
  codeBlock: string,
): UnformattedSyntaxTree => {
  const tokens = tokenizeOnTopLevelPunctuation(codeBlock);
  if (tokens.length === 1) {
    const token = tokens[0];
    if (isMethodInvocation(token)) {
      const { methodToken, argumentTokens } = getMethodTokenAndArgumentTokensFromMethodInvocation(token);
      return {
        type: TokenType.Method,
        method: parseCodeBlockToSyntaxTree(fullCode)(methodToken),
        arguments: argumentTokens.map(parseCodeBlockToSyntaxTree(fullCode)),
      };
    }
    if (isClosureInvocation(token)) {
      const { methodToken, closureCodeBlock } = getMethodTokenAndClosureCodeBlockFromClosureInvocation(token, fullCode);
      return {
        type: TokenType.Closure,
        method: parseCodeBlockToSyntaxTree(fullCode)(methodToken),
        closureCodeBlock,
      };
    }
    if (isString(token)) {
      return {
        type: TokenType.String,
        string: token,
      };
    }
    return {
      type: TokenType.Word,
      word: token,
    };
  }
  return {
    type: TokenType.Traversal,
    steps: tokens.map(parseCodeBlockToSyntaxTree(fullCode)),
    initialHorizontalPosition: shouldCalculateInitialHorizontalPosition
      ? fullCode.substr(0, fullCode.indexOf(codeBlock)).split('\n').slice(-1)[0].length
      : 0,
  };
};

export const parseNonGremlinCodeToSyntaxTree = (code: string): UnformattedNonGremlinSyntaxTree => ({
  type: TokenType.NonGremlinCode,
  code,
});

export const parseToSyntaxTrees = (code: string): UnformattedSyntaxTree[] => {
  const queries = extractGremlinQueries(code);
  const { syntaxTrees, remainingCode } = queries.reduce(
    (state, query: string): { syntaxTrees: UnformattedSyntaxTree[]; remainingCode: string } => {
      const indexOfQuery = state.remainingCode.indexOf(query);
      const nonGremlinCode = state.remainingCode.substr(0, indexOfQuery);
      return {
        syntaxTrees: [
          ...state.syntaxTrees,
          parseNonGremlinCodeToSyntaxTree(nonGremlinCode),
          parseCodeBlockToSyntaxTree(code, true)(query),
        ],
        remainingCode: state.remainingCode.substr(indexOfQuery + query.length),
      };
    },
    { syntaxTrees: [] as UnformattedSyntaxTree[], remainingCode: code },
  );
  if (!remainingCode) return syntaxTrees;
  return [...syntaxTrees, parseNonGremlinCodeToSyntaxTree(remainingCode)];
};
