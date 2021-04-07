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

const LEFT_WHITE_PARENTHESIS = '⦅';
const RIGHT_WHITE_PARENTHESIS = '⦆';
const LEFT_WHITE_SQUARE_BRACKET = '⟦';
const RIGHT_WHITE_SQUARE_BRACKET = '⟧';
const LEFT_WHITE_CURLY_BRACKET = '⦃';
const RIGHT_WHITE_CURLY_BRACKET = '⦄';
const WHITE_DOT = '。';

const encodeAllNestedBracketsAndDots = (code: string): string => {
  const { word } = code.split('').reduce(
    (state, char) => {
      if (char === '.') {
        return {
          ...state,
          word:
            !state.isInsideSingleQuoteString &&
            !state.parenthesesCount &&
            !state.squareBracketsCount &&
            !state.curlyBracketsCount
              ? state.word + '.'
              : state.word + WHITE_DOT,
        };
      }
      if (char === '(') {
        return {
          ...state,
          parenthesesCount: state.parenthesesCount + (state.isInsideSingleQuoteString ? 0 : 1),
          word:
            !state.isInsideSingleQuoteString &&
            !state.parenthesesCount &&
            !state.squareBracketsCount &&
            !state.curlyBracketsCount
              ? state.word + '('
              : state.word + LEFT_WHITE_PARENTHESIS,
        };
      }
      if (char === '[') {
        return {
          ...state,
          squareBracketsCount: state.squareBracketsCount + (state.isInsideSingleQuoteString ? 0 : 1),
          word:
            !state.isInsideSingleQuoteString &&
            !state.parenthesesCount &&
            !state.squareBracketsCount &&
            !state.curlyBracketsCount
              ? state.word + '['
              : state.word + LEFT_WHITE_SQUARE_BRACKET,
        };
      }
      if (char === '{') {
        return {
          ...state,
          curlyBracketsCount: state.curlyBracketsCount + (state.isInsideSingleQuoteString ? 0 : 1),
          word:
            !state.isInsideSingleQuoteString &&
            !state.parenthesesCount &&
            !state.squareBracketsCount &&
            !state.curlyBracketsCount
              ? state.word + '{'
              : state.word + LEFT_WHITE_CURLY_BRACKET,
        };
      }
      if (char === ')') {
        return {
          ...state,
          parenthesesCount: state.parenthesesCount - (state.isInsideSingleQuoteString ? 0 : 1),
          word:
            !state.isInsideSingleQuoteString &&
            state.parenthesesCount === 1 &&
            !state.squareBracketsCount &&
            !state.curlyBracketsCount
              ? state.word + ')'
              : state.word + RIGHT_WHITE_PARENTHESIS,
        };
      }
      if (char === ']') {
        return {
          ...state,
          squareBracketsCount: state.squareBracketsCount - (state.isInsideSingleQuoteString ? 0 : 1),
          word:
            !state.isInsideSingleQuoteString &&
            !state.parenthesesCount &&
            state.squareBracketsCount === 1 &&
            !state.curlyBracketsCount
              ? state.word + ']'
              : state.word + RIGHT_WHITE_SQUARE_BRACKET,
        };
      }
      if (char === '}') {
        return {
          ...state,
          curlyBracketsCount: state.curlyBracketsCount - (state.isInsideSingleQuoteString ? 0 : 1),
          word:
            !state.isInsideSingleQuoteString &&
            !state.parenthesesCount &&
            !state.squareBracketsCount &&
            state.curlyBracketsCount === 1
              ? state.word + '}'
              : state.word + RIGHT_WHITE_CURLY_BRACKET,
        };
      }
      if (char === "'") {
        return {
          ...state,
          isInsideSingleQuoteString: !state.isInsideSingleQuoteString,
          word: state.word + "'",
        };
      }
      return {
        ...state,
        word: state.word + char,
      };
    },
    { word: '', parenthesesCount: 0, squareBracketsCount: 0, curlyBracketsCount: 0, isInsideSingleQuoteString: false },
  );
  return word;
};

const decodeEncodedBracketsAndDots = (code: string) => {
  return code
    .split(WHITE_DOT)
    .join('.')
    .split(LEFT_WHITE_PARENTHESIS)
    .join('(')
    .split(RIGHT_WHITE_PARENTHESIS)
    .join(')')
    .split(LEFT_WHITE_SQUARE_BRACKET)
    .join('[')
    .split(RIGHT_WHITE_SQUARE_BRACKET)
    .join(']')
    .split(LEFT_WHITE_CURLY_BRACKET)
    .join('{')
    .split(RIGHT_WHITE_CURLY_BRACKET)
    .join('}');
};

const SPACE = `\\s`;
const HORIZONTAL_SPACE = `[^\\S\\r\\n]`;
const DOT = `\\.`;
const METHOD_STEP = `\\w+${HORIZONTAL_SPACE}*\\([^\\)]*\\)`;
const CLOSURE_STEP = `\\w+${HORIZONTAL_SPACE}*\\{[^\\}]*\\}`;
const WORD_STEP = `\\w+`;
const GREMLIN_STEP = `(${METHOD_STEP}|${CLOSURE_STEP}|${WORD_STEP})`;
const STEP_CONNECTOR = `(${SPACE}*${DOT}${SPACE}*)`;
const GREMLIN_QUERY = `g(${STEP_CONNECTOR}${GREMLIN_STEP})+`;

const gremlinQueryRegExp = new RegExp(GREMLIN_QUERY, 'g');

export const extractGremlinQueries = (code: string) => {
  const encodedCode = encodeAllNestedBracketsAndDots(code);
  const gremlinQueries = encodedCode.match(gremlinQueryRegExp);
  if (!gremlinQueries) return [];
  return gremlinQueries.map(decodeEncodedBracketsAndDots);
};
