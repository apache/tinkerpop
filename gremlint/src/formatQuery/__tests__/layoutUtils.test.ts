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

import { willFitOnLine, getOneLinerWidth } from '../layoutUtils';
import { TokenType, UnformattedSyntaxTree } from '../types';

describe('layoutUtils', () => {
  const config = {
    globalIndentation: 0,
    maxLineLength: 20,
  };

  const simpleWord: UnformattedSyntaxTree = {
    type: TokenType.Word,
    word: 'g.V()',
  };

  const longWord: UnformattedSyntaxTree = {
    type: TokenType.Word,
    word: 'veryLongTraversalDefinition',
  };

  describe('willFitOnLine', () => {
    test('should return true if query fits', () => {
      expect(willFitOnLine(simpleWord, config)).toBe(true);
    });

    test('should return false if query exceeds maxLineLength', () => {
      expect(willFitOnLine(longWord, config)).toBe(false);
    });

    test('should account for global indentation', () => {
      const indentedConfig = { ...config, globalIndentation: 16 };
      expect(willFitOnLine(simpleWord, indentedConfig)).toBe(false);
    });

    test('should account for horizontal position', () => {
      expect(willFitOnLine(simpleWord, config, 16)).toBe(false);
    });
  });

  describe('getOneLinerWidth', () => {
    test('should return the correct width', () => {
      expect(getOneLinerWidth(simpleWord)).toBe(5);
    });

    test('should account for horizontal position in width if applicable (though usually trimmed)', () => {
       // getOneLinerWidth trims the result
       expect(getOneLinerWidth(simpleWord, 10)).toBe(5);
    });
  });
});
