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

import recreateQueryOnelinerFromSyntaxTree from './recreateQueryOnelinerFromSyntaxTree';
import { GremlintInternalConfig, UnformattedSyntaxTree } from './types';

/**
 * Checks if a syntax tree or a set of syntax trees can fit on a single line
 * within the maxLineLength constraint.
 */
export const willFitOnLine = (
  syntaxTree: UnformattedSyntaxTree | { type: string; steps: UnformattedSyntaxTree[] },
  config: Pick<GremlintInternalConfig, 'globalIndentation' | 'maxLineLength'>,
  horizontalPosition: number = 0,
): boolean => {
  const recreatedQuery = recreateQueryOnelinerFromSyntaxTree({ globalIndentation: 0 }, horizontalPosition)(
    syntaxTree as any,
  );
  return config.globalIndentation + recreatedQuery.length <= config.maxLineLength;
};

/**
 * Calculates the width of a syntax tree when formatted as a one-liner.
 */
export const getOneLinerWidth = (
  syntaxTree: UnformattedSyntaxTree,
  horizontalPosition: number = 0,
): number => {
  const recreatedQuery = recreateQueryOnelinerFromSyntaxTree({ globalIndentation: 0 }, horizontalPosition)(
    syntaxTree as any,
  );
  return recreatedQuery.trim().length;
};
