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

import recreateQueryOnelinerFromSyntaxTree from '../../recreateQueryOnelinerFromSyntaxTree';
import {
  FormattedSyntaxTree,
  FormattedTraversalSyntaxTree,
  GremlinSyntaxTreeFormatter,
  GremlintInternalConfig,
  TokenType,
  UnformattedTraversalSyntaxTree,
} from '../../types';
import { last, pipe, sum } from '../../utils';
import { withIncreasedHorizontalPosition, withZeroIndentation } from '../utils';
import { getStepGroups } from './getStepGroups';
import { isTraversalSource } from './getStepGroups/utils';

// Groups steps into step groups and adds a localIndentation property
export const formatTraversal = (formatSyntaxTree: GremlinSyntaxTreeFormatter) => (config: GremlintInternalConfig) => (
  syntaxTree: UnformattedTraversalSyntaxTree,
): FormattedTraversalSyntaxTree => {
  const initialHorizontalPositionIndentationIncrease =
    syntaxTree.steps[0] && isTraversalSource(syntaxTree.steps[0]) ? syntaxTree.initialHorizontalPosition : 0;
  const recreatedQuery = recreateQueryOnelinerFromSyntaxTree(
    config.localIndentation + initialHorizontalPositionIndentationIncrease,
  )(syntaxTree);
  if (recreatedQuery.length <= config.maxLineLength) {
    return {
      type: TokenType.Traversal,
      steps: syntaxTree.steps,
      stepGroups: [
        {
          steps: syntaxTree.steps.reduce((steps, step, stepIndex) => {
            const formattedStep =
              stepIndex === 0
                ? formatSyntaxTree(config)(step)
                : // Since the traversal's steps will be on the same line, their horizontal position is increased by the
                  // steps's width plus the width of the dots between them
                  formatSyntaxTree(
                    pipe(
                      withZeroIndentation,
                      withIncreasedHorizontalPosition(
                        syntaxTree.initialHorizontalPosition +
                          steps.map(({ width }) => width).reduce(sum, 0) +
                          steps.length,
                      ),
                    )(config),
                  )(step);
            return [...steps, formattedStep];
          }, [] as FormattedSyntaxTree[]),
        },
      ],
      initialHorizontalPosition: syntaxTree.initialHorizontalPosition,
      localIndentation: 0,
      width: recreatedQuery.trim().length,
    };
  }
  const stepGroups = getStepGroups(formatSyntaxTree, syntaxTree.steps, config);
  const lastStepGroup = last(stepGroups);
  const width = lastStepGroup
    ? lastStepGroup.steps.map(({ width }) => width).reduce(sum, 0) + stepGroups.length - 1
    : 0;
  return {
    type: TokenType.Traversal,
    steps: syntaxTree.steps,
    stepGroups,
    initialHorizontalPosition: syntaxTree.initialHorizontalPosition,
    localIndentation: 0,
    width,
  };
};
