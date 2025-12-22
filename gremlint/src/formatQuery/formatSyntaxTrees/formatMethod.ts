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
  FormattedMethodSyntaxTree,
  FormattedSyntaxTree,
  GremlinSyntaxTreeFormatter,
  GremlintInternalConfig,
  TokenType,
  UnformattedMethodSyntaxTree,
} from '../types';
import { last, pipe, sum } from '../utils';
import { willFitOnLine } from '../layoutUtils';
import {
  withHorizontalPosition,
  withIncreasedHorizontalPosition,
  withIncreasedIndentation,
  withIndentation,
  withNoEndDotInfo,
  withZeroDotInfo,
  withZeroIndentation,
} from './utils';

// Groups arguments into argument groups an adds a localIndentation property
export const formatMethod = (formatSyntaxTree: GremlinSyntaxTreeFormatter) => (config: GremlintInternalConfig) => (
  syntaxTree: UnformattedMethodSyntaxTree,
): FormattedMethodSyntaxTree => {
  const method = formatSyntaxTree(withNoEndDotInfo(config))(syntaxTree.method);
  const argumentsWillNotBeWrapped = willFitOnLine(syntaxTree, config, config.horizontalPosition);
  if (argumentsWillNotBeWrapped) {
    const recreatedQuery = recreateQueryOnelinerFromSyntaxTree(config, config.horizontalPosition)(syntaxTree);
    return {
      type: TokenType.Method,
      method,
      // The arguments property is here so that the resulted syntax tree can
      // still be understood by recreateQueryOnelinerFromSyntaxTree
      arguments: syntaxTree.arguments,
      argumentGroups: [
        syntaxTree.arguments.reduce((argumentGroup: FormattedSyntaxTree[], syntaxTree) => {
          return [
            ...argumentGroup,
            formatSyntaxTree(
              // Since the method's arguments will be on the same line, their horizontal position is increased by the
              // method's width plus the width of the opening parenthesis
              pipe(
                withZeroIndentation,
                withZeroDotInfo,
                withIncreasedHorizontalPosition(
                  method.width +
                    1 +
                    (config.shouldStartWithDot ? 1 : 0) +
                    argumentGroup.map(({ width }) => width).reduce(sum, 0) +
                    argumentGroup.length,
                ),
              )(config),
            )(syntaxTree),
          ];
        }, []),
      ],
      argumentsShouldStartOnNewLine: false,
      shouldStartWithDot: false,
      shouldEndWithDot: Boolean(config.shouldEndWithDot),
      localIndentation: config.localIndentation,
      horizontalPosition: config.horizontalPosition,
      width: recreatedQuery.trim().length,
    };
  }
  // shouldEndWithDot has to reside on the method object, so the end dot can be
  // placed after the method parentheses. shouldStartWithDot has to be passed on
  // further down so the start dot can be placed after the indentation.
  const horizontalPosition = config.horizontalPosition + method.width + (config.shouldStartWithDot ? 1 : 0) + 1;

  const greedyArgumentGroups = syntaxTree.arguments.reduce(
    (acc: { groups: FormattedSyntaxTree[][]; currentGroup: FormattedSyntaxTree[] }, arg) => {
      const formattedArg = formatSyntaxTree(
        pipe(withZeroIndentation, withZeroDotInfo, withHorizontalPosition(horizontalPosition))(config),
      )(arg);
      const currentGroupWidth =
        acc.currentGroup.map(({ width }) => width).reduce(sum, 0) + Math.max(acc.currentGroup.length - 1, 0) * 2;
      const potentialGroupWidth =
        acc.currentGroup.length > 0 ? currentGroupWidth + 2 + formattedArg.width : formattedArg.width;

      if (acc.currentGroup.length > 0 && horizontalPosition + potentialGroupWidth > config.maxLineLength) {
        return {
          groups: [...acc.groups, acc.currentGroup],
          currentGroup: [
            formatSyntaxTree(
              pipe(withIndentation(horizontalPosition), withZeroDotInfo, withHorizontalPosition(horizontalPosition))(
                config,
              ),
            )(arg),
          ],
        };
      }
      return {
        ...acc,
        currentGroup: [
          ...acc.currentGroup,
          formatSyntaxTree(
            pipe(withIndentation(horizontalPosition), withZeroDotInfo, withHorizontalPosition(horizontalPosition))(
              config,
            ),
          )(arg),
        ],
      };
    },
    { groups: [], currentGroup: [] },
  );
  const argumentGroups =
    greedyArgumentGroups.currentGroup.length > 0
      ? [...greedyArgumentGroups.groups, greedyArgumentGroups.currentGroup]
      : greedyArgumentGroups.groups;

  const indentationWidth = horizontalPosition;

  const greedyFits = argumentGroups.every((group) => {
    const groupWidth = group.map(({ width }) => width).reduce(sum, 0) + (group.length - 1) * 2;
    return indentationWidth + groupWidth <= config.maxLineLength;
  });

  if (greedyFits) {
    const lastArgumentGroup = last(argumentGroups);
    // Add the width of the last line of parameters, the dots between them and the indentation of the parameters
    const width = lastArgumentGroup
      ? lastArgumentGroup.map(({ width }) => width).reduce(sum, 0) + (lastArgumentGroup.length - 1) * 2
      : 0;
    return {
      type: TokenType.Method,
      method,
      arguments: syntaxTree.arguments,
      argumentGroups,
      argumentsShouldStartOnNewLine: false,
      localIndentation: config.localIndentation,
      horizontalPosition: config.horizontalPosition,
      shouldStartWithDot: false,
      shouldEndWithDot: Boolean(config.shouldEndWithDot),
      width,
    };
  }

  // Fallback to wrap-all
  const wrapAllArgumentGroups = syntaxTree.arguments.map((step) => [
    formatSyntaxTree(
      pipe(withIndentation(horizontalPosition), withZeroDotInfo, withHorizontalPosition(horizontalPosition))(config),
    )(step),
  ]);

  const lastWrapAllArgumentGroup = last(wrapAllArgumentGroups);
  const wrapAllWidth = lastWrapAllArgumentGroup
    ? lastWrapAllArgumentGroup.map(({ width }) => width).reduce(sum, 0) + (lastWrapAllArgumentGroup.length - 1) * 2
    : 0;

  return {
    type: TokenType.Method,
    method,
    arguments: syntaxTree.arguments,
    argumentGroups: wrapAllArgumentGroups,
    argumentsShouldStartOnNewLine: false,
    localIndentation: config.localIndentation,
    horizontalPosition: config.horizontalPosition,
    shouldStartWithDot: false,
    shouldEndWithDot: Boolean(config.shouldEndWithDot),
    width: wrapAllWidth,
  };
};
