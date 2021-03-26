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
  FormattedSyntaxTree,
  GremlinStepGroup,
  GremlinSyntaxTreeFormatter,
  GremlintInternalConfig,
  UnformattedSyntaxTree,
} from '../../../types';
import { pipe } from '../../../utils';
import { withDotInfo, withHorizontalPosition, withIndentation } from '../../utils';
import { isModulator, isTraversalSource } from './utils';

// If it should be the last step in a line
// We don't want to newline after words which are not methods. For
// instance, g.V() should be one one line, as should __.as
const reduceSingleStepInStepGroup = (formatSyntaxTree: GremlinSyntaxTreeFormatter, config: GremlintInternalConfig) => (
  {
    stepsInStepGroup,
    stepGroups,
  }: {
    stepsInStepGroup: FormattedSyntaxTree[];
    stepGroups: GremlinStepGroup[];
  },
  step: UnformattedSyntaxTree,
  index: number,
  steps: UnformattedSyntaxTree[],
) => {
  const isFirstStepGroup = stepGroups.length === 0;
  const isLastStepGroup = index === steps.length - 1;
  const traversalSourceIndentationIncrease = stepGroups[0] && isTraversalSource(stepGroups[0].steps[0]) ? 2 : 0;
  const modulatorIndentationIncrease = isModulator(step) ? 2 : 0;
  const localIndentation = config.localIndentation + traversalSourceIndentationIncrease + modulatorIndentationIncrease;

  // This is the only step in the step group, so it is the first step in
  // the step group. It should only start with a dot if it is not the
  // first stepGroup and config.shouldPlaceDotsAfterLineBreaks
  const shouldStartWithDot = !isFirstStepGroup && config.shouldPlaceDotsAfterLineBreaks;

  // It is the last step in a group and should only end with dot if not
  // config.shouldPlaceDotsAfterLineBreaks this is not the last step in
  // steps
  const shouldEndWithDot = !isLastStepGroup && !config.shouldPlaceDotsAfterLineBreaks;

  return {
    stepsInStepGroup: [],
    stepGroups: [
      ...stepGroups,
      {
        steps: [
          formatSyntaxTree(
            pipe(
              withIndentation(localIndentation),
              withDotInfo({ shouldStartWithDot, shouldEndWithDot }),
              withHorizontalPosition(localIndentation + +config.shouldPlaceDotsAfterLineBreaks),
            )(config),
          )(step),
        ],
      },
    ],
  };
};

export default reduceSingleStepInStepGroup;
