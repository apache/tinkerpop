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
import { pipe, sum } from '../../../utils';
import { withDotInfo, withIncreasedHorizontalPosition, withZeroIndentation } from '../../utils';

// If it should be the last step in a line
// We don't want to newline after words which are not methods. For
// instance, g.V() should be one one line, as should __.as
const reduceLastStepInStepGroup = (formatSyntaxTree: GremlinSyntaxTreeFormatter, config: GremlintInternalConfig) => (
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
  const isLastStepGroup = index === steps.length - 1;
  // If it is the last (and also not first) step in a group
  // This is not the first step in the step group, so it should not
  // start with a dot
  const shouldStartWithDot = false;

  const shouldEndWithDot = !isLastStepGroup && !config.shouldPlaceDotsAfterLineBreaks;

  return {
    stepsInStepGroup: [],
    stepGroups: [
      ...stepGroups,
      {
        steps: [
          ...stepsInStepGroup,
          formatSyntaxTree(
            pipe(
              withZeroIndentation,
              withDotInfo({ shouldStartWithDot, shouldEndWithDot }),
              withIncreasedHorizontalPosition(
                // If I recall correctly, the + stepsInStepGroup.length handles the horizontal increase caused by the dots joining the steps
                stepsInStepGroup.map(({ width }) => width).reduce(sum, 0) + stepsInStepGroup.length,
              ),
            )(config),
          )(step),
        ],
      },
    ],
  };
};

export default reduceLastStepInStepGroup;
