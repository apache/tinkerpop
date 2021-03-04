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
import { isTraversalSource } from './utils';

// If it is the first step in a group and also not the last one, format it
// with indentation, otherwise, remove the indentation
const reduceFirstStepInStepGroup = (formatSyntaxTree: GremlinSyntaxTreeFormatter, config: GremlintInternalConfig) => (
  {
    stepsInStepGroup,
    stepGroups,
  }: {
    stepsInStepGroup: FormattedSyntaxTree[];
    stepGroups: GremlinStepGroup[];
  },
  step: UnformattedSyntaxTree,
) => {
  const localIndentation =
    config.localIndentation + (stepGroups[0] && isTraversalSource(stepGroups[0].steps[0]) ? 2 : 0);

  const isFirstStepGroup = stepGroups.length === 0;

  // It is the first step in a group and should start with a dot if it is
  // not the first stepGroup and config.shouldPlaceDotsAfterLineBreaks
  const shouldStartWithDot = !isFirstStepGroup && config.shouldPlaceDotsAfterLineBreaks;

  // It is the first step in a group, but not the last, so it should not
  // end with a dot.
  const shouldEndWithDot = false;

  return {
    stepsInStepGroup: [
      formatSyntaxTree(
        pipe(
          withIndentation(localIndentation),
          withDotInfo({ shouldStartWithDot, shouldEndWithDot }),
          withHorizontalPosition(localIndentation),
        )(config),
      )(step),
    ],
    stepGroups,
  };
};

export default reduceFirstStepInStepGroup;
