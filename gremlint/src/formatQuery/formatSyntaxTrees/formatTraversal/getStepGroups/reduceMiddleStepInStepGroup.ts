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
import { withDotInfo, withHorizontalPosition, withZeroIndentation } from '../../utils';

const reduceMiddleStepInStepGroup = (formatSyntaxTree: GremlinSyntaxTreeFormatter, config: GremlintInternalConfig) => (
  {
    stepsInStepGroup,
    stepGroups,
  }: {
    stepsInStepGroup: FormattedSyntaxTree[];
    stepGroups: GremlinStepGroup[];
  },
  step: UnformattedSyntaxTree,
) => {
  const horizontalPosition =
    config.localIndentation + stepsInStepGroup.map(({ width }) => width).reduce(sum, 0) + stepsInStepGroup.length;

  return {
    stepsInStepGroup: [
      ...stepsInStepGroup,
      formatSyntaxTree(
        pipe(
          withZeroIndentation,
          withDotInfo({ shouldStartWithDot: false, shouldEndWithDot: false }),
          withHorizontalPosition(horizontalPosition),
        )(config),
      )(step),
    ],
    stepGroups,
  };
};

export default reduceMiddleStepInStepGroup;
