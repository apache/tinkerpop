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

import reduceSingleStepInStepGroup from './reduceSingleStepInStepGroup';
import reduceLastStepInStepGroup from './reduceLastStepInStepGroup';
import reduceFirstStepInStepGroup from './reduceFirstStepInStepGroup';
import reduceMiddleStepInStepGroup from './reduceMiddleStepInStepGroup';
import { isStepFirstStepInStepGroup, shouldStepBeLastStepInStepGroup } from './utils';
import { choose } from '../../../utils';
import {
  GremlinStepGroup,
  FormattedSyntaxTree,
  GremlinSyntaxTreeFormatter,
  GremlintInternalConfig,
  UnformattedSyntaxTree,
} from '../../../types';

export const getStepGroups = (
  formatSyntaxTree: GremlinSyntaxTreeFormatter,
  steps: UnformattedSyntaxTree[],
  config: GremlintInternalConfig,
): GremlinStepGroup[] => {
  return steps.reduce(
    choose(
      shouldStepBeLastStepInStepGroup(config),
      choose(
        isStepFirstStepInStepGroup,
        reduceSingleStepInStepGroup(formatSyntaxTree, config),
        reduceLastStepInStepGroup(formatSyntaxTree, config),
      ),
      choose(
        isStepFirstStepInStepGroup,
        reduceFirstStepInStepGroup(formatSyntaxTree, config),
        reduceMiddleStepInStepGroup(formatSyntaxTree, config),
      ),
    ),
    {
      stepsInStepGroup: [] as FormattedSyntaxTree[],
      stepGroups: [] as GremlinStepGroup[],
    },
  ).stepGroups;
};
