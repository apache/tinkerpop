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
  GremlintInternalConfig,
  TokenType,
  UnformattedSyntaxTree,
} from '../../../types';
import { STEP_MODULATORS } from '../../../consts';
import recreateQueryOnelinerFromSyntaxTree from '../../../recreateQueryOnelinerFromSyntaxTree';

export const isTraversalSource = (step: UnformattedSyntaxTree | FormattedSyntaxTree): boolean => {
  return step.type === TokenType.Word && step.word === 'g';
};

export const isModulator = (step: UnformattedSyntaxTree | FormattedSyntaxTree): boolean => {
  if (step.type !== TokenType.Method && step.type !== TokenType.Closure) return false;
  if (step.method.type !== TokenType.Word) return false;
  return STEP_MODULATORS.includes(step.method.word);
};

export const isStepFirstStepInStepGroup = ({ stepsInStepGroup }: { stepsInStepGroup: FormattedSyntaxTree[] }) => {
  return !stepsInStepGroup.length;
};

const isLineTooLongWithSubsequentModulators = (config: GremlintInternalConfig) => (
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
  const stepsWithSubsequentModulators = steps.slice(index + 1).reduce(
    (aggregator, step) => {
      const { stepsInStepGroup, hasReachedFinalModulator } = aggregator;
      if (hasReachedFinalModulator) return aggregator;
      if (isModulator(step)) {
        return {
          ...aggregator,
          stepsInStepGroup: [...stepsInStepGroup, step],
        };
      }
      return { ...aggregator, hasReachedFinalModulator: true };
    },
    {
      stepsInStepGroup: [...stepsInStepGroup, step],
      hasReachedFinalModulator: false,
    },
  ).stepsInStepGroup;

  const stepGroupIndentationIncrease = (() => {
    const traversalSourceIndentationIncrease = stepGroups[0] && isTraversalSource(stepGroups[0].steps[0]) ? 2 : 0;
    const modulatorIndentationIncrease = isModulator([...stepsInStepGroup, step][0]) ? 2 : 0;
    const indentationIncrease = traversalSourceIndentationIncrease + modulatorIndentationIncrease;
    return indentationIncrease;
  })();

  const recreatedQueryWithSubsequentModulators = recreateQueryOnelinerFromSyntaxTree(
    config.localIndentation + stepGroupIndentationIncrease,
  )({
    type: TokenType.Traversal,
    steps: stepsWithSubsequentModulators,
  });

  const lineIsTooLongWithSubsequentModulators = recreatedQueryWithSubsequentModulators.length > config.maxLineLength;
  return lineIsTooLongWithSubsequentModulators;
};

// If the first step in a group is a modulator, then it must also be the last step in the group
export const shouldStepBeLastStepInStepGroup = (config: GremlintInternalConfig) => (
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
  const isFirstStepInStepGroup = !stepsInStepGroup.length;

  const isLastStep = index === steps.length - 1;
  const nextStepIsModulator = !isLastStep && isModulator(steps[index + 1]);

  const lineIsTooLongWithSubsequentModulators = isLineTooLongWithSubsequentModulators(config)(
    { stepsInStepGroup, stepGroups },
    step,
    index,
    steps,
  );

  // If the first step in a group is a modulator, then it must also be the last step in the group
  const stepShouldBeLastStepInStepGroup =
    isLastStep ||
    (isFirstStepInStepGroup && isModulator(step)) ||
    ((step.type === TokenType.Method || step.type === TokenType.Closure) &&
      !(nextStepIsModulator && !lineIsTooLongWithSubsequentModulators));
  return stepShouldBeLastStepInStepGroup;
};
