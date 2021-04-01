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

import React, { useEffect, useState } from 'react';
import styled from 'styled-components';
import { white } from '../styleVariables';
import gremlintLoadingLogoColored from '../gremlint-loading-logo-colored.png';
import gremlintLoadingLogoGrayscale from '../gremlint-loading-logo-grayscale.png';

const LoadingAnimationWrapper = styled.div`
  position: fixed;
  background: ${white};
  top: 0;
  right: 0;
  bottom: 0;
  left: 0;
  z-index: 2;
`;

const GrayscaleImageWrapper = styled.div`
  height: 100%;
  width: 100%;
  position: absolute;
  bottom: calc(50vh - 25vmin);
`;

const ColoredImageWrapper = styled.div<{ $loadingCompletion: number }>`
  overflow: hidden;
  height: ${({ $loadingCompletion }) => $loadingCompletion / 2}vmin;
  width: 100%;
  position: absolute;
  bottom: calc(50vh - 25vmin);
`;

const Image = styled.img<{ $opacity: number }>`
  opacity: ${({ $opacity }) => $opacity};
  transition: 0.25s;
  height: 50vmin;
  width: 50vmin;
  display: block;
  margin: auto;
  position: absolute;
  bottom: 0;
  left: 50%;
  transform: translate(-50%, 0);
`;

type LoadingAnimationProps = {
  onLoadingComplete: VoidFunction;
};

const LoadingAnimation = ({ onLoadingComplete }: LoadingAnimationProps) => {
  const [loadingCompletion, setLoadingCompletion] = useState(0);
  const [coloredImageHasLoaded, setColoredImageHasLoaded] = useState(false);
  const [grayscaleImageHasLoaded, setGrayscaleImageHasLoaded] = useState(false);

  useEffect(() => {
    setTimeout(
      () => {
        if (loadingCompletion < 100) {
          if (coloredImageHasLoaded && grayscaleImageHasLoaded) {
            setLoadingCompletion(loadingCompletion + 1);
          }
        } else {
          setTimeout(onLoadingComplete, 250);
        }
      },
      loadingCompletion === 0 ? 250 : 10,
    );
  }, [loadingCompletion, coloredImageHasLoaded, grayscaleImageHasLoaded, onLoadingComplete]);

  return (
    <LoadingAnimationWrapper>
      <GrayscaleImageWrapper>
        <Image
          src={gremlintLoadingLogoGrayscale}
          $opacity={grayscaleImageHasLoaded && loadingCompletion !== 100 ? 1 : 0}
          onLoad={() => setGrayscaleImageHasLoaded(true)}
        />
      </GrayscaleImageWrapper>
      <ColoredImageWrapper $loadingCompletion={loadingCompletion}>
        <Image
          src={gremlintLoadingLogoColored}
          $opacity={loadingCompletion !== 100 ? 1 : 0}
          onLoad={() => setColoredImageHasLoaded(true)}
        />
      </ColoredImageWrapper>
    </LoadingAnimationWrapper>
  );
};

export default LoadingAnimation;
