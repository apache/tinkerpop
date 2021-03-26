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

import React from 'react';
import styled from 'styled-components';
import NavigationButton from './NavigationButton';
import { white } from '../styleVariables';

const NavigatorWrapper = styled.div`
  background: ${white};
  box-shadow: ${white} 0 0 10px;
  position: fixed;
  top: 0;
  left: 0;
  right: 0;
  z-index: 1;
`;

const NavigatorCenterContainer = styled.div`
  width: min(800px, 100vw);
  margin-left: calc(50vw - min(400px, 50vw));
`;

const Spacer = styled.div`
  height: 40px;
`;

type NavigatorProps = {
  matchedRoute: string;
};

const Navigator = ({ matchedRoute }: NavigatorProps) => (
  <div>
    <NavigatorWrapper>
      <NavigatorCenterContainer>
        <NavigationButton isSelected={matchedRoute === '/'} label="Query formatter" href="#/" />
        <NavigationButton isSelected={matchedRoute === '/style-guide'} label="Style guide" href="#/style-guide" />
      </NavigatorCenterContainer>
    </NavigatorWrapper>
    <Spacer />
  </div>
);

export default Navigator;
