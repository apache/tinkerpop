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
import { highlightColor, highlightedTextColor, textColor } from '../styleVariables';

const NavigationButtonWrapper = styled.span`
  display: inline-block;
  vertical-align: bottom;
  padding: 10px;
  box-sizing: border-box;
  height: 40px;
  width: 160px;
`;

const NavigationButtonLink = styled.a<{ $isSelected: boolean }>`
  text-decoration: none;
  display: inline-block;
  height: 20px;
  line-height: 20px;
  font-size: 15px;
  color: ${({ $isSelected }) => ($isSelected ? highlightedTextColor : textColor)};
  border-bottom: ${({ $isSelected }) => ($isSelected ? `2px solid ${highlightColor}` : 'none')};
  &:hover {
    color: ${highlightedTextColor};
  }
`;

type NavigationButtonProps = {
  isSelected: boolean;
  href: string;
  label: string;
};

const NavigationButton = ({ isSelected, href, label }: NavigationButtonProps) => (
  <NavigationButtonWrapper>
    <NavigationButtonLink href={href} $isSelected={isSelected}>
      {label}
    </NavigationButtonLink>
  </NavigationButtonWrapper>
);

export default NavigationButton;
