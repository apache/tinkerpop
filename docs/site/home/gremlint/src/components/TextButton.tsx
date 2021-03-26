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
import { highlightedTextColor, textColor } from '../styleVariables';

const TextButtonWrapper = styled.span`
  display: inline-block;
  padding: 10px;
  box-sizing: border-box;
`;

const TextButtonButton = styled.button`
  height: 20px;
  line-height: 20px;
  font-size: 15px;
  color: ${textColor};
  &: {
    color: ${highlightedTextColor};
  }
  background: none;
  border: none;
  cursor: pointer;
  padding: 0;
  outline: none;
`;

type TextButtonProps = {
  label: string;
  onClick: VoidFunction;
};

const TextButton = ({ label, onClick }: TextButtonProps) => (
  <TextButtonWrapper>
    <TextButtonButton onClick={onClick}>{label}</TextButtonButton>
  </TextButtonWrapper>
);

export default TextButton;
