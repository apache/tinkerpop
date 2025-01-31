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

import styled from 'styled-components';
import CenteredContainer from './CenteredContainer';
import { textColor } from '../styleVariables';
import lockFile from '../../package-lock.json';

const gremlintVersion = lockFile.packages['node_modules/gremlint'].version;

const FooterContent = styled.div`
  padding: 10px;
  color: ${textColor};
  font-size: 15px;
  text-align: center;
  line-height: 20px;
`;

const Footer = () => (
  <CenteredContainer>
    <FooterContent>
      <p>Gremlint version: {gremlintVersion}</p>
      <p>Copyright © 2015-2025 The Apache Software Foundation.</p>
    </FooterContent>
  </CenteredContainer>
);

export default Footer;
