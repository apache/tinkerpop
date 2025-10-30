<!--

 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.

-->
# TinkerPop Upgrade Risk Assessment Guide

Upgrading to TinkerPop versions can sometimes be a challenging process depending on the scope of changes for a
particular version and the size and complexity of a particular codebase being upgraded. While TinkerPop provides Upgrade
Documentation which outlines all the changes introduced, that still leaves the reader with the heavy job of determining
where in their codebase the changes will need to be applied. TinkerPop provides a purpose-built prompt designed for use
with advanced coding agents, such as Claude, GitHub Copilot, or Amazon Q Developer, which can intelligently analyze your
project for areas that may break or behave unexpectedly after the upgrade. Leveraging AI, minimizes time spent on manual
review, reduces the likelihood of missing subtle incompatibilities, and provides actionable advice for adapting existing
code to the new version. This approach enables upgrades with greater confidence, efficiency, and accuracy, ensuring that
all significant compatibility issues are surfaced and addressed early in the migration process.

This directory contains tools and testing to help build this release version-specific prompt and has the following
structure:

```
upgrade
|-java [Sample application to test prompts against]
|-prompts [LLM prompts and associated context]
|-spec [The specification for the sample application]
```