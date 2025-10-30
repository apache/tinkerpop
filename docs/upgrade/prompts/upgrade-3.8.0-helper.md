////
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
////
# TinkerPop 3.8.0 Upgrade Risk Assessment Guide

## Overview

Given the range of changes in Upgrading to TinkerPop 3.8.0 can be a complex process. While TinkerPop provides 
[Upgrade Documentation](xxx), which outlines all the changes introduced, that still leaves the reader with the heavy job
of determining where in their code bases This documentation introduces a purpose-built prompt designed for use with 
advanced coding agents, such as Claude, GitHub Copilot, and GPT-5, which can intelligently analyze your project for 
areas that may break or behave unexpectedly after the upgrade. By leveraging AI, you minimize time spent on manual 
review, reduce the likelihood of missing subtle incompatibilities, and gain actionable advice for adapting your code to 
the new version. This approach empowers you to upgrade with greater confidence, efficiency, and accuracy, ensuring that 
all significant compatibility issues are surfaced and addressed early in the migration process.

## Usage

It is advisable that you **update your code to version 3.7.4**, before using this prompt. The report produced can be 
used as a guide to fix problems yourself or potentially as a specification for a coding agent to attempt fixes on your 
behalf. In either case, it may be helpful to run this prompt after fixes have been put in place to see if it can uncover
any further problems. This prompt is not a replacement for personal understanding the 3.8.0 changes themselves in their 
entirety as coding agents may not always correctly note subtle aspects of what your Gremlin is doing.

Here are a few additional tips to consider when using this prompt:

* The prompt is written in such a way as to "analyze the entire codebase" which may not be advisable depending on that 
codebase's size. Consider adjusting that text to work within a smaller context (e.g. specific files or modules that use 
Gremlin) if the "entire codebase" is not effective.
* The prompt references a link to the full 3.8.0 Upgrade Documentation. Better results may be achieved by providing 
a subset of the documentation at a time to better isolate fixes.
* 

```text
You are an expert software migration advisor. The task is to assist a development team in upgrading a codebase from 
Apache TinkerPop 3.7.4 to 3.8.0. The 3.8.0 release introduces significant breaking changes to APIs as well as Gremlin 
language semantics.

Carefully review the official Upgrade Documentation for 3.8.0 here: https://raw.githubusercontent.com/apache/tinkerpop/refs/heads/3.8-dev/docs/src/upgrade/release-3.8.0.asciidoc

Your tasks are:
1. Thoroughly analyze the entire codebase for usages of APIs, methods, or Gremlin syntax patterns that are impacted by the breaking changes outlined in the 3.8.0 upgrade documentation.
2. Label each occurrence of an incompatibility or area of risk found with the section header from Upgrade Documentation to which it applies. We will refer to these groupings of occurrences as findings.  
3. Generate a "Apache TinkerPop 3.8.0 Risk Assessment" report using Markdown formatting that organizes each Finding by section header that can be used as a specification to fix the problems.
  a. In the introductory section of the report, include the following warning verbatim: "This report is only intended as a general guide for identifying possible upgrade issues between 3.7.4 and 3.8.0. There may be other problems to resolve. Be familiar with the [Upgrade Documentation](https://tinkerpop.apache.org/docs/3.8.0/upgrade/#_tinkerpop_3_8_0_2) and review your code carefully."
  b. For each finding, include a link that references a specific section of the Upgrade Documentation that helped form the basis for the problem found. The format of this link is `https://tinkerpop.apache.org/docs/3.8.0/upgrade/#<anchor>` where "<anchor>" refers to the asciidoc anchor associated with the section header in the Upgrade Documentation.
  c. For each occurrence, provide a confidence level for it as high, medium or low. In assigning this confidence level when Gremlin is involved, determine how well you understand the Gremlin semantics of the code in question while forming the recommendation.

Refer to the 3.8.0 upgrade documentation for all findings and recommendations. Do not provide other advice for upgrade
that is not specific to Apache TinkerPop or that is related to anything outside of the 3.8.0 upgrade itself. Do not 
include any information about risks or incompatibilities that were not found. A final summary of changes is not 
required.
```