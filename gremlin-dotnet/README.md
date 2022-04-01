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

# Gremlin.Net

[Apache TinkerPop™][tk] is a graph computing framework for both graph databases (OLTP) and graph analytic systems
(OLAP). [Gremlin][gremlin] is the graph traversal language of TinkerPop. It can be described as a functional,
data-flow language that enables users to succinctly express complex traversals on (or queries of) their application's
property graph.

Gremlin.Net implements Gremlin within the C# language. It targets .NET Standard and can therefore be used on different
operating systems and with different .NET frameworks, such as .NET Framework and .NET Core.

```bash
nuget install Gremlin.Net
```

The Gremlin language allows users to write highly expressive graph traversals and has a broad list of functions that 
cover a wide body of features. The [Reference Documentation][steps] describes these functions and other aspects of the 
TinkerPop ecosystem including some specifics on [Gremlin in .NET][docs] itself. Most of the examples found in the 
documentation use Groovy language syntax in the [Gremlin Console][console]. For the most part, these examples
should generally translate to C# with [some logical modification][differences]. Given the strong correspondence 
between canonical Gremlin in Java and its variants like C#, there is a limited amount of C#-specific 
documentation and examples. This strong correspondence among variants ensures that the general Gremlin reference 
documentation is applicable to all variants and that users moving between development languages can easily adopt the 
Gremlin variant for that language.

__NOTE__ that versions suffixed with "-rc" are considered release candidates (i.e. pre-alpha, alpha, beta, etc.) and thus
for early testing purposes only. These releases are not suitable for production.

[tk]: https://tinkerpop.apache.org
[gremlin]: https://tinkerpop.apache.org/gremlin.html
[docs]: https://tinkerpop.apache.org/docs/current/reference/#gremlin-dotnet
[console]: https://tinkerpop.apache.org/docs/current/tutorials/the-gremlin-console/
[steps]: https://tinkerpop.apache.org/docs/current/reference/#graph-traversal-steps
[differences]: https://tinkerpop.apache.org/docs/current/reference/#gremlin-javascript-differences
