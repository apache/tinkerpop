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

# Gremlin.Net Template

This dotnet template helps getting started with [Gremlin.Net](http://tinkerpop.apache.org/docs/current/reference/#gremlin-DotNet) - the .NET Gremlin Language Variant (GLV) of Apache TinkerPop™. It creates a new C# console project that shows how to connect to a [Gremlin Server](http://tinkerpop.apache.org/docs/current/reference/#gremlin-server) with Gremlin.Net.

## Installation

You can install the template with the dotnet CLI tool:

```bash
dotnet new -i Gremlin.Net.Template
```

## Creating a project using the template

After the template is installed, a new project based on this template can be installed:

```bash
dotnet new gremlin
```

You can specify the output directory for the new project which will then also be used as the name of the created project:

```bash
dotnet new gremlin -o MyFirstGremlinProject
```