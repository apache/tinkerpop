<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements. See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership. The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License. You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied. See the License for the
  specific language governing permissions and limitations
  under the License.
-->

![Gremlint Github Header 1920x1024](https://user-images.githubusercontent.com/25663729/88488788-d5a73700-cf8f-11ea-9adb-03d62c77c1b7.png)

### What is Gremlint?

Gremlint is a code formatter which parses Gremlin queries and rewrites them to adhere to certain styling rules. It does so by parsing the query to an abstract syntax tree, and reprinting it from scratch.

### But why?

- To make Gremlin queries more readable
- To make your queries more beautiful
- To act as a "living" style guide

### Install Gremlint as a JavaScript / TypeScript package

Since Gremlint is not yet "published", it has to be installed from its GitHub repo:

```bash
npm install OyvindSabo/gremlint#master
```

### Basic example

```typescript
import { formatQuery } from 'gremlint';

const unformattedQuery = `g.V().has('person', 'name', 'marko').shortestPath().with(ShortestPath.target, __.has('name', 'josh')).with(ShortestPath.distance, 'weight')`;

const formattedQuery = formatQuery(unformattedQuery);

console.log(formattedQuery);
```

```
g.V().
  has('person', 'name', 'marko').
  shortestPath().
    with(ShortestPath.target, __.has('name', 'josh')).
    with(ShortestPath.distance, 'weight')
```

### Override default max line length

The default max line length is 80, but it can easily be overridden.

```typescript
import { formatQuery } from 'gremlint';

const unformattedQuery = `g.V().has('person', 'name', 'marko').shortestPath().with(ShortestPath.target, __.has('name', 'josh')).with(ShortestPath.distance, 'weight')`;

const formattedQuery = formatQuery(unformattedQuery, { maxLineLength: 50 });

console.log(formattedQuery);
```

```
g.V().
  has('person', 'name', 'marko').
  shortestPath().
    with(
      ShortestPath.target,
      __.has('name', 'josh')).
    with(ShortestPath.distance, 'weight')
```

### Other formatting options

```typescript
import { formatQuery } from 'gremlint';

const unformattedQuery = `g.V().has('person', 'name', 'marko').shortestPath().with(ShortestPath.target, __.has('name', 'josh')).with(ShortestPath.distance, 'weight')`;

const formattedQuery = formatQuery(unformattedQuery, {
  indentation: 4, // default: 0
  maxLineLength: 40, // default: 80
  shouldPlaceDotsAfterLineBreaks: true, // default: false
});

console.log(formattedQuery);
```

```
    g.V()
      .has('person', 'name', 'marko')
      .shortestPath()
        .with(
          ShortestPath.target,
          __.has('name', 'josh'))
        .with(
          ShortestPath.distance,
          'weight')
```

### Just looking for an online Gremlin query formatter?

https://gremlint.com is a website which utilizes the Gremlint library to give users an online "living" style guide for Gremlin queries. It also serves as a platform for showcasing the features of Gremlint. Its source code is available [here](https://github.com/OyvindSabo/gremlint.com).
![Gremlint V2 Screenshot](https://user-images.githubusercontent.com/25663729/88488518-f078ac00-cf8d-11ea-9e1c-01edec285751.png)

### For contributors

**Install dependencies**

`npm install`

**Lint source files**

`npm run lint`

**Format source files**

`npm run format`

**Run tests**

`npm test`

**Compile the TypeScript source code**

`npm run build`

**Bump version**

`npm version [major | minor | patch]`
