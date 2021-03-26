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

export const rules = [
  {
    title: 'Break long queries into multiple lines',
    explanation: `What is considered too long depends on your application.
When breaking the query, not all parts of the traversal have to be broken up. First, divide the query into logical groups, based on which steps belong naturally together. For instance, every set of steps which end with an as()-step often belong together, as they together form a new essential step in the query.
    
If anoymous traversals are passed as arguments to another step, like a filter()-step, and it's causing the line to be too long, first split the line at the commas. Only if the traversal arguments are still too long, consider splitting them further.`,
    example: `// Good (80 characters max width)
g.V().hasLabel('person').where(outE("created").count().is(P.gte(2))).count()
    
// Good (50 characters max width)
g.V().
  hasLabel('person').
  where(outE("created").count().is(P.gte(2))).
  count()
    
// Good (30 characters max width)
g.V().
  hasLabel('person').
  where(
    outE("created").
    count().
    is(P.gte(2))).
  count()`,
  },
  {
    title: 'Use soft tabs (spaces) for indentation',
    explanation: 'This ensures that your code looks the same for anyone, regardless of their text editor settings.',
    example: `// Bad - indented using hard tabs
g.V().
  hasLabel('person').as('person').
  properties('location').as('location').
  select('person','location').
    by('name').
    by(valueMap())
    
// Good - indented using spaces
g.V().
∙∙hasLabel('person').as('person').
∙∙properties('location').as('location').
∙∙select('person','location').
∙∙∙∙by('name').
∙∙∙∙by(valueMap())`,
  },
  {
    title: 'Use two spaces for indentation',
    explanation:
      'Two spaces makes the intent of the indent clear, but does not waste too much space. Of course, more spaces are allowed when indenting from an already indented block of code.',
    example: `// Bad - Indented using four spaces
g.V().
    hasLabel('person').as('person').
    properties('location').as('location').
    select('person','location').
        by('name').
        by(valueMap())
// Good - Indented using two spaces
g.V().
  hasLabel('person').as('person').
  properties('location').as('location').
  select('person','location').
    by('name').
    by(valueMap())`,
  },
  {
    title: 'Use indents wisely',
    explanation: `No newline should ever have the same indent as the line starting with the traversal source g.
Use indents when the step in the new line is a modulator of a previous line.
Use indents when the content in the new line is an argument of a previous step.
If multiple anonymous traversals are passed as arguments to a function, each newline which is not the first step of the traversal should be indented to make it more clear where the distinction between each argument goes. If this is the case, but the newline would already be indented because the step in the content in the new line is the argument of a previous step, there is no need to double-indent.
Don't be tempted to add extra indentation to vertically align a step with a step in a previous line.`,
    example: `// Bad - No newline should have the same indent as the line starting with the traversal source g
g.V().
group().
by().
by(bothE().count())
// Bad - Modulators of a step on a previous line should be indented
g.V().
  group().
  by().
  by(bothE().count())
// Good
g.V().
  group().
    by().
    by(bothE().count())
// Bad - You have ignored the indent rules to achieve the temporary satisfaction of vertical alignment
g.V().local(union(identity(),
                  bothE().count()).
            fold())
// Good
g.V().
  local(
    union(
      identity(),
      bothE().count()).
    fold())
// Bad - When multiple anonymous traversals are passed as arguments to a function, each newline which is not the first of line of the step should be indented to make it more clear where the distinction between each argument goes.
g.V().
  has('person','name','marko').
  fold().
  coalesce(
    unfold(),
    addV('person').
    property('name','marko').
    property('age',29))
// Good - We make it clear that the coalesce step takes two traversals as arguments
g.V().
  has('person','name','marko').
  fold().
  coalesce(
    unfold(),
    addV('person').
      property('name','marko').
      property('age',29))`,
  },
  {
    title: 'Keep as()-steps at the end of each line',
    explanation: `The end of the line is a natural place to assign a label to a step. It's okay if the as()-step is in the middle of the line if there are multiple consecutive label assignments, or if the line is so short that a newline doesn't make sense. Maybe a better way to put it is to not start a line with an as()-step, unless you're using it inside a match()-step of course.`,
    example: `// Bad
g.V().
  as('a').
  out('created').
  as('b').
  select('a','b')
// Good
g.V().as('a').
  out('created').as('b').
  select('a','b')
// Good
g.V().as('a').out('created').as('b').select('a','b')`,
  },
  {
    title: 'Add linebreak after punctuation, not before',
    explanation: `While adding the linebreak before the punctuation looks good in most cases, it introduces alignment problems when not all lines start with a punctuation. You never know if the next line should be indented relative to the punctuation of the previous line or the method of the previous line. Switching between having the punctuation at the start or the end of the line depending on whether it works in a particular case requires much brainpower (which we don't have), so it's better to be consistent. Adding the punctuation before the linebreak also means that you can know if you have reached the end of the query without reading the next line.`,
    example: `// Bad - Looks okay, though
g.V().has('name','marko')
     .out('knows')
     .has('age', gt(29))
     .values('name')
// Good
g.V().
  has('name','marko').
  out('knows').
  has('age', gt(29)).
  values('name')
// Bad - Punctuation at the start of the line makes the transition from filter to select to count too smooth
g.V()
  .hasLabel("person")
  .group()
    .by(values("name", "age").fold())
  .unfold()
  .filter(
    select(values)
    .count(local)
    .is(gt(1)))
// Good - Keeping punctuation at the end of each line, more clearly shows the query structure
g.V().
  hasLabel("person").
  group().
    by(values("name", "age").fold()).
  unfold().
  filter(
    select(values).
    count(local).
    is(gt(1)))`,
  },
  {
    title: 'Add linebreak and indentation for nested traversals which are long enough to span multiple lines',
    explanation: '',
    example: `// Bad - Not newlining the first argument of a function whose arguments span over multipe lines causes the arguments to not align.
g.V().
  hasLabel("person").
  groupCount().
    by(values("age").
      choose(is(lt(28)),
        constant("young"),
        choose(is(lt(30)),
          constant("old"),
          constant("very old"))))
// Bad - We talked about this in the indentation section, didn't we?
g.V().
  hasLabel("person").
  groupCount().
    by(values("age").
       choose(is(lt(28)),
              constant("young"),
              choose(is(lt(30)),
                     constant("old"),
                     constant("very old"))))
// Good
g.V().
  hasLabel("person").
  groupCount().
    by(
      values("age").
      choose(
        is(lt(28)),
        constant("young"),
        choose(
          is(lt(30)),
          constant("old"),
          constant("very old"))))`,
  },
  {
    title: 'Place all trailing parentheses on a single line instead of distinct lines',
    explanation:
      'Aligning the end parenthesis with the step to which the start parenthesis belongs might make it easier to check that the number of parentheses is correct, but looks ugly and wastes a lot of space.',
    example: `// Bad
g.V().
  hasLabel("person").
  groupCount().
    by(
      values("age").
      choose(
        is(lt(28)),
        constant("young"),
        choose(
          is(lt(30)),
          constant("old"),
          constant("very old")
        )
      )
    )
// Good
g.V().
  hasLabel("person").
  groupCount().
    by(
      values("age").
      choose(
        is(lt(28)),
        constant("young"),
        choose(
          is(lt(30)),
          constant("old"),
          constant("very old"))))`,
  },
  {
    title: 'Use // for single line comments. Place single line comments on a newline above the subject of the comment.',
    explanation: '',
    example: `// Bad
g.V().
  has('name','alice').out('bought'). // Find everything that Alice has bought
  in('bought').dedup().values('name') // Find everyone who have bought some of the same things as Alice
// Good
g.V().
  // Find everything that Alice has bought
  has('name','alice').out('bought').
  // Find everyone who have bought some of the same things as Alice
  in('bought').dedup().values('name')`,
  },
  {
    title: 'Use single quotes for strings',
    explanation:
      'Use single quotes for literal string values. If the string contains double quotes or single quotes, surround the string with the type of quote which creates the fewest escaped characters.',
    example: `// Bad - Use single quotes where possible
g.V().has("Movie", "name", "It's a wonderful life")
// Bad - Escaped single quotes are even worse than double quotes
g.V().has('Movie', 'name', 'It\\'s a wonderful life')
// Good
g.V().has('Movie', 'name', "It's a wonderful life")`,
  },
  {
    title: 'Write idiomatic Gremlin code',
    explanation: `If there is a simpler way, do it the simpler way. Use the Gremlin methods for what they're worth.`,
    example: `// Bad
g.V().outE().inV()
// Good
g.V().out()
// Bad
g.V().
  has('name', 'alice').
  outE().hasLabel('bought').inV().
  values('name')
// Good
g.V().
  has('name','alice').
  out('bought').
  values('name')
// Bad
g.V().hasLabel('person').has('name', 'alice')
// Good
g.V().has('person', 'name', 'alice')`,
  },
];
