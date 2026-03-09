/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import assert from 'assert';
import { GremlinTranslator } from '../../../lib/language/translator/GremlinTranslator.js';
import { TranslatorException } from '../../../lib/language/translator/TranslatorException.js';
import { Translator } from '../../../lib/language/translator/Translator.js';
import TranslateVisitor from '../../../lib/language/translator/TranslateVisitor.js';
import JavascriptTranslateVisitor from '../../../lib/language/translator/JavascriptTranslateVisitor.js';
import PythonTranslateVisitor from '../../../lib/language/translator/PythonTranslateVisitor.js';
import GoTranslateVisitor from '../../../lib/language/translator/GoTranslateVisitor.js';
import DotNetTranslateVisitor from '../../../lib/language/translator/DotNetTranslateVisitor.js';
import JavaTranslateVisitor from '../../../lib/language/translator/JavaTranslateVisitor.js';
import GroovyTranslateVisitor from '../../../lib/language/translator/GroovyTranslateVisitor.js';
import AnonymizedTranslateVisitor from '../../../lib/language/translator/AnonymizedTranslateVisitor.js';

describe('GremlinTranslator', function () {
  describe('basic translation', function () {
    const tests = [
      // basic traversals
      ['g.V()', 'g.V()'],
      ['g.V(1)', 'g.V(1)'],
      ['g.V("name")', 'g.V("name")'],
      ['g.V().hasLabel("person")', 'g.V().hasLabel("person")'],
      ['g.V().out("knows")', 'g.V().out("knows")'],
      // reserved word mapping
      ['g.V().in("knows")', 'g.V().in_("knows")'],
      ['g.V().from("a")', 'g.V().from_("a")'],
      // anonymous traversal
      ['g.V().where(__.out("knows"))', 'g.V().where(__.out("knows"))'],
      // enum explicit naming
      ['g.V().order().by(T.id)', 'g.V().order().by(T.id)'],
      ['g.V().order().by(Order.asc)', 'g.V().order().by(Order.asc)'],
      // predicates
      ['g.V().has("age", P.gt(30))', 'g.V().has("age", P.gt(30))'],
      ['g.V().has("name", TextP.containing("mar"))', 'g.V().has("name", TextP.containing("mar"))'],
      // integer suffix stripping
      ['g.V().range(0, 10l)', 'g.V().range(0, 10)'],
      // float suffix stripping
      ['g.V().has("weight", P.gt(0.5f))', 'g.V().has("weight", P.gt(0.5))'],
      // Map literal
      ['g.addV("person").property(T.id, [(T.label): "person"])', 'g.addV("person").property(T.id, new Map([[T.label, "person"]]))'],
      // Set literal
      ['g.inject({1,2,3})', 'g.inject(new Set([1, 2, 3]))'],
      // Collection literal
      ['g.inject([1,2,3])', 'g.inject([1, 2, 3])'],
      // Nan/Inf
      ['g.inject(NaN)', 'g.inject(Number.NaN)'],
      ['g.inject(Infinity)', 'g.inject(Number.POSITIVE_INFINITY)'],
      ['g.inject(-Infinity)', 'g.inject(Number.NEGATIVE_INFINITY)'],
    ];

    tests.forEach(function ([input, expected]) {
      it(`translates: ${input}`, function () {
        const result = GremlinTranslator.translate(input);
        assert.strictEqual(result.getTranslated(), expected);
      });
    });
  });

  describe('custom graphTraversalSourceName', function () {
    it('uses custom source name', function () {
      const result = GremlinTranslator.translate('g.V()', 'source');
      assert.strictEqual(result.getTranslated(), 'source.V()');
    });
  });

  describe('Translator registry', function () {
    it('accepts JAVASCRIPT key', function () {
      const result = GremlinTranslator.translate('g.V()', 'g', 'JAVASCRIPT');
      assert.strictEqual(result.getTranslated(), 'g.V()');
    });

    it('has JAVASCRIPT factory', function () {
      const visitor = Translator.JAVASCRIPT('g');
      assert.ok(visitor instanceof JavascriptTranslateVisitor);
    });
  });

  describe('custom visitor', function () {
    it('accepts TranslateVisitor subclass directly', function () {
      const visitor = new JavascriptTranslateVisitor('myG');
      const result = GremlinTranslator.translate('g.V()', 'myG', visitor);
      assert.strictEqual(result.getTranslated(), 'myG.V()');
    });
  });

  describe('parameter tracking', function () {
    it('tracks variables as parameters', function () {
      const result = GremlinTranslator.translate('g.V(vid1)');
      assert.ok(result.getParameters().has('vid1'));
    });

    it('returns original query', function () {
      const query = 'g.V().hasLabel("person")';
      const result = GremlinTranslator.translate(query);
      assert.strictEqual(result.getOriginal(), query);
    });
  });

  describe('range literals throw', function () {
    it('throws TranslatorException for range literal', function () {
      assert.throws(
        () => GremlinTranslator.translate('g.inject(1..5)'),
        TranslatorException,
      );
    });
  });
});

describe('PythonTranslateVisitor', function () {
  const translate = (q) => GremlinTranslator.translate(q, 'g', 'PYTHON').getTranslated();

  describe('basic translation', function () {
    const tests = [
      // basic traversals — camelCase methods become snake_case
      ['g.V()', 'g.V()'],
      ['g.V(1)', 'g.V(1)'],
      ['g.V("name")', "g.V('name')"],
      ['g.V().hasLabel("person")', "g.V().has_label('person')"],
      ['g.V().out("knows")', "g.V().out('knows')"],
      // reserved word mapping
      ['g.V().in("knows")', "g.V().in_('knows')"],
      ['g.V().from("a")', "g.V().from_('a')"],
      ['g.V().filter(__.has("name"))', 'g.V().filter_(__.has(\'name\'))'],
      // string literals use single quotes
      ['g.V().has("name", "marko")', "g.V().has('name', 'marko')"],
      // boolean capitalisation
      ['g.V().has("active", true)', "g.V().has('active', True)"],
      ['g.V().has("active", false)', "g.V().has('active', False)"],
      // null → None
      ['g.V().has("name", null)', "g.V().has('name', None)"],
      // enum explicit naming — T.id → T.id_ in Python (id is a builtin)
      ['g.V().order().by(T.id)', 'g.V().order().by(T.id_)'],
      ['g.V().order().by(Order.asc)', 'g.V().order().by(Order.asc)'],
      // camelCase step names → snake_case
      ['g.V().valueMap()', 'g.V().value_map()'],
      ['g.V().outE()', 'g.V().out_e()'],
      // predicates
      ['g.V().has("age", P.gt(30))', "g.V().has('age', P.gt(30))"],
      ['g.V().has("name", TextP.containing("mar"))', "g.V().has('name', TextP.containing('mar'))"],
      // integer suffix stripping
      ['g.V().range(0, 10l)', 'g.V().range_(0, long(10))'],
      // float suffix stripping
      ['g.V().has("weight", P.gt(0.5f))', "g.V().has('weight', P.gt(0.5))"],
      // NaN/Inf
      ["g.inject(NaN)", "g.inject(float('nan'))"],
      ["g.inject(Infinity)", "g.inject(float('inf'))"],
      ["g.inject(-Infinity)", "g.inject(float('-inf'))"],
      // Collection literals
      ['g.inject([1,2,3])', 'g.inject([1, 2, 3])'],
      // Set literal (non-empty)
      ['g.inject({1,2,3})', 'g.inject({1, 2, 3})'],
      // Map literal — T.id → T.id_ in Python
      ['g.addV("person").property(T.id, [(T.label): "person"])', "g.add_v('person').property(T.id_, { T.label: 'person' })"],
    ];

    tests.forEach(function ([input, expected]) {
      it(`translates: ${input}`, function () {
        assert.strictEqual(translate(input), expected);
      });
    });
  });

  describe('Translator registry', function () {
    it('accepts PYTHON key', function () {
      const result = GremlinTranslator.translate('g.V()', 'g', 'PYTHON');
      assert.strictEqual(result.getTranslated(), 'g.V()');
    });

    it('has PYTHON factory', function () {
      const visitor = Translator.PYTHON('g');
      assert.ok(visitor instanceof PythonTranslateVisitor);
    });
  });

  describe('range literals throw', function () {
    it('throws TranslatorException for range literal', function () {
      assert.throws(
        () => GremlinTranslator.translate('g.inject(1..5)', 'g', 'PYTHON'),
        TranslatorException,
      );
    });
  });
});

describe('GoTranslateVisitor', function () {
  const translate = (q) => GremlinTranslator.translate(q, 'g', 'GO').getTranslated();

  describe('basic translation', function () {
    const tests = [
      // basic traversals — all step methods become PascalCase
      ['g.V()', 'g.V()'],
      ['g.V(1)', 'g.V(1)'],
      ['g.V("name")', 'g.V("name")'],
      ['g.V().hasLabel("person")', 'g.V().HasLabel("person")'],
      ['g.V().out("knows")', 'g.V().Out("knows")'],
      // anonymous traversal uses gremlingo.T__.
      ['g.V().where(__.out("knows"))', 'g.V().Where(gremlingo.T__.Out("knows"))'],
      // enum explicit naming with gremlingo. prefix
      ['g.V().order().by(T.id)', 'g.V().Order().By(gremlingo.T.Id)'],
      ['g.V().order().by(Order.asc)', 'g.V().Order().By(gremlingo.Order.Asc)'],
      // predicates get gremlingo. prefix and PascalCase method
      ['g.V().has("age", P.gt(30))', 'g.V().Has("age", gremlingo.P.Gt(30))'],
      ['g.V().has("name", TextP.containing("mar"))', 'g.V().Has("name", gremlingo.TextP.Containing("mar"))'],
      // integer suffixes become Go type casts
      ['g.V().range(0, 10l)', 'g.V().Range(0, int64(10))'],
      // float suffix
      ['g.V().has("weight", P.gt(0.5f))', 'g.V().Has("weight", gremlingo.P.Gt(float32(0.5)))'],
      // Map literal
      ['g.addV("person").property(T.id, [(T.label): "person"])', 'g.AddV("person").Property(gremlingo.T.Id, map[interface{}]interface{}{gremlingo.T.Label: "person" })'],
      // Set literal
      ['g.inject({1,2,3})', 'g.Inject(gremlingo.NewSimpleSet(1, 2, 3))'],
      // Collection literal
      ['g.inject([1,2,3])', 'g.Inject([]interface{}{1, 2, 3})'],
      // NaN/Inf
      ['g.inject(NaN)', 'g.Inject(math.NaN())'],
      ['g.inject(Infinity)', 'g.Inject(math.Inf(1))'],
      ['g.inject(-Infinity)', 'g.Inject(math.Inf(-1))'],
      // null → nil
      ['g.V().has("name", null)', 'g.V().Has("name", nil)'],
    ];

    tests.forEach(function ([input, expected]) {
      it(`translates: ${input}`, function () {
        assert.strictEqual(translate(input), expected);
      });
    });
  });

  describe('Translator registry', function () {
    it('accepts GO key', function () {
      const result = GremlinTranslator.translate('g.V()', 'g', 'GO');
      assert.strictEqual(result.getTranslated(), 'g.V()');
    });

    it('has GO factory', function () {
      const visitor = Translator.GO('g');
      assert.ok(visitor instanceof GoTranslateVisitor);
    });
  });

  describe('range literals throw', function () {
    it('throws TranslatorException for range literal', function () {
      assert.throws(
        () => GremlinTranslator.translate('g.inject(1..5)', 'g', 'GO'),
        TranslatorException,
      );
    });
  });
});

describe('DotNetTranslateVisitor', function () {
  const translate = (q) => GremlinTranslator.translate(q, 'g', 'DOTNET').getTranslated();

  describe('basic translation', function () {
    const tests = [
      // basic traversals — all step methods become PascalCase
      ['g.V()', 'g.V()'],
      ['g.V(1)', 'g.V(1)'],
      ['g.V("name")', 'g.V("name")'],
      ['g.V().hasLabel("person")', 'g.V().HasLabel("person")'],
      ['g.V().out("knows")', 'g.V().Out("knows")'],
      // anonymous traversal uses __. (no prefix)
      ['g.V().where(__.out("knows"))', 'g.V().Where(__.Out("knows"))'],
      // enum naming: prefix as-is, value capitalized
      ['g.V().order().by(T.id)', 'g.V().Order().By(T.Id)'],
      ['g.V().order().by(Order.asc)', 'g.V().Order().By(Order.Asc)'],
      // predicates: PascalCase method name
      ['g.V().has("age", P.gt(30))', 'g.V().Has("age", P.Gt(30))'],
      ['g.V().has("name", TextP.containing("mar"))', 'g.V().Has("name", TextP.Containing("mar"))'],
      // integer suffixes
      ['g.V().range(0, 10l)', 'g.V().Range<object>(0, 10l)'],
      ['g.V().range(0, 5i)', 'g.V().Range<object>(0, 5)'],
      // float suffixes — f/d kept for C#
      ['g.V().has("weight", P.gt(0.5f))', 'g.V().Has("weight", P.Gt(0.5f))'],
      // NaN / Inf
      ['g.inject(NaN)', 'g.Inject<object>(Double.NaN)'],
      ['g.inject(Infinity)', 'g.Inject<object>(Double.PositiveInfinity)'],
      ['g.inject(-Infinity)', 'g.Inject<object>(Double.NegativeInfinity)'],
      // Collections
      ['g.inject([1,2,3])', 'g.Inject<object>(new List<object> { 1, 2, 3 })'],
      ['g.inject({1,2,3})', 'g.Inject<object>(new HashSet<object> { 1, 2, 3 })'],
      // Map literal
      ['g.addV("person").property(T.id, [(T.label): "person"])', 'g.AddV((string) "person").Property(T.Id, new Dictionary<object, object> {{ T.Label, "person" }})'],
      // values gets <object>
      ['g.V().values("name")', 'g.V().Values<object>("name")'],
      // null cast
      ['g.V().has("name", null)', 'g.V().Has("name", (object) null)'],
    ];

    tests.forEach(function ([input, expected]) {
      it(`translates: ${input}`, function () {
        assert.strictEqual(translate(input), expected);
      });
    });
  });

  describe('Translator registry', function () {
    it('accepts DOTNET key', function () {
      const result = GremlinTranslator.translate('g.V()', 'g', 'DOTNET');
      assert.strictEqual(result.getTranslated(), 'g.V()');
    });

    it('has DOTNET factory', function () {
      const visitor = Translator.DOTNET('g');
      assert.ok(visitor instanceof DotNetTranslateVisitor);
    });
  });

  describe('range literals throw', function () {
    it('throws TranslatorException for range literal', function () {
      assert.throws(
        () => GremlinTranslator.translate('g.inject(1..5)', 'g', 'DOTNET'),
        TranslatorException,
      );
    });
  });
});

describe('JavaTranslateVisitor', function () {
  const translate = (q) => GremlinTranslator.translate(q, 'g', 'JAVA').getTranslated();

  describe('basic translation', function () {
    const tests = [
      // basic traversals — camelCase preserved
      ['g.V()', 'g.V()'],
      ['g.V(1)', 'g.V(1)'],
      ['g.V("name")', 'g.V("name")'],
      ['g.V().hasLabel("person")', 'g.V().hasLabel("person")'],
      ['g.V().out("knows")', 'g.V().out("knows")'],
      // anonymous traversal
      ['g.V().where(__.out("knows"))', 'g.V().where(__.out("knows"))'],
      // enum explicit naming — base class handles, as-is
      ['g.V().order().by(T.id)', 'g.V().order().by(T.id)'],
      ['g.V().order().by(Order.asc)', 'g.V().order().by(Order.asc)'],
      // predicates
      ['g.V().has("age", P.gt(30))', 'g.V().has("age", P.gt(30))'],
      ['g.V().has("name", TextP.containing("mar"))', 'g.V().has("name", TextP.containing("mar"))'],
      // integer suffixes
      ['g.V().range(0, 10l)', 'g.V().range(0, 10l)'],
      ['g.inject(1b)', 'g.inject(new Byte(1))'],
      ['g.inject(1s)', 'g.inject(new Short(1))'],
      ['g.inject(1i)', 'g.inject(1)'],
      ['g.inject(1n)', 'g.inject(new BigInteger("1"))'],
      // float suffixes
      ['g.V().has("weight", P.gt(0.5f))', 'g.V().has("weight", P.gt(0.5f))'],
      // NaN/Inf
      ['g.inject(NaN)', 'g.inject(Double.NaN)'],
      ['g.inject(Infinity)', 'g.inject(Double.POSITIVE_INFINITY)'],
      ['g.inject(-Infinity)', 'g.inject(Double.NEGATIVE_INFINITY)'],
      // Collection literals
      ['g.inject([1,2,3])', 'g.inject(new ArrayList<Object>() {{ add(1); add(2); add(3); }})'],
      // Set literal
      ['g.inject({1,2,3})', 'g.inject(new HashSet<Object>() {{ add(1); add(2); add(3); }})'],
      // Map literal
      ['g.addV("person").property(T.id, [(T.label): "person"])', 'g.addV("person").property(T.id, new LinkedHashMap<Object, Object>() {{ put(T.label, "person"); }})'],
    ];

    tests.forEach(function ([input, expected]) {
      it(`translates: ${input}`, function () {
        assert.strictEqual(translate(input), expected);
      });
    });
  });

  describe('Translator registry', function () {
    it('accepts JAVA key', function () {
      const result = GremlinTranslator.translate('g.V()', 'g', 'JAVA');
      assert.strictEqual(result.getTranslated(), 'g.V()');
    });

    it('has JAVA factory', function () {
      const visitor = Translator.JAVA('g');
      assert.ok(visitor instanceof JavaTranslateVisitor);
    });
  });

  describe('range literals throw', function () {
    it('throws TranslatorException for range literal', function () {
      assert.throws(
        () => GremlinTranslator.translate('g.inject(1..5)', 'g', 'JAVA'),
        TranslatorException,
      );
    });
  });
});

describe('GroovyTranslateVisitor', function () {
  const translate = (q) => GremlinTranslator.translate(q, 'g', 'GROOVY').getTranslated();

  describe('basic translation', function () {
    const tests = [
      // basic traversals — camelCase preserved
      ['g.V()', 'g.V()'],
      ['g.V(1)', 'g.V(1)'],
      ['g.V("name")', 'g.V("name")'],
      ['g.V().hasLabel("person")', 'g.V().hasLabel("person")'],
      ['g.V().out("knows")', 'g.V().out("knows")'],
      // anonymous traversal
      ['g.V().where(__.out("knows"))', 'g.V().where(__.out("knows"))'],
      // enum explicit naming — base class handles, as-is
      ['g.V().order().by(T.id)', 'g.V().order().by(T.id)'],
      ['g.V().order().by(Order.asc)', 'g.V().order().by(Order.asc)'],
      // predicates
      ['g.V().has("age", P.gt(30))', 'g.V().has("age", P.gt(30))'],
      ['g.V().has("name", TextP.containing("mar"))', 'g.V().has("name", TextP.containing("mar"))'],
      // integer suffixes
      ['g.V().range(0, 10l)', 'g.V().range(0, 10l)'],
      ['g.inject(1b)', 'g.inject((byte)1)'],
      ['g.inject(1s)', 'g.inject((short)1)'],
      ['g.inject(1i)', 'g.inject(1i)'],
      ['g.inject(1n)', 'g.inject(1g)'],
      // float suffixes
      ['g.V().has("weight", P.gt(0.5f))', 'g.V().has("weight", P.gt(0.5f))'],
      // NaN — base class outputs NaN as-is
      ['g.inject(NaN)', 'g.inject(NaN)'],
      // Infinity
      ['g.inject(Infinity)', 'g.inject(Double.POSITIVE_INFINITY)'],
      ['g.inject(-Infinity)', 'g.inject(Double.NEGATIVE_INFINITY)'],
      // Collection literals — base class handles as [items]
      ['g.inject([1,2,3])', 'g.inject([1, 2, 3])'],
      // Set literal — Groovy: [items] as Set
      ['g.inject({1,2,3})', 'g.inject([1, 2, 3] as Set)'],
      // Map literal — Groovy preserves parens on expression keys
      ['g.addV("person").property(T.id, [(T.label): "person"])', 'g.addV("person").property(T.id, [(T.label):"person"])'],
      // null as Map in mergeV
      ['g.mergeV(null)', 'g.mergeV((Map) null)'],
      // strategy no-args: output name as-is
      ['g.withStrategies(AdjacentToIncidentStrategy).V()', 'g.withStrategies(AdjacentToIncidentStrategy).V()'],
      // strategy with args: prepend new
      ['g.withStrategies(EdgeLabelVerificationStrategy(throwException: true, logWarning: false)).V().out()', 'g.withStrategies(new EdgeLabelVerificationStrategy(throwException:true, logWarning:false)).V().out()'],
      // withoutStrategies: no .class suffix
      ['g.withoutStrategies(EarlyLimitStrategy).V()', 'g.withoutStrategies(EarlyLimitStrategy).V()'],
    ];

    tests.forEach(function ([input, expected]) {
      it(`translates: ${input}`, function () {
        assert.strictEqual(translate(input), expected);
      });
    });
  });

  describe('Translator registry', function () {
    it('accepts GROOVY key', function () {
      const result = GremlinTranslator.translate('g.V()', 'g', 'GROOVY');
      assert.strictEqual(result.getTranslated(), 'g.V()');
    });

    it('has GROOVY factory', function () {
      const visitor = Translator.GROOVY('g');
      assert.ok(visitor instanceof GroovyTranslateVisitor);
    });
  });
});

describe('AnonymizedTranslateVisitor', function () {
  const translate = (q) => GremlinTranslator.translate(q, 'g', 'ANONYMIZED').getTranslated();

  describe('basic anonymization', function () {
    const tests = [
      // structure preserved, strings replaced
      ['g.V().hasLabel("person")', 'g.V().hasLabel(string0)'],
      // same value reuses same placeholder
      ['g.V().has("name", "name")', 'g.V().has(string0, string0)'],
      // different string values get different placeholders
      ['g.V().has("age", "marko")', 'g.V().has(string0, string1)'],
      // plain numbers — no suffix → number
      ['g.V().has("age", P.gt(30))', 'g.V().has(string0, P.gt(number0))'],
      ['g.V().has("age", P.between(26, 30))', 'g.V().has(string0, P.between(number0, number1))'],
      // integer suffix i → integer
      ['g.inject(1i)', 'g.inject(integer0)'],
      // integer suffix l → long
      ['g.inject(10l)', 'g.inject(long0)'],
      // integer suffix b → byte
      ['g.inject(1b)', 'g.inject(byte0)'],
      // integer suffix s → short
      ['g.inject(1s)', 'g.inject(short0)'],
      // integer suffix n → biginteger
      ['g.inject(1n)', 'g.inject(biginteger0)'],
      // float suffix f → float
      ['g.inject(0.5f)', 'g.inject(float0)'],
      // float suffix d → double
      ['g.inject(0.5d)', 'g.inject(double0)'],
      // float suffix m → bigdecimal
      ['g.inject(1.5m)', 'g.inject(bigdecimal0)'],
      // NaN → number
      ['g.inject(NaN)', 'g.inject(number0)'],
      // Infinity → number
      ['g.inject(Infinity)', 'g.inject(number0)'],
      ['-Infinity' + '' , 'number0'],  // handled via inject below
      // boolean
      ['g.inject(true)', 'g.inject(boolean0)'],
      ['g.inject(false)', 'g.inject(boolean0)'],
      // null → object
      ['g.inject(null)', 'g.inject(object0)'],
      // collection → list
      ['g.inject([1,2,3])', 'g.inject(list0)'],
      // set → set
      ['g.inject({1,2,3})', 'g.inject(set0)'],
      // map → map
      ['g.addV("person").property(T.id, [(T.label): "person"])', 'g.addV(string0).property(T.id, map0)'],
      // enums/traversal steps pass through unchanged
      ['g.V().order().by(T.id)', 'g.V().order().by(T.id)'],
      ['g.V().order().by(Order.asc)', 'g.V().order().by(Order.asc)'],
      // variables pass through unchanged
      ['g.V(vid1)', 'g.V(vid1)'],
    ];

    tests
      .filter(([input]) => !input.startsWith('-Infinity'))
      .forEach(function ([input, expected]) {
        it(`anonymizes: ${input}`, function () {
          assert.strictEqual(translate(input), expected);
        });
      });

    it('anonymizes: g.inject(-Infinity)', function () {
      assert.strictEqual(translate('g.inject(-Infinity)'), 'g.inject(number0)');
    });
  });

  describe('Translator registry', function () {
    it('accepts ANONYMIZED key', function () {
      const result = GremlinTranslator.translate('g.V()', 'g', 'ANONYMIZED');
      assert.strictEqual(result.getTranslated(), 'g.V()');
    });

    it('has ANONYMIZED factory', function () {
      const visitor = Translator.ANONYMIZED('g');
      assert.ok(visitor instanceof AnonymizedTranslateVisitor);
    });
  });
});
