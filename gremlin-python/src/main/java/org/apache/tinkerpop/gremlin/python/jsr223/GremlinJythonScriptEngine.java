/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.tinkerpop.gremlin.python.jsr223;

import org.apache.tinkerpop.gremlin.jsr223.CoreGremlinPlugin;
import org.apache.tinkerpop.gremlin.jsr223.CoreImports;
import org.apache.tinkerpop.gremlin.jsr223.Customizer;
import org.apache.tinkerpop.gremlin.jsr223.GremlinScriptEngine;
import org.apache.tinkerpop.gremlin.jsr223.GremlinScriptEngineFactory;
import org.apache.tinkerpop.gremlin.jsr223.ImportCustomizer;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.python.jsr223.PyScriptEngine;
import org.python.jsr223.PyScriptEngineFactory;

import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptException;
import javax.script.SimpleBindings;
import java.io.Reader;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GremlinJythonScriptEngine implements GremlinScriptEngine {

    private final PyScriptEngine pyScriptEngine;

    public GremlinJythonScriptEngine(final Customizer... customizers) {
        this.pyScriptEngine = (PyScriptEngine) new PyScriptEngineFactory().getScriptEngine();
        final List<Customizer> listOfCustomizers = new ArrayList<>(Arrays.asList(customizers));

        // always need this plugin for a scriptengine to be "Gremlin-enabled"
        CoreGremlinPlugin.instance().getCustomizers("gremlin-jython").ifPresent(c -> listOfCustomizers.addAll(Arrays.asList(c)));

        final List<ImportCustomizer> importCustomizers = listOfCustomizers.stream()
                .filter(p -> p instanceof ImportCustomizer)
                .map(p -> (ImportCustomizer) p)
                .collect(Collectors.toList());

        try {
            for (ImportCustomizer ic : importCustomizers) {
                for (Class<?> c : ic.getClassImports()) {
                    if (null == c.getDeclaringClass())
                        this.pyScriptEngine.eval("from " + c.getPackage().getName() + " import " + c.getSimpleName());
                    else
                        this.pyScriptEngine.eval("from " + c.getPackage().getName() + "." + c.getDeclaringClass().getSimpleName() + " import " + c.getSimpleName());
                }

                for (Method m : ic.getMethodImports()) {
                    this.pyScriptEngine.eval(SymbolHelper.toPython(m.getName()) + " = " + m.getDeclaringClass().getSimpleName() + "." + m.getName());
                }

                // enums need to import after methods for some reason or else label comes in as a PyReflectedFunction
                for (Enum e : ic.getEnumImports()) {
                    this.pyScriptEngine.eval(SymbolHelper.toPython(e.name()) + " = " + e.getDeclaringClass().getSimpleName() + "." + e.name());
                }
            }

            loadSugar();

        } catch (Exception ex) {
            throw new IllegalStateException(ex);
        }
    }

    @Override
    public Traversal.Admin eval(final Bytecode bytecode, final Bindings bindings, final String traversalSource) throws ScriptException {
        // these validations occur before merging in bytecode bindings which will override existing ones. need to
        // extract the named traversalsource prior to that happening so that bytecode bindings can share the same
        // namespace as global bindings (e.g. traversalsources and graphs).
        if (traversalSource.equals(HIDDEN_G))
            throw new IllegalArgumentException("The traversalSource cannot have the name " + HIDDEN_G+ " - it is reserved");

        if (bindings.containsKey(HIDDEN_G))
            throw new IllegalArgumentException("Bindings cannot include " + HIDDEN_G + " - it is reserved");

        if (!bindings.containsKey(traversalSource))
            throw new IllegalArgumentException("The bindings available to the ScriptEngine do not contain a traversalSource named: " + traversalSource);

        final Object b = bindings.get(traversalSource);
        if (!(b instanceof TraversalSource))
            throw new IllegalArgumentException(traversalSource + " is of type " + b.getClass().getSimpleName() + " and is not an instance of TraversalSource");

        final Bindings inner = new SimpleBindings();
        inner.putAll(bindings);
        inner.putAll(bytecode.getBindings());
        inner.put(HIDDEN_G, b);

        return (Traversal.Admin) this.eval(JythonTranslator.of(HIDDEN_G).translate(bytecode), inner);
    }

    @Override
    public Object eval(final String script, final ScriptContext context) throws ScriptException {
        return this.pyScriptEngine.eval(script, context);
    }

    @Override
    public Object eval(final Reader reader, final ScriptContext context) throws ScriptException {
        return this.pyScriptEngine.eval(reader, context);
    }

    @Override
    public Object eval(final String script) throws ScriptException {
        return this.pyScriptEngine.eval(script);
    }

    @Override
    public Object eval(final Reader reader) throws ScriptException {
        return this.pyScriptEngine.eval(reader);
    }

    @Override
    public Object eval(final String script, final Bindings n) throws ScriptException {
        this.pyScriptEngine.getBindings(ScriptContext.ENGINE_SCOPE).putAll(n); // TODO: groovy and jython act different
        return this.pyScriptEngine.eval(script);
    }

    @Override
    public Object eval(final Reader reader, final Bindings n) throws ScriptException {
        this.pyScriptEngine.getBindings(ScriptContext.ENGINE_SCOPE).putAll(n); // TODO: groovy and jython act different
        return this.pyScriptEngine.eval(reader);
    }

    @Override
    public void put(final String key, final Object value) {
        this.pyScriptEngine.put(key, value);
    }

    @Override
    public Object get(final String key) {
        return this.pyScriptEngine.get(key);
    }

    @Override
    public Bindings getBindings(final int scope) {
        return this.pyScriptEngine.getBindings(scope);
    }

    @Override
    public void setBindings(final Bindings bindings, final int scope) {
        this.pyScriptEngine.setBindings(bindings, scope);
    }

    @Override
    public Bindings createBindings() {
        return this.pyScriptEngine.createBindings();
    }

    @Override
    public ScriptContext getContext() {
        return this.pyScriptEngine.getContext();
    }

    @Override
    public void setContext(final ScriptContext context) {
        this.pyScriptEngine.setContext(context);
    }

    @Override
    public GremlinScriptEngineFactory getFactory() {
        return new GremlinJythonScriptEngineFactory();
    }

    private void loadSugar() throws ScriptException {
        // add sugar methods
        this.pyScriptEngine.eval("def getitem_bypass(self, index):\n" +
                "  if isinstance(index,int):\n    return self.range(index,index+1)\n" +
                "  elif isinstance(index,slice):\n    return self.range(index.start,index.stop)\n" +
                "  else:\n    return TypeError('Index must be int or slice')");
        this.pyScriptEngine.eval(GraphTraversal.class.getSimpleName() + ".__getitem__ = getitem_bypass");
        this.pyScriptEngine.eval(GraphTraversal.class.getSimpleName() + ".__getattr__ = lambda self, key: self.values(key)\n");
        this.pyScriptEngine.eval("\n" +
                "from java.lang import Long\n" +
                "import org.apache.tinkerpop.gremlin.util.function.Lambda\n" + // todo: remove or remove imported subclass names? (choose)
                "from org.apache.tinkerpop.gremlin.util.function.Lambda import AbstractLambda\n" +
                "from org.apache.tinkerpop.gremlin.util.function.Lambda import UnknownArgLambda\n" +
                "from org.apache.tinkerpop.gremlin.util.function.Lambda import ZeroArgLambda\n" +
                "from org.apache.tinkerpop.gremlin.util.function.Lambda import OneArgLambda\n" +
                "from org.apache.tinkerpop.gremlin.util.function.Lambda import TwoArgLambda\n\n" +

                "class JythonUnknownArgLambda(UnknownArgLambda):\n" +
                "  def __init__(self,func,script='none',lang='gremlin-jython'):\n" +
                "    UnknownArgLambda.__init__(self, script, lang, -1)\n" +
                "    self.func = func\n" +
                "  def __repr__(self):\n" +
                "    return self.getLambdaScript()\n\n" +

                "class JythonZeroArgLambda(ZeroArgLambda):\n" +
                "  def __init__(self,func,script='none',lang='gremlin-jython'):\n" +
                "    ZeroArgLambda.__init__(self, script, lang)\n" +
                "    self.func = func\n" +
                "  def __repr__(self):\n" +
                "    return self.getLambdaScript()\n" +
                "  def get(self):\n" +
                "    return self.func()\n\n" +

                "class JythonOneArgLambda(OneArgLambda):\n" +
                "  def __init__(self,func,script='none',lang='gremlin-jython'):\n" +
                "    OneArgLambda.__init__(self, script, lang)\n" +
                "    self.func = func\n" +
                "  def __repr__(self):\n" +
                "    return self.getLambdaScript()\n" +
                "  def test(self,a):\n" +
                "    return self.func(a)\n" +
                "  def apply(self,a):\n" +
                "    return self.func(a)\n" +
                "  def accept(self,a):\n" +
                "    self.func(a)\n" +
                "  def compare(self,a,b):\n" +
                "    return self.func(a,b)\n\n" +

                "class JythonTwoArgLambda(TwoArgLambda):\n" +
                "  def __init__(self,func,script='none',lang='gremlin-jython'):\n" +
                "    TwoArgLambda.__init__(self, script, lang)\n" +
                "    self.func = func\n" +
                "  def __repr__(self):\n" +
                "    return self.getLambdaScript()\n" +
                "  def apply(self,a,b):\n" +
                "    return self.func(a,b)\n" +
                "  def compare(self,a,b):\n" +
                "    return self.func(a,b)\n"
        );
    }
}
