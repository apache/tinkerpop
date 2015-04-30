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
package org.apache.tinkerpop.gremlin.groovy.jsr223;

import groovy.transform.ThreadInterrupt;
import org.apache.tinkerpop.gremlin.groovy.DefaultImportCustomizerProvider;
import org.apache.tinkerpop.gremlin.groovy.EmptyImportCustomizerProvider;
import org.apache.tinkerpop.gremlin.groovy.ImportCustomizerProvider;
import org.apache.tinkerpop.gremlin.groovy.SecurityCustomizerProvider;
import org.apache.tinkerpop.gremlin.groovy.loaders.GremlinLoader;
import org.apache.tinkerpop.gremlin.groovy.plugin.Artifact;
import org.apache.tinkerpop.gremlin.groovy.plugin.GremlinPlugin;
import org.apache.tinkerpop.gremlin.groovy.plugin.GremlinPluginException;
import groovy.grape.Grape;
import groovy.lang.Binding;
import groovy.lang.Closure;
import groovy.lang.DelegatingMetaClass;
import groovy.lang.MetaClass;
import groovy.lang.MissingMethodException;
import groovy.lang.MissingPropertyException;
import groovy.lang.Script;
import groovy.lang.Tuple;
import groovy.transform.TimedInterrupt;
import org.codehaus.groovy.ast.tools.GeneralUtils;
import org.codehaus.groovy.control.CompilationFailedException;
import org.codehaus.groovy.control.CompilerConfiguration;
import org.codehaus.groovy.control.customizers.ASTTransformationCustomizer;
import org.codehaus.groovy.jsr223.GroovyCompiledScript;
import org.codehaus.groovy.jsr223.GroovyScriptEngineImpl;
import org.codehaus.groovy.runtime.InvokerHelper;
import org.codehaus.groovy.runtime.MetaClassHelper;
import org.codehaus.groovy.runtime.MethodClosure;
import org.codehaus.groovy.syntax.SyntaxException;
import org.codehaus.groovy.util.ReferenceBundle;

import javax.script.Bindings;
import javax.script.CompiledScript;
import javax.script.ScriptContext;
import javax.script.ScriptEngineFactory;
import javax.script.ScriptException;
import javax.script.SimpleBindings;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.Writer;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

/**
 * This {@code ScriptEngine} implementation is heavily adapted from the {@code GroovyScriptEngineImpl} to include
 * some additional functionality.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GremlinGroovyScriptEngine extends GroovyScriptEngineImpl implements DependencyManager, AutoCloseable {

    public static final long DEFAULT_SCRIPT_EVALUATION_TIMEOUT = 60000;
    public static final String KEY_REFERENCE_TYPE = "#jsr223.groovy.engine.keep.globals";
    public static final String REFERENCE_TYPE_PHANTOM = "phantom";
    public static final String REFERENCE_TYPE_WEAK = "weak";
    public static final String REFERENCE_TYPE_SOFT = "soft";
    public static final String REFERENCE_TYPE_HARD = "hard";

    private static final Pattern patternImportStatic = Pattern.compile("\\Aimport\\sstatic.*");

    private ThreadLocal<Boolean> registeredSandbox = new ThreadLocal<Boolean>() {
        @Override
        protected Boolean initialValue() {
            return false;
        }
    };

    /**
     * Script to generated Class map.
     */
    private ManagedConcurrentValueMap<String, Class> classMap = new ManagedConcurrentValueMap<>(ReferenceBundle.getSoftBundle());

    /**
     * Global closures map - this is used to simulate a single global functions namespace
     */
    private ManagedConcurrentValueMap<String, Closure> globalClosures = new ManagedConcurrentValueMap<>(ReferenceBundle.getHardBundle());

    protected GremlinGroovyClassLoader loader;

    private AtomicLong counter = new AtomicLong(0l);

    /**
     * The list of loaded plugins for the console.
     */
    private final Set<String> loadedPlugins = new HashSet<>();

    private volatile GremlinGroovyScriptEngineFactory factory;

    private static final String STATIC = "static";
    private static final String SCRIPT = "Script";
    private static final String DOT_GROOVY = ".groovy";
    private static final String GROOVY_LANG_SCRIPT = "groovy.lang.Script";

    private ImportCustomizerProvider importCustomizerProvider;
    private Optional<SecurityCustomizerProvider> securityProvider;

    /**
     * If this value is zero then no timeout is applied.
     */
    private final long scriptEvaluationTimeout;

    private final Set<Artifact> artifactsToUse = new HashSet<>();

    public GremlinGroovyScriptEngine() {
        this(new DefaultImportCustomizerProvider());
    }

    public GremlinGroovyScriptEngine(final ImportCustomizerProvider importCustomizerProvider) {
        this(importCustomizerProvider, null);
    }

    public GremlinGroovyScriptEngine(final ImportCustomizerProvider importCustomizerProvider,
                                     final SecurityCustomizerProvider securityCustomizerProvider) {
        this(importCustomizerProvider, securityCustomizerProvider, DEFAULT_SCRIPT_EVALUATION_TIMEOUT);
    }

    public GremlinGroovyScriptEngine(final ImportCustomizerProvider importCustomizerProvider,
                                     final SecurityCustomizerProvider securityCustomizerProvider,
                                     final long scriptEvaluationTimeout) {
        GremlinLoader.load();
        this.importCustomizerProvider = importCustomizerProvider;
        this.securityProvider = Optional.ofNullable(securityCustomizerProvider);
        this.scriptEvaluationTimeout = scriptEvaluationTimeout;
        createClassLoader();
    }

    /**
     * {@inheritDoc}
     * <p/>
     * This method should be called after "expected" imports have been added to the {@code DependencyManager}
     * because adding imports with {@link #addImports(java.util.Set)} will reset the classloader and flush away
     * dependencies.
     */
    @Override
    public synchronized List<GremlinPlugin> use(final String group, final String artifact, final String version) {
        final Map<String, Object> dependency = new HashMap<String, Object>() {{
            put("group", group);
            put("module", artifact);
            put("version", version);
        }};

        final Map<String, Object> args = new HashMap<String, Object>() {{
            put("classLoader", loader);
        }};

        Grape.grab(args, dependency);

        // note that the service loader utilized the classloader from the groovy shell as shell class are available
        // from within there given loading through Grape.
        final List<GremlinPlugin> pluginsFound = new ArrayList<>();
        ServiceLoader.load(GremlinPlugin.class, loader).forEach(pluginsFound::add);

        artifactsToUse.add(new Artifact(group, artifact, version));

        return pluginsFound;
    }

    @Override
    public void loadPlugins(final List<GremlinPlugin> plugins) throws GremlinPluginException {
        for (GremlinPlugin gremlinPlugin : plugins) {
            if (!loadedPlugins.contains(gremlinPlugin.getName())) {
                gremlinPlugin.pluginTo(new ScriptEnginePluginAcceptor(this));
                loadedPlugins.add(gremlinPlugin.getName());
            }
        }
    }

    @Override
    public Map[] dependencies() {
        return Grape.listDependencies(loader);
    }

    @Override
    public Map<String, Set<String>> imports() {
        final Map<String, Set<String>> m = new HashMap<>();
        m.put("imports", importCustomizerProvider.getImports());
        m.put("staticImports", importCustomizerProvider.getStaticImports());
        m.put("extraImports", importCustomizerProvider.getExtraImports());
        m.put("extraStaticImports", importCustomizerProvider.getExtraStaticImports());
        return m;
    }

    @Override
    public synchronized void addImports(final Set<String> importStatements) {
        final Set<String> staticImports = new HashSet<>();
        final Set<String> imports = new HashSet<>();

        importStatements.forEach(s -> {
            final String trimmed = s.trim();
            if (patternImportStatic.matcher(trimmed).matches()) {
                final int pos = trimmed.indexOf(STATIC);
                staticImports.add(s.substring(pos + 6).trim());
            } else
                imports.add(s.substring(6).trim());
        });

        // use the EmptyImportCustomizer because it doesn't come with static initializers containing
        // existing imports.
        this.importCustomizerProvider = new EmptyImportCustomizerProvider(
                this.importCustomizerProvider, imports, staticImports);
        reset();
    }

    public Set plugins() {
        return loadedPlugins;
    }

    @Override
    public void close() throws Exception {
        this.securityProvider.ifPresent(SecurityCustomizerProvider::unregisterInterceptors);
    }

    @Override
    public void reset() {
        this.securityProvider.ifPresent(SecurityCustomizerProvider::unregisterInterceptors);
        createClassLoader();

        // must clear the local cache here because the the classloader has been reset.  therefore, classes previously
        // referenced before that might not have evaluated might cleanly evaluate now.
        this.classMap.clear();
        this.globalClosures.clear();

        this.loadedPlugins.clear();

        final Set<Artifact> toReuse = new HashSet<>(artifactsToUse);
        toReuse.forEach(this::use);

        this.getContext().getBindings(ScriptContext.ENGINE_SCOPE).clear();
    }

    private void use(final Artifact artifact) {
        use(artifact.getGroup(), artifact.getArtifact(), artifact.getVersion());
    }

    @Override
    public Object eval(final Reader reader, final ScriptContext context) throws ScriptException {
        return eval(readFully(reader), context);
    }

    @Override
    public Object eval(final String script, final ScriptContext context) throws ScriptException {
        try {
            final String val = (String) context.getAttribute(KEY_REFERENCE_TYPE, ScriptContext.ENGINE_SCOPE);
            ReferenceBundle bundle = ReferenceBundle.getHardBundle();
            if (val != null && val.length() > 0) {
                if (val.equalsIgnoreCase(REFERENCE_TYPE_SOFT)) {
                    bundle = ReferenceBundle.getSoftBundle();
                } else if (val.equalsIgnoreCase(REFERENCE_TYPE_WEAK)) {
                    bundle = ReferenceBundle.getWeakBundle();
                } else if (val.equalsIgnoreCase(REFERENCE_TYPE_PHANTOM)) {
                    bundle = ReferenceBundle.getPhantomBundle();
                }
            }
            globalClosures.setBundle(bundle);
        } catch (ClassCastException cce) { /*ignore.*/ }

        try {
            final Class clazz = getScriptClass(script);
            if (null == clazz) throw new ScriptException("Script class is null");
            return eval(clazz, context);
        } catch (SyntaxException e) {
            throw new ScriptException(e.getMessage(), e.getSourceLocator(), e.getLine());
        } catch (Exception e) {
            throw new ScriptException(e);
        }
    }

    @Override
    public Bindings createBindings() {
        return new SimpleBindings();
    }

    @Override
    public ScriptEngineFactory getFactory() {
        if (this.factory == null) {
            synchronized (this) {
                if (this.factory == null) {
                    this.factory = new GremlinGroovyScriptEngineFactory();
                }
            }
        }
        return this.factory;
    }

    @Override
    public CompiledScript compile(final String scriptSource) throws ScriptException {
        try {
            return new GroovyCompiledScript(this, getScriptClass(scriptSource));
        } catch (SyntaxException e) {
            throw new ScriptException(e.getMessage(), e.getSourceLocator(), e.getLine());
        } catch (IOException e) {
            throw new ScriptException(e);
        } catch (CompilationFailedException ee) {
            throw new ScriptException(ee);
        }
    }

    @Override
    public CompiledScript compile(final Reader reader) throws ScriptException {
        return compile(readFully(reader));
    }

    @Override
    public Object invokeFunction(final String name, final Object... args) throws ScriptException, NoSuchMethodException {
        return invokeImpl(null, name, args);
    }

    @Override
    public Object invokeMethod(final Object thiz, final String name, final Object... args) throws ScriptException, NoSuchMethodException {
        if (thiz == null) {
            throw new IllegalArgumentException("Script object can not be null");
        } else {
            return invokeImpl(thiz, name, args);
        }
    }

    @Override
    public <T> T getInterface(final Class<T> clazz) {
        return makeInterface(null, clazz);
    }

    @Override
    public <T> T getInterface(final Object thiz, final Class<T> clazz) {
        if (null == thiz) throw new IllegalArgumentException("script object is null");

        return makeInterface(thiz, clazz);
    }

    Class getScriptClass(final String script) throws SyntaxException, CompilationFailedException, IOException {
        ensureSandbox();

        Class clazz = classMap.get(script);
        if (clazz != null) return clazz;

        clazz = loader.parseClass(script, generateScriptName());
        classMap.put(script, clazz);
        return clazz;
    }

    boolean isCached(final String script) {
        return classMap.get(script) != null;
    }

    Object eval(final Class scriptClass, final ScriptContext context) throws ScriptException {
        ensureSandbox();

        context.setAttribute("context", context, ScriptContext.ENGINE_SCOPE);
        final Writer writer = context.getWriter();
        context.setAttribute("out", writer instanceof PrintWriter ? writer : new PrintWriter(writer), ScriptContext.ENGINE_SCOPE);
        final Binding binding = new Binding() {
            @Override
            public Object getVariable(final String name) {
                synchronized (context) {
                    final int scope = context.getAttributesScope(name);
                    if (scope != -1) {
                        return context.getAttribute(name, scope);
                    }
                    throw new MissingPropertyException(name, getClass());
                }
            }

            @Override
            public void setVariable(final String name, final Object value) {
                synchronized (context) {
                    int scope = context.getAttributesScope(name);
                    if (scope == -1) {
                        scope = ScriptContext.ENGINE_SCOPE;
                    }
                    context.setAttribute(name, value, scope);
                }
            }
        };

        try {
            final Script scriptObject = InvokerHelper.createScript(scriptClass, binding);
            for (Method m : scriptClass.getMethods()) {
                final String name = m.getName();
                globalClosures.put(name, new MethodClosure(scriptObject, name));
            }

            final MetaClass oldMetaClass = scriptObject.getMetaClass();
            scriptObject.setMetaClass(new DelegatingMetaClass(oldMetaClass) {
                @Override
                public Object invokeMethod(final Object object, final String name, final Object args) {
                    if (args == null) {
                        return invokeMethod(object, name, MetaClassHelper.EMPTY_ARRAY);
                    } else if (args instanceof Tuple) {
                        return invokeMethod(object, name, ((Tuple) args).toArray());
                    } else if (args instanceof Object[]) {
                        return invokeMethod(object, name, (Object[]) args);
                    } else {
                        return invokeMethod(object, name, new Object[]{args});
                    }
                }

                @Override
                public Object invokeMethod(final Object object, final String name, final Object args[]) {
                    try {
                        return super.invokeMethod(object, name, args);
                    } catch (MissingMethodException mme) {
                        return callGlobal(name, args, context);
                    }
                }

                @Override
                public Object invokeStaticMethod(final Object object, final String name, final Object args[]) {
                    try {
                        return super.invokeStaticMethod(object, name, args);
                    } catch (MissingMethodException mme) {
                        return callGlobal(name, args, context);
                    }
                }
            });
            return scriptObject.run();
        } catch (Exception e) {
            throw new ScriptException(e);
        }
    }

    private void ensureSandbox() {
        if (securityProvider.isPresent() && !this.registeredSandbox.get()) {
            this.securityProvider.get().registerInterceptors();
            this.registeredSandbox.set(true);
        }
    }

    private Object invokeImpl(final Object thiz, final String name, final Object args[]) throws ScriptException, NoSuchMethodException {
        if (name == null) {
            throw new NullPointerException("Method name can not be null");
        }
        try {
            if (thiz != null) {
                return InvokerHelper.invokeMethod(thiz, name, args);
            }
        } catch (MissingMethodException mme) {
            throw new NoSuchMethodException(mme.getMessage());
        } catch (Exception e) {
            throw new ScriptException(e);
        }
        return callGlobal(name, args);
    }

    private synchronized void createClassLoader() {
        final CompilerConfiguration conf = new CompilerConfiguration();
        conf.addCompilationCustomizers(this.importCustomizerProvider.getCompilationCustomizer());

        if (this.securityProvider.isPresent())
            conf.addCompilationCustomizers(this.securityProvider.get().getCompilationCustomizer());

        if (scriptEvaluationTimeout > 0) {
            final Map<String, Object> timedInterruptAnnotationParams = new HashMap<>();
            timedInterruptAnnotationParams.put("value", scriptEvaluationTimeout);
            timedInterruptAnnotationParams.put("unit", GeneralUtils.propX(GeneralUtils.classX(TimeUnit.class), TimeUnit.MILLISECONDS.toString()));
            conf.addCompilationCustomizers(new ASTTransformationCustomizer(timedInterruptAnnotationParams, TimedInterrupt.class));
        }

        conf.addCompilationCustomizers(new ASTTransformationCustomizer(ThreadInterrupt.class));

        this.loader = new GremlinGroovyClassLoader(getParentLoader(), conf);
        this.securityProvider.ifPresent(SecurityCustomizerProvider::registerInterceptors);
    }

    private Object callGlobal(final String name, final Object args[]) {
        return callGlobal(name, args, context);
    }

    private Object callGlobal(final String name, final Object args[], final ScriptContext ctx) {
        final Closure closure = globalClosures.get(name);
        if (closure != null) {
            return closure.call(args);
        }

        final Object value = ctx.getAttribute(name);
        if (value instanceof Closure) {
            return ((Closure) value).call(args);
        } else {
            throw new MissingMethodException(name, getClass(), args);
        }
    }

    private synchronized String generateScriptName() {
        return SCRIPT + counter.incrementAndGet() + DOT_GROOVY;
    }

    @SuppressWarnings("unchecked")
    private <T> T makeInterface(final Object obj, final Class<T> clazz) {
        if (null == clazz || !clazz.isInterface()) throw new IllegalArgumentException("interface Class expected");

        return (T) Proxy.newProxyInstance(
                clazz.getClassLoader(),
                new Class[]{clazz},
                (proxy, m, args) -> invokeImpl(obj, m.getName(), args));
    }

    protected ClassLoader getParentLoader() {
        final ClassLoader ctxtLoader = Thread.currentThread().getContextClassLoader();
        try {
            final Class c = ctxtLoader.loadClass(GROOVY_LANG_SCRIPT);
            if (c == groovy.lang.Script.class) {
                return ctxtLoader;
            }
        } catch (ClassNotFoundException e) {
        }
        return groovy.lang.Script.class.getClassLoader();
    }

    private String readFully(final Reader reader) throws ScriptException {
        final char arr[] = new char[8192];
        final StringBuilder buf = new StringBuilder();
        int numChars;
        try {
            while ((numChars = reader.read(arr, 0, arr.length)) > 0) {
                buf.append(arr, 0, numChars);
            }
        } catch (IOException exp) {
            throw new ScriptException(exp);
        }
        return buf.toString();
    }

}