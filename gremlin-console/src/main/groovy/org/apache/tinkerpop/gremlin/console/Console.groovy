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
package org.apache.tinkerpop.gremlin.console

import org.apache.tinkerpop.gremlin.console.commands.*
import org.apache.tinkerpop.gremlin.console.plugin.PluggedIn
import org.apache.tinkerpop.gremlin.groovy.loaders.GremlinLoader
import org.apache.tinkerpop.gremlin.groovy.plugin.GremlinPlugin
import jline.console.history.FileHistory
import org.apache.tinkerpop.gremlin.util.iterator.ArrayIterator
import org.codehaus.groovy.tools.shell.AnsiDetector
import org.codehaus.groovy.tools.shell.ExitNotification
import org.codehaus.groovy.tools.shell.Groovysh
import org.codehaus.groovy.tools.shell.IO
import org.codehaus.groovy.tools.shell.InteractiveShellRunner
import org.codehaus.groovy.tools.shell.commands.SetCommand
import org.codehaus.groovy.tools.shell.util.Preferences
import org.fusesource.jansi.Ansi
import org.fusesource.jansi.AnsiConsole

import java.nio.charset.Charset
import java.util.concurrent.TimeUnit
import java.util.prefs.PreferenceChangeEvent
import java.util.prefs.PreferenceChangeListener

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class Console {
    static {
        // this is necessary so that terminal doesn't lose focus to AWT
        System.setProperty("java.awt.headless", "true")
        // must be called before IO(), since it modifies System.in
        // Install the system adapters, replaces System.out and System.err
        // Must be called before using IO(), because IO stores refs to System.out and System.err
        AnsiConsole.systemInstall()
        // Register jline ansi detector
        Ansi.setDetector(new AnsiDetector())
        Ansi.enabled = true
    }

    public static final String PREFERENCE_ITERATION_MAX = "max-iteration"
    private static final int DEFAULT_ITERATION_MAX = 100
    private static int maxIteration = DEFAULT_ITERATION_MAX

    private static final String HISTORY_FILE = ".gremlin_groovy_history"
    private static final String STANDARD_INPUT_PROMPT = "gremlin> "
    private static final String STANDARD_RESULT_PROMPT = "==>"
    private static final String IMPORT_SPACE = "import "
    private static final String IMPORT_STATIC_SPACE = "import static "
    private static final String NULL = "null"
    private static final String ELLIPSIS = "..."

    private Iterator tempIterator = Collections.emptyIterator()

    private final IO io = new IO(System.in, System.out, System.err)
    private final Groovysh groovy = new GremlinGroovysh()

    public Console(final String initScriptFile) {
        io.out.println()
        io.out.println("         \\,,,/")
        io.out.println("         (o o)")
        io.out.println("-----oOOo-(3)-oOOo-----")

        maxIteration = Integer.parseInt(Preferences.get(PREFERENCE_ITERATION_MAX, Integer.toString(DEFAULT_ITERATION_MAX)))
        Preferences.addChangeListener(new PreferenceChangeListener() {
            @Override
            void preferenceChange(PreferenceChangeEvent evt) {
                if (evt.key == PREFERENCE_ITERATION_MAX)
                    maxIteration = Integer.parseInt(evt.newValue)
            }
        })

        final Mediator mediator = new Mediator(this)
        def commandsToRemove = groovy.getRegistry().commands().findAll{it instanceof SetCommand}
        commandsToRemove.each {groovy.getRegistry().remove(it)}
        groovy.register(new GremlinSetCommand(groovy))
        groovy.register(new UninstallCommand(groovy, mediator))
        groovy.register(new InstallCommand(groovy, mediator))
        groovy.register(new PluginCommand(groovy, mediator))
        groovy.register(new RemoteCommand(groovy, mediator))
        groovy.register(new SubmitCommand(groovy, mediator))

        // hide output temporarily while imports execute
        showShellEvaluationOutput(false)

        // add the default imports
        new ConsoleImportCustomizerProvider().getCombinedImports().stream()
                .collect { IMPORT_SPACE + it }.each { groovy.execute(it) }
        new ConsoleImportCustomizerProvider().getCombinedStaticImports().stream()
                .collect { IMPORT_STATIC_SPACE + it }.each { groovy.execute(it) }

        final InteractiveShellRunner runner = new InteractiveShellRunner(groovy, handlePrompt)
        runner.setErrorHandler(handleError)
        try {
            final FileHistory history = new FileHistory(new File(System.getProperty("user.home") + System.getProperty("file.separator") + HISTORY_FILE))
            groovy.setHistory(history)
            runner.setHistory(history)
        } catch (IOException ignored) {
            io.err.println("Unable to create history file: " + HISTORY_FILE)
        }

        GremlinLoader.load()

        // check for available plugins.  if they are in the "active" plugins strategies then "activate" them
        def activePlugins = Mediator.readPluginState()
        ServiceLoader.load(GremlinPlugin.class, groovy.getInterp().getClassLoader()).each { plugin ->
            if (!mediator.availablePlugins.containsKey(plugin.class.name)) {
                def pluggedIn = new PluggedIn(plugin, groovy, io, false)
                mediator.availablePlugins.put(plugin.class.name, pluggedIn)

                if (activePlugins.contains(plugin.class.name)) {
                    pluggedIn.activate()
                    io.out.println("plugin activated: " + plugin.getName())
                }
            }
        }

        // remove any "uninstalled" plugins from plugin state as it means they were installed, activated, but not
        // deactivated, and are thus hanging about
        mediator.writePluginState()

        // start iterating results to show as output
        showShellEvaluationOutput(true)
        if (initScriptFile != null) initializeShellWithScript(initScriptFile)

        try {
            runner.run()
        } catch (ExitNotification ignored) {
            // occurs on exit
        } catch (Throwable t) {
            t.printStackTrace()
        } finally {
            try {
                mediator.close().get(3, TimeUnit.SECONDS)
            } catch (Exception ignored) {
                // ok if this times out - just trying to be polite on shutdown
            } finally {
                System.exit(0)
            }
        }
    }

    def showShellEvaluationOutput(final boolean show) {
        if (show)
            groovy.setResultHook(handleResultIterate)
        else
            groovy.setResultHook(handleResultShowNothing)
    }

    private def handlePrompt = { STANDARD_INPUT_PROMPT }

    private def handleResultShowNothing = { args -> null }

    private def handleResultIterate = { result ->
        try {
            // necessary to save persist history to file
            groovy.getHistory().flush()
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e)
        }

        while (true) {
            if (this.tempIterator.hasNext()) {
                int counter = 0;
                while (this.tempIterator.hasNext() && (maxIteration == -1 || counter < maxIteration)) {
                    final Object object = this.tempIterator.next()
                    io.out.println(buildResultPrompt() + ((null == object) ? NULL : object.toString()))
                    counter++;
                }
                if(this.tempIterator.hasNext())
                    io.out.println(ELLIPSIS);
                this.tempIterator = Collections.emptyIterator();
                return null
            } else {
                try {
                    // if the result is an empty iterator then the tempIterator needs to be set to one, as a
                    // future assignment to the strategies that produced the iterator will maintain that reference
                    // and try to iterate it above.  in other words, this:
                    //
                    // x =[]
                    // x << "test"
                    //
                    // would throw a ConcurrentModificationException because the assignment of x to the tempIterator
                    // on the first line would maintain a reference on the next result iteration call and would
                    // drop into the other part of this if statement and throw.
                    if (result instanceof Iterator) {
                        this.tempIterator = (Iterator) result
                        if (!this.tempIterator.hasNext()) {
                            this.tempIterator = Collections.emptyIterator();
                            return null
                        }
                    } else if (result instanceof Iterable) {
                        this.tempIterator = ((Iterable) result).iterator()
                        if (!this.tempIterator.hasNext()) {
                            this.tempIterator = Collections.emptyIterator();
                            return null
                        }
                    } else if (result instanceof Object[]) {
                        this.tempIterator = new ArrayIterator((Object[]) result)
                        if (!this.tempIterator.hasNext()) {
                            this.tempIterator = Collections.emptyIterator();
                            return null
                        }
                    } else if (result instanceof Map) {
                        this.tempIterator = ((Map) result).entrySet().iterator()
                        if (!this.tempIterator.hasNext()) {
                            this.tempIterator = Collections.emptyIterator();
                            return null
                        }
                    } else {
                        io.out.println(buildResultPrompt() + ((null == result) ? NULL : result.toString()))
                        return null
                    }
                } catch (final Exception e) {
                    this.tempIterator = Collections.emptyIterator()
                    throw e
                }
            }
        }
    }

    private def handleError = { err ->
        this.tempIterator = Collections.emptyIterator();
        if (err instanceof Throwable) {
            try {
                final Throwable e = (Throwable) err
                String message = e.getMessage()
                if (null != message) {
                    message = message.replace("startup failed:", "")
                    io.err.println(message.trim())
                } else {
                    io.err.println(e)
                }

                io.err.print("Display stack trace? [yN] ")
                io.err.flush()
                String line = new BufferedReader(io.in).readLine()
                if (null == line)
                    line = ""
                io.err.print(line.trim())
                io.err.println()
                if (line.trim().equals("y") || line.trim().equals("Y")) {
                    e.printStackTrace(io.err)
                }
            } catch (Exception ignored) {
                io.err.println("An undefined error has occurred: " + err)
            }
        } else {
            io.err.println("An undefined error has occurred: " + err.toString())
        }

        return null
    }

    private static String buildResultPrompt() {
        final String groovyshellProperty = System.getProperty("gremlin.prompt")
        if (groovyshellProperty != null)
            return groovyshellProperty

        final String groovyshellEnv = System.getenv("GREMLIN_PROMPT")
        if (groovyshellEnv != null)
            return groovyshellEnv

        return STANDARD_RESULT_PROMPT
    }

    private void initializeShellWithScript(final String initScriptFile) {
        try {
            final File file = new File(initScriptFile)
            file.eachLine { line ->
                try {
                    groovy.execute(line)
                } catch (Exception ex) {
                    io.err.println("Bad line in Gremlin initialization file at [$line] - ${ex.message}")
                    System.exit(1)
                }
            }
        } catch (FileNotFoundException ignored) {
            io.err.println("Gremlin initialization file not found at [$initScriptFile].")
            System.exit(1)
        } catch (Exception ex) {
            io.err.println("Error starting Gremlin with initialization script at [$initScriptFile] - ${ex.message}")
            System.exit(1)
        }
    }

    public static void main(final String[] args) {
        new Console(args.length == 1 ? args[0] : null)
    }
}
