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

import groovy.cli.picocli.CliBuilder
import groovy.cli.picocli.OptionAccessor
import jline.TerminalFactory
import jline.console.history.FileHistory

import org.apache.tinkerpop.gremlin.console.commands.BytecodeCommand
import org.apache.tinkerpop.gremlin.console.commands.GremlinSetCommand
import org.apache.tinkerpop.gremlin.console.commands.InstallCommand
import org.apache.tinkerpop.gremlin.console.commands.PluginCommand
import org.apache.tinkerpop.gremlin.console.commands.RemoteCommand
import org.apache.tinkerpop.gremlin.console.commands.ClsCommand
import org.apache.tinkerpop.gremlin.console.commands.SubmitCommand
import org.apache.tinkerpop.gremlin.console.commands.UninstallCommand
import org.apache.tinkerpop.gremlin.groovy.loaders.GremlinLoader
import org.apache.tinkerpop.gremlin.jsr223.CoreGremlinPlugin
import org.apache.tinkerpop.gremlin.jsr223.GremlinPlugin
import org.apache.tinkerpop.gremlin.jsr223.ImportCustomizer
import org.apache.tinkerpop.gremlin.jsr223.console.RemoteException
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalExplanation
import org.apache.tinkerpop.gremlin.structure.Edge
import org.apache.tinkerpop.gremlin.structure.T
import org.apache.tinkerpop.gremlin.structure.Vertex
import org.apache.tinkerpop.gremlin.util.Gremlin
import org.apache.tinkerpop.gremlin.util.iterator.ArrayIterator
import org.codehaus.groovy.tools.shell.ExitNotification
import org.codehaus.groovy.tools.shell.Groovysh
import org.codehaus.groovy.tools.shell.IO
import org.codehaus.groovy.tools.shell.InteractiveShellRunner
import org.codehaus.groovy.tools.shell.commands.SetCommand
import org.fusesource.jansi.Ansi
import sun.misc.Signal
import sun.misc.SignalHandler

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class Console {
    static {
        // this is necessary so that terminal doesn't lose focus to AWT
        System.setProperty("java.awt.headless", "true")
        Colorizer.installAnsi()
    }

    private static final String ELLIPSIS = "..."

    private Iterator tempIterator = Collections.emptyIterator()

    private final IO io
    private final Groovysh groovy
    private final boolean interactive

    public Console(final IO io, final List<List<String>> scriptsAndArgs, final boolean interactive) {
        this.io = io
        this.interactive = interactive

        if (!io.quiet) {
            io.out.println()
            io.out.println("         " + Colorizer.render(Preferences.gremlinColor, "\\,,,/"))
            io.out.println("         " + Colorizer.render(Preferences.gremlinColor, "(o o)"))
            io.out.println("" + Colorizer.render(Preferences.gremlinColor, "-----oOOo-(3)-oOOo-----"))
        }

        final Mediator mediator = new Mediator(this)

        // make sure that remotes are closed on jvm shutdown
        addShutdownHook { mediator.close() }

        // try to grab ctrl+c to interrupt an evaluation.
        final Thread main = Thread.currentThread()
        Signal.handle(new Signal("INT"), new SignalHandler() {
            @Override
            void handle(final Signal signal) {
                if (mediator.evaluating.get()) {
                    io.out.println("Execution interrupted by ctrl+c")
                    main.interrupt()
                }
            }
        })

        groovy = new GremlinGroovysh(mediator, io)

        def commandsToRemove = groovy.getRegistry().commands().findAll { it instanceof SetCommand }
        commandsToRemove.each { groovy.getRegistry().remove(it) }
        groovy.register(new GremlinSetCommand(groovy))
        groovy.register(new UninstallCommand(groovy, mediator))
        groovy.register(new InstallCommand(groovy, mediator))
        groovy.register(new PluginCommand(groovy, mediator))
        groovy.register(new RemoteCommand(groovy, mediator))
        groovy.register(new SubmitCommand(groovy, mediator))
        groovy.register(new BytecodeCommand(groovy, mediator))
        groovy.register(new ClsCommand(groovy, mediator))

        // hide output temporarily while imports execute
        showShellEvaluationOutput(false)

        def imports = (ImportCustomizer) CoreGremlinPlugin.instance().getCustomizers("gremlin-groovy").get()[0]
        imports.getClassPackages().collect { Mediator.IMPORT_SPACE + it.getName() + Mediator.IMPORT_WILDCARD }.each { groovy.execute(it) }
        imports.getMethodClasses().collect { Mediator.IMPORT_STATIC_SPACE + it.getCanonicalName() + Mediator.IMPORT_WILDCARD}.each{ groovy.execute(it) }
        imports.getEnumClasses().collect { Mediator.IMPORT_STATIC_SPACE + it.getCanonicalName() + Mediator.IMPORT_WILDCARD}.each{ groovy.execute(it) }
        imports.getFieldClasses().collect { Mediator.IMPORT_STATIC_SPACE + it.getCanonicalName() + Mediator.IMPORT_WILDCARD}.each{ groovy.execute(it) }

        final InteractiveShellRunner runner = new InteractiveShellRunner(groovy, handlePrompt)
        runner.reader.setHandleUserInterrupt(false)
        runner.reader.setHandleLitteralNext(false)
        runner.setErrorHandler(handleError)
        try {
            final FileHistory history = new FileHistory(new File(ConsoleFs.HISTORY_FILE))
            groovy.setHistory(history)
            runner.setHistory(history)
        } catch (IOException ignored) {
            io.err.println(Colorizer.render(Preferences.errorColor, "Unable to create history file: " + ConsoleFs.HISTORY_FILE))
        }

        GremlinLoader.load()

        // check for available plugins on the path and track them by plugin class name
        def activePlugins = Mediator.readPluginState()
        ServiceLoader.load(GremlinPlugin, groovy.getInterp().getClassLoader()).each { plugin ->
            if (!mediator.availablePlugins.containsKey(plugin.class.name)) {
                def pluggedIn = new PluggedIn((GremlinPlugin) plugin, groovy, io, false)

                mediator.availablePlugins.put(plugin.class.name, pluggedIn)
            }
        }

        // if there are active plugins then initialize them in the order that they are listed
        activePlugins.each { pluginName ->
            def pluggedIn = mediator.availablePlugins[pluginName]
            pluggedIn.activate()

            if (!io.quiet)
                io.out.println(Colorizer.render(Preferences.infoColor, "plugin activated: " + pluggedIn.getPlugin().getName()))
        }

        // remove any "uninstalled" plugins from plugin state as it means they were installed, activated, but not
        // deactivated, and are thus hanging about (e.g. user deleted the plugin directories to uninstall). checking
        // the number of expected active plugins from the plugins.txt file against the number activated on startup
        // should be enough to tell if something changed which would justify that the file be re-written
        if (activePlugins.size() != mediator.activePlugins().size())
            mediator.writePluginState()

        try {
            // if the init script contains :x command it will throw an ExitNotification so init script execution
            // needs to appear in the try/catch
            if (scriptsAndArgs != null && !scriptsAndArgs.isEmpty()) executeInShell(scriptsAndArgs)

            // start iterating results to show as output
            showShellEvaluationOutput(true)

            runner.run()
        } catch (ExitNotification ignored) {
            // occurs on exit
        } catch (Throwable t) {
            t.printStackTrace()
        } finally {
            // shutdown hook defined above will kill any open remotes
            System.exit(0)
        }
    }

    def showShellEvaluationOutput(final boolean show) {
        if (show)
            groovy.setResultHook(handleResultIterate)
        else
            groovy.setResultHook(handleResultShowNothing)
    }

    private def handlePrompt = { 
        if (interactive) {
            int lineNo = groovy.buffers.current().size() 
            if (lineNo > 0 ) {
                String lineStr = lineNo.toString() + ">"
                int pad = Preferences.inputPrompt.length()
                return Colorizer.render(Preferences.inputPromptColor, lineStr.toString().padLeft(pad, '.') + ' ')
            } else {
                return Colorizer.render(Preferences.inputPromptColor, Preferences.inputPrompt + ' ')
            }
        } else {
            return ""
        }
    }

    private def handleResultShowNothing = { args -> null }

    private def handleResultIterate = { result ->

        try {
            // necessary to save persist history to file
            groovy.getHistory().flush()
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e)
        }

        while (true) {
            // give ctrl+c a chance
            Thread.yield()

            // if this is true then ctrl+c was triggered
            if (Thread.interrupted()) {
                this.tempIterator = Collections.emptyIterator()
                return null
            }

            if (this.tempIterator.hasNext()) {
                int counter = 0
                while (this.tempIterator.hasNext() && (Preferences.maxIteration == -1 || counter < Preferences.maxIteration)) {
                    // give ctrl+c a chance
                    Thread.yield()

                    // if this is true then ctrl+c was triggered
                    if (Thread.interrupted()) {
                        this.tempIterator = Collections.emptyIterator()
                        return null
                    }

                    printResult(tempIterator.next())
                    counter++
                }
                if (this.tempIterator.hasNext())
                    io.out.println(Colorizer.render(Preferences.resultPromptColor,ELLIPSIS))
                this.tempIterator = Collections.emptyIterator()
                break
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
                            this.tempIterator = Collections.emptyIterator()
                            return null
                        }
                    } else if (result instanceof Iterable) {
                        this.tempIterator = ((Iterable) result).iterator()
                        if (!this.tempIterator.hasNext()) {
                            this.tempIterator = Collections.emptyIterator()
                            return null
                        }
                    } else if (result instanceof Object[]) {
                        this.tempIterator = new ArrayIterator((Object[]) result)
                        if (!this.tempIterator.hasNext()) {
                            this.tempIterator = Collections.emptyIterator()
                            return null
                        }
                    } else if (result instanceof Map) {
                        this.tempIterator = ((Map) result).entrySet().iterator()
                        if (!this.tempIterator.hasNext()) {
                            this.tempIterator = Collections.emptyIterator()
                            return null
                        }
                    } else if (result instanceof TraversalExplanation) {
                        final int width = TerminalFactory.get().getWidth()
                        io.out.println(Colorizer.render(Preferences.resultPromptColor,(buildResultPrompt() + result.prettyPrint(width < 20 ? 80 : width))))
                        return null
                    } else {
                        printResult(result)
                        return null
                    }
                } catch (final Exception e) {
                    this.tempIterator = Collections.emptyIterator()
                    throw e
                }
            }
        }
    }

    /**
     * Clears the console screen.
     */
    def clear() {
        io.out.println("\033[H\033[2J")
    }

    def printResult(def object) {
        final String prompt = Colorizer.render(Preferences.resultPromptColor, buildResultPrompt())
        // if preference is set to empty string then don't print any result
        if (object != null) {
            io.out.println(prompt + colorizeResult(object))
        } else {
            if (!Preferences.emptyResult.isEmpty()) {
                io.out.println(prompt + Preferences.emptyResult)
            }
        }
    }

    def colorizeResult = { object ->
        if (object instanceof Vertex) {
            return Colorizer.render(Preferences.vertexColor, object.toString())
        } else if (object instanceof Edge) {
            return Colorizer.render(Preferences.edgeColor, object.toString())
        } else if (object instanceof Iterable) {
            List<String> buf = new ArrayList<>()
            def pathIter = object.iterator()
            while (pathIter.hasNext()) {
                Object n = pathIter.next()
                buf.add(colorizeResult(n))
            }
            return ("[" + buf.join(",") + "]")
        } else if (object instanceof Map) {
            List<String> buf = new ArrayList<>()
            object.each{k, v ->
                buf.add(colorizeResult(k) + ":" + colorizeResult(v))
            }
            return ("[" + buf.join(",") + "]")
        } else if (object instanceof String) {
            return Colorizer.render(Preferences.stringColor, object)
        } else if (object instanceof Number) {
            return Colorizer.render(Preferences.numberColor, object)
        } else if (object instanceof T) {
            return Colorizer.render(Preferences.tColor, object)
        } else {
            return object.toString()
        }
    }

    private def handleError = { err ->
        this.tempIterator = Collections.emptyIterator()
        if (err instanceof Throwable) {
            try {
                final Throwable e = (Throwable) err
                String message = e.getMessage()
                if (null != message) {
                    message = message.replace("startup failed:", "")
                    io.err.println(Colorizer.render(Preferences.errorColor, message.trim()))
                } else {
                    io.err.println(Colorizer.render(Preferences.errorColor,e))
                }

                if (interactive) {
                    io.err.println(Colorizer.render(Preferences.infoColor,"Type ':help' or ':h' for help."))
                    io.err.print(Colorizer.render(Preferences.errorColor, "Display stack trace? [yN]"))
                    io.err.flush()
                    String line = new BufferedReader(io.in).readLine()
                    if (null == line)
                        line = ""
                    io.err.print(line.trim())
                    io.err.println()
                    if (line.trim().equals("y") || line.trim().equals("Y")) {
                        if (err instanceof RemoteException && err.remoteStackTrace.isPresent()) {
                            io.err.print(err.remoteStackTrace.get())
                            io.err.flush()
                        } else {
                            e.printStackTrace(io.err)
                        }
                    }
                } else {
                    e.printStackTrace(io.err)
                    System.exit(1)
                }
            } catch (Exception ignored) {
                io.err.println(Colorizer.render(Preferences.errorColor, "An undefined error has occurred: " + err))
                if (!interactive) System.exit(1)
            }
        } else {
            io.err.println(Colorizer.render(Preferences.errorColor, "An undefined error has occurred: " + err.toString()))
            if (!interactive) System.exit(1)
        }

        groovy.buffers.current().clear()

        return null
    }

    private static String buildResultPrompt() {
        final String groovyshellProperty = System.getProperty("gremlin.prompt")
        if (groovyshellProperty != null)
            return groovyshellProperty

        final String groovyshellEnv = System.getenv("GREMLIN_PROMPT")
        if (groovyshellEnv != null)
            return groovyshellEnv

        return Preferences.resultPrompt
    }

    private void executeInShell(final List<List<String>> scriptsAndArgs) {
        scriptsAndArgs.eachWithIndex { scriptAndArgs, idx ->
            final String scriptFile = scriptAndArgs[0]
            try {
                // check if this script comes with arguments. if so then set them up in an "args" bundle
                if (scriptAndArgs.size() > 1) {
                    List<String> args = scriptAndArgs.subList(1, scriptAndArgs.size())
                    groovy.execute("args = [\"" + args.join('\",\"') + "\"]")
                } else {
                    groovy.execute("args = []")
                }

                File file = new File(scriptFile)
                if (!file.exists() && !file.isAbsolute()) {
                    final String userWorkingDir = System.getProperty("user.working_dir")
                    if (userWorkingDir != null) {
                        file = new File(userWorkingDir, scriptFile)
                    }
                }
                int lineNumber = 0
                def lines = file.readLines()
                for (String line : lines) {
                    try {
                        lineNumber++
                        groovy.execute(line)
                    } catch (Exception ex) {
                        io.err.println(Colorizer.render(Preferences.errorColor, "Error in $scriptFile at [$lineNumber: $line] - ${ex.message}"))
                        if (interactive)
                            break
                        else {
                            ex.printStackTrace(io.err)
                            System.exit(1)
                        }

                    }
                }
            } catch (FileNotFoundException ignored) {
                io.err.println(Colorizer.render(Preferences.errorColor, "Gremlin file not found at [$scriptFile]."))
                if (!interactive) System.exit(1)
            } catch (Exception ex) {
                io.err.println(Colorizer.render(Preferences.errorColor, "Failure processing Gremlin script [$scriptFile] - ${ex.message}"))
                if (!interactive) System.exit(1)
            }
        }

        if (!interactive) System.exit(0)
    }

    public static void main(final String[] args) {

        Preferences.expandoMagic()

        IO io = new IO(System.in, System.out, System.err)

        final CliBuilder cli = new CliBuilder()
        cli.stopAtNonOption = false
        cli.name = "gremlin.sh"

        // note that the inclusion of -l is really a setting handled by gremlin.sh and not by Console class itself.
        // it is mainly listed here for informational purposes when the user starts things up with -h
        cli.h(type: Boolean, longOpt: 'help', "Display this help message")
        cli.v(type: Boolean,longOpt: 'version', "Display the version")
        cli.l("Set the logging level of components that use standard logging output independent of the Console")
        cli.V(type: Boolean, longOpt: 'verbose', "Enable verbose Console output")
        cli.Q(type: Boolean, longOpt: 'quiet', "Suppress superfluous Console output")
        cli.D(type: Boolean, longOpt: 'debug', "Enabled debug Console output")
        cli.i(type: List, longOpt: 'interactive', arity: "1..*", argName: "SCRIPT ARG1 ARG2 ...", "Execute the specified script and leave the console open on completion")
        cli.e(type: List, longOpt: 'execute', argName: "SCRIPT ARG1 ARG2 ...", "Execute the specified script (SCRIPT ARG1 ARG2 ...) and close the console on completion")
        cli.C(type: Boolean, longOpt: 'color', "Disable use of ANSI colors")

        OptionAccessor options = cli.parse(args)

        if (options == null) {
            // CliBuilder prints error, but does not exit
            System.exit(22) // Invalid Args
        }

        if (options.C) {
            Ansi.enabled = false
        }

        if (options.h) {
            cli.usage()
            System.exit(0)
        }

        if (options.v) {
        if (args.length == 1 && !args[0].startsWith("-"))
            new Console(io, [args[0]], true)
            println("gremlin " + Gremlin.version())
            System.exit(0)
        }

        if (options.V) io.verbosity = IO.Verbosity.VERBOSE
        if (options.D) io.verbosity = IO.Verbosity.DEBUG
        if (options.Q) io.verbosity = IO.Verbosity.QUIET

        // override verbosity if not explicitly set and -e is used
        if (options.e && (!options.V && !options.D && !options.Q))
            io.verbosity = IO.Verbosity.QUIET

        if (options.i && options.e) {
            println("-i and -e options are mutually exclusive - provide one or the other")
            System.exit(0)
        }

        def scriptAndArgs = parseArgs(options.e ? ["-e", "--execute"] : ["-i", "--interactive"], args, cli)
        new Console(io, scriptAndArgs, !options.e)
    }

    /**
     * Provides a bit of a hack around the limitations of the {@code CliBuilder}. This method directly parses the
     * argument list to allow for multiple {@code -e} and {@code -i} values and parses such parameters into a list
     * of lists where the inner list is a script file and its arguments.
     */
    private static List<List<String>> parseArgs(final List<String> options, final String[] args, final CliBuilder cli) {
        def parsed = []
        def normalizedArgs = normalizeArgs(options, args)
        for (int ix = 0; ix < normalizedArgs.length; ix++) {
            if (normalizedArgs[ix] in options) {
                // increment the counter to move past the option that was found. should now be positioned on the
                // first argument to that option
                ix++

                def parsedSet = []
                for (ix; ix < normalizedArgs.length; ix++) {
                    // this is a do nothing as there's no arguments to the option or it's the start of a new option
                    if (cli.savedTypeOptions.values().any { "-" + it.opt == normalizedArgs[ix] || "--" + it.longOpt == normalizedArgs[ix] }) {
                        // rollback the counter now that we hit the next option
                        ix--
                        break
                    }
                    parsedSet << normalizedArgs[ix]
                }

                if (!parsedSet.isEmpty()) {
                    // check if the params were passed in with double quotes such that they arrive as a single arg
                    if (parsedSet.size() == 1)
                        parsed << parsedSet[0].toString().split(" ").toList()
                    else
                        parsed << parsedSet
                }
            }
        }

        return parsed
    }

    /**
     * The {@code args} value contains the individual flagged parameters provided on the command line which may come
     * with or without an "=" to separate the flag from the argument to the flag. This method normalizes these values
     * to split the flag from the argument so that it can be evaluated in a consistent way by {@code parseArgs()}.
     */
    private static def normalizeArgs(final List<String> options, final String[] args) {
        return args.collect{ arg ->
                // arguments that match -i/-e options should be normalized where long forms need to be split on "="
                // and short forms need to have the "=" included with the argument
                if (options.any{ arg.startsWith(it) }) {
                    return arg.matches("^-[e,i]=.*") ? [arg.substring(0, 2), arg.substring(2)] : arg.split("=", 2)
                } 
                return arg
            }.flatten().toArray()
    }
}
