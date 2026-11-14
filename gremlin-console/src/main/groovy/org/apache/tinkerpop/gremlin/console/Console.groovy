/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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
import groovy.transform.ThreadInterrupt
import org.apache.groovy.groovysh.GroovyshOptions
import org.apache.groovy.groovysh.Main
import org.apache.groovy.groovysh.jline.GroovyEngine
import org.apache.groovy.groovysh.jline.GroovySystemRegistry
import org.apache.tinkerpop.gremlin.console.commands.UsageException
import org.apache.tinkerpop.gremlin.groovy.loaders.GremlinLoader
import org.apache.tinkerpop.gremlin.jsr223.CoreGremlinPlugin
import org.apache.tinkerpop.gremlin.jsr223.GremlinPlugin
import org.apache.tinkerpop.gremlin.process.traversal.Failure
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalExplanation
import org.apache.tinkerpop.gremlin.structure.Edge
import org.apache.tinkerpop.gremlin.structure.T
import org.apache.tinkerpop.gremlin.structure.Vertex
import org.apache.tinkerpop.gremlin.util.Gremlin
import org.apache.tinkerpop.gremlin.util.iterator.ArrayIterator
import org.codehaus.groovy.control.CompilerConfiguration
import org.codehaus.groovy.control.customizers.ASTTransformationCustomizer
import org.codehaus.groovy.tools.shell.IO
import org.jline.jansi.Ansi
import org.jline.builtins.Options.HelpException
import org.jline.reader.EOFError
import org.jline.reader.LineReader
import org.jline.reader.Widget
import org.jline.reader.impl.DefaultHighlighter
import org.jline.reader.impl.DefaultParser
import org.jline.reader.impl.LineReaderImpl
import org.jline.shell.Command
import org.jline.shell.CommandGroup
import org.jline.shell.CommandSession
import org.jline.terminal.Terminal
import org.jline.terminal.TerminalBuilder

import java.lang.reflect.UndeclaredThrowableException
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.util.function.BiConsumer
import java.util.function.Consumer
import java.util.function.Predicate
import java.util.function.Supplier

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class Console {
    static {
        // this is necessary so that terminal doesn't lose focus to AWT
        System.setProperty("java.awt.headless", "true")
        if (System.getProperty("groovysh.interpreterMode") == null)
            System.setProperty("groovysh.interpreterMode", "false")
        Colorizer.installAnsi()
    }

    private static final String ELLIPSIS = "..."

    static final Map<String, String> COMMAND_ALIASES = [
            "/l": "/load",
            "/H": "/history",
            "/a": "/alias",
            "/g": "/grab",
            "/D": "/doc",
            "/n": "/inspect",
            "/quit": "/exit"]

    private final IO io
    private final boolean interactive
    private final GroovyEngine engine
    private final CommandGroup commands
    private LineReader reader

    public Console(final IO io, final List<List<String>> scriptsAndArgs, final boolean interactive) {
        this.io = io
        this.interactive = interactive

        if (!io.quiet) {
            io.out.println()
            io.out.println("         " + Colorizer.render(Preferences.gremlinColor, "\\,,,/"))
            io.out.println("         " + Colorizer.render(Preferences.gremlinColor, "(o o)"))
            io.out.println("" + Colorizer.render(Preferences.gremlinColor, "-----oOOo-(" + Gremlin.majorVersion() + ")-oOOo-----"))
            io.out.flush()
        }

        final Mediator mediator = new Mediator(this)

        engine = createEngine()

        GremlinLoader.load()

        // check for available plugins on the path and track them by plugin class name
        def activePlugins = Mediator.readPluginState()
        ServiceLoader.load(GremlinPlugin, engine.getClassLoader()).each { plugin ->
            if (!mediator.availablePlugins.containsKey(plugin.class.name)) {
                def pluggedIn = new PluggedIn((GremlinPlugin) plugin, engine, false)
                mediator.availablePlugins.put(plugin.class.name, pluggedIn)
                pluggedIn.activate()
            }
        }

        // if there are active plugins then initialize them in the order that they are listed
        activePlugins.each { pluginName ->
            def pluggedIn = mediator.availablePlugins[pluginName]
            if (pluggedIn != null) {
                pluggedIn.activate()
                if (!io.quiet)
                    io.out.println(Colorizer.render(Preferences.infoColor, "plugin activated: " + pluggedIn.getPlugin().getName()))
            } else if (!io.quiet) {
                io.out.println(Colorizer.render(Preferences.infoColor, "invalid plugin: " + pluginName + " -- removing reference to this plugin."))
            }
        }
        io.out.flush()

        // remove any "uninstalled" plugins from plugin state as it means they were installed, activated, but not
        // deactivated, and are thus hanging about (e.g. user deleted the plugin directories to uninstall). checking
        // the number of expected active plugins from the plugins.txt file against the number activated on startup
        // should be enough to tell if something changed which would justify that the file be re-written
        if (activePlugins.size() != mediator.activePlugins().size())
            mediator.writePluginState()

        commands = GremlinCommands.create(mediator, engine)

        if (scriptsAndArgs != null && !scriptsAndArgs.isEmpty()) executeInShell(scriptsAndArgs, commands)
    }

    /**
     * Runs the shell on the terminal until the session ends and returns its exit code.
     */
    int start(final Terminal terminal, final Path historyFile = Paths.get(ConsoleFs.HISTORY_FILE)) {
        final GroovyshOptions options = GroovyshOptions.builder()
                .engine(engine)
                .terminal(terminal)
                .showBanner(false)
                .prompt({ interactive ? Colorizer.render(Preferences.inputPromptColor, Preferences.inputPrompt + ' ') : "" } as Supplier<String>)
                .historyFile(historyFile)
                .groups(commands)
                .resultHandler({ printer, result -> handleResultIterate(result) } as GroovyshOptions.ResultHandler)
                .errorHandler({ Throwable err, Consumer<Throwable> trace ->
                    // groovysh ends the session if this throws
                    try {
                        // command help arrives as an exception; groovysh prints it
                        if (err instanceof HelpException)
                            trace.accept(err)
                        else if (err instanceof UsageException)
                            io.err.println(Colorizer.render(Preferences.errorColor, err.message))
                        else
                            handleError(err)
                    } catch (Throwable t) {
                        io.err.println(Colorizer.render(Preferences.errorColor, "An undefined error has occurred: " + err))
                    }
                } as GroovyshOptions.ErrorHandler)
                .onReaderReady({ LineReader r, GroovySystemRegistry registry ->
                    this.reader = r
                    keepHistoryInMemoryIfUnusable(r, historyFile)
                    r.setHighlighter(new DefaultHighlighter())
                    // the "......1> " continuation prompt
                    r.setVariable(LineReader.SECONDARY_PROMPT_PATTERN, "%P.%N> ")
                    // short forms of old console commands that have a groovysh 6 equivalent
                    COMMAND_ALIASES.each { alias, command -> registry.invoke("/alias", alias, command) }
                    final Predicate<String> isCommand = { String c -> registry.hasCommand(c) || registry.isCommandAlias(c) } as Predicate<String>
                    r.setCompleter(new ColonCommandCompleter(r.getCompleter(), isCommand))
                    // wait for more input after a line such as "g.V()." that ends too early
                    ((LineReaderImpl) r).setParser(new StatementParser(r.getParser(), engine.getCompilerConfiguration(), isCommand))
                    if (r.terminal.type == Terminal.TYPE_DUMB || r.terminal.type == Terminal.TYPE_DUMB_COLOR)
                        readPipedInput(r)
                } as BiConsumer<LineReader, GroovySystemRegistry>)
                .build()

        return Main.start(options, new String[0])
    }

    /**
     * Creates the engine statements run in, with the core Gremlin imports. {@code @ThreadInterrupt} lets ctrl+c stop
     * a running statement.
     */
    static GroovyEngine createEngine() {
        final CompilerConfiguration compilerConfig = new CompilerConfiguration(CompilerConfiguration.DEFAULT)
        compilerConfig.addCompilationCustomizers(new ASTTransformationCustomizer(ThreadInterrupt.class))
        final GroovyEngine engine = new GroovyEngine(Console.class.classLoader, new Binding(), compilerConfig)
        new PluggedIn(CoreGremlinPlugin.instance(), engine, false).activate()
        return engine
    }

    /**
     * Keeps the history in memory when the history file cannot be used, which the line reader would fail on.
     */
    private void keepHistoryInMemoryIfUnusable(final LineReader r, final Path historyFile) {
        if (!Files.exists(historyFile) ||
                (Files.isRegularFile(historyFile) && Files.isReadable(historyFile) && Files.isWritable(historyFile)))
            return
        io.err.println(Colorizer.render(Preferences.errorColor,
                "Cannot use the history file $historyFile - the history of this session will not be saved"))
        r.setVariable(LineReader.HISTORY_FILE, null)
    }

    /**
     * Adjusts the reader for input that is piped in rather than typed.
     */
    private static void readPipedInput(final LineReader r) {
        // piped input brings its own closing brackets
        r.option(LineReader.Option.INSERT_BRACKET, false)

        // without a redraw, the continuation prompts of a piped statement appear only once it is complete
        final Widget accept = r.widgets.get(LineReader.ACCEPT_LINE)
        r.widgets.put(LineReader.ACCEPT_LINE, {
            final int before = r.buffer.length()
            final boolean accepted = accept.apply()
            if (r.buffer.length() > before) r.callWidget(LineReader.REDISPLAY)
            return accepted
        } as Widget)
    }

    private void handleResultIterate(final Object result) {
        // give ctrl+c a chance
        Thread.yield()
        if (interruptedWhilePrinting()) return

        if (result instanceof TraversalExplanation) {
            final int width = reader == null ? 80 : reader.terminal.width
            println(Colorizer.render(Preferences.resultPromptColor, (buildResultPrompt() + result.prettyPrint(width < 20 ? 80 : width))))
            return
        }

        final Iterator iterator = asIterator(result)
        if (iterator == null) {
            printResult(result)
            return
        }

        int counter = 0
        while (iterator.hasNext() && (Preferences.maxIteration == -1 || counter < Preferences.maxIteration)) {
            // give ctrl+c a chance
            Thread.yield()
            if (interruptedWhilePrinting()) return

            printResult(iterator.next())
            counter++
        }
        if (iterator.hasNext())
            println(Colorizer.render(Preferences.resultPromptColor, ELLIPSIS))
        System.out.flush()
    }

    private static Iterator asIterator(final Object result) {
        if (result instanceof Iterator) return (Iterator) result
        if (result instanceof Iterable) return ((Iterable) result).iterator()
        if (result instanceof Object[]) return new ArrayIterator((Object[]) result)
        if (result instanceof Map) return ((Map) result).entrySet().iterator()
        return null
    }

    /**
     * Whether ctrl+c was pressed while results were printing, in which case the rest of the results are dropped.
     */
    private boolean interruptedWhilePrinting() {
        if (!Thread.interrupted()) return false
        io.out.println("Execution interrupted by ctrl+c")
        return true
    }

    /**
     * Clears the console screen.
     */
    def clear() {
        println("\033[H\033[2J")
    }

    def printResult(def object) {
        final String prompt = Colorizer.render(Preferences.resultPromptColor, buildResultPrompt())
        // if preference is set to empty string then don't print any result
        if (object != null) {
            println(prompt + colorizeResult(object))
        } else if (!Preferences.emptyResult.isEmpty()) {
            println(prompt + Preferences.emptyResult)
        }
        System.out.flush()
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
                buf.add(colorizeResult(pathIter.next()))
            }
            return ("[" + buf.join(",") + "]")
        } else if (object instanceof Map) {
            List<String> buf = new ArrayList<>()
            object.each { k, v ->
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

    private void handleError(Throwable e) {
        // ctrl+c interrupts the running statement. inside a closure that implements an interface, e.g. one coerced
        // with "as Iterator", the InterruptedException arrives wrapped.
        if (e instanceof UndeclaredThrowableException && e.cause instanceof InterruptedException)
            e = e.cause
        if (e instanceof InterruptedException)
            io.out.println("Execution interrupted by ctrl+c")

        if (e instanceof Failure) {
            io.err.println(Colorizer.render(Preferences.errorColor, ((Failure) e).format()))
            return
        }

        String message = e.getMessage()
        if (null != message) {
            message = message.replace("startup failed:", "")
            io.err.println(Colorizer.render(Preferences.errorColor, message.trim()))
        } else {
            io.err.println(Colorizer.render(Preferences.errorColor, e))
        }

        // provide a hint in the case of a stackoverflow as it can be common when running large Gremlin
        // scripts and it isn't immediately apparent what the error might mean in this context especially
        // if the user isn't familiar with the JVM. it really can only be a hint since we can't be completely
        // sure it arose as a result of a long Gremlin traversal.
        if (e instanceof StackOverflowError) {
            io.err.println(Colorizer.render(Preferences.errorColor,
                    "A StackOverflowError can indicate that the Gremlin traversal being executed is too long. If " +
                            "you have a single Gremlin statement that is \"long\", you may break it up into " +
                            "multiple separate commands, re-write the traversal to operate on a stream of " +
                            "input via inject() rather than literals, or attempt to increase the -Xss setting" +
                            "of the Gremlin Console by modifying gremlin.sh."))
        }

        io.err.println(Colorizer.render(Preferences.infoColor, "Type ':help' or ':h' for help."))
        io.err.print(Colorizer.render(Preferences.errorColor, "Display stack trace? [yN]"))
        io.err.flush()

        // ctrl+c leaves the interrupt flag set, which would make reading the answer fail
        Thread.interrupted()
        String line
        reader.setVariable(LineReader.DISABLE_HISTORY, true)
        try {
            line = reader.readLine("")
        } catch (Exception ignored) {
            line = ""
        } finally {
            reader.setVariable(LineReader.DISABLE_HISTORY, false)
        }
        if (null == line) line = ""
        if (line.trim().equalsIgnoreCase("y")) e.printStackTrace(io.err)
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

    /**
     * Runs the -i and -e scripts line by line, collecting lines until they form a statement that compiles. A line
     * starting with ":" is a console command.
     */
    private void executeInShell(final List<List<String>> scriptsAndArgs, final CommandGroup commands) {
        final StatementParser statements = StatementParser.forScripts(engine.getCompilerConfiguration())
        scriptsAndArgs.each { scriptAndArgs ->
            final String scriptFile = scriptAndArgs[0]
            try {
                // check if this script comes with arguments. if so then set them up in an "args" bundle
                if (scriptAndArgs.size() > 1) {
                    List<String> args = scriptAndArgs.subList(1, scriptAndArgs.size())
                    engine.execute("args = [\"" + args.join('\",\"') + "\"]")
                } else {
                    engine.execute("args = []")
                }

                File file = new File(scriptFile)
                if (!file.exists() && !file.isAbsolute()) {
                    final String userWorkingDir = System.getProperty("user.working_dir")
                    if (userWorkingDir != null) {
                        file = new File(userWorkingDir, scriptFile)
                    }
                }
                // read first, so a missing file is reported as such below
                final List<String> lines = file.readLines()
                int lineNumber = 0
                String line = null
                final List<String> statement = []
                try {
                    for (String next : lines) {
                        line = next
                        lineNumber++
                        if (line.trim().isEmpty() && statement.isEmpty()) continue
                        if (statement.isEmpty() && line.trim().startsWith(":")) {
                            executeCommand(line.trim(), commands)
                            continue
                        }
                        statement << line
                        final String text = statement.join(System.lineSeparator())
                        if (statements.isUnfinished(text)) continue
                        statement.clear()
                        engine.execute(text)
                    }
                    if (!statement.isEmpty())
                        throw new IllegalArgumentException("the statement is not finished at the end of the file")
                } catch (Exception ex) {
                    io.err.println(Colorizer.render(Preferences.errorColor, "Error in $scriptFile at [$lineNumber: $line] - ${ex.message}"))
                    if (!interactive) {
                        ex.printStackTrace(io.err)
                        System.exit(1)
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

    /**
     * Runs a console command from a script. Only the Gremlin commands and the exit commands are available, as
     * groovysh's own commands do not exist until the shell starts.
     */
    private static void executeCommand(final String line, final CommandGroup commands) {
        List<String> words
        try {
            words = new DefaultParser(eofOnUnclosedQuote: true).parse(line, line.length()).words().findAll { !it.isEmpty() }
        } catch (EOFError e) {
            throw new IllegalArgumentException("Missing closing " + (e.missing == "quote" ? "'" : '"'))
        }
        final String name = GremlinCommands.slashName(words[0])
        if (name in EXIT_COMMANDS) System.exit(0)

        final Command command = commands.command(name)
        if (command == null) throw new IllegalArgumentException("${words[0]} cannot be used in a script")
        command.execute(new CommandSession(), words.drop(1) as String[])
    }

    private static final List<String> EXIT_COMMANDS = ["/x", "/exit", "/q", "/quit"]

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
        final Console console = new Console(io, scriptAndArgs, !options.e)
        // built here so JLine doesn't send terminal capability queries at startup
        System.exit(console.start(TerminalBuilder.builder().name("gremlin").graphemeCluster(false).build()))
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
