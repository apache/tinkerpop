package com.tinkerpop.gremlin.groovy.console;

import groovy.lang.Closure;
import org.codehaus.groovy.tools.shell.IO;

import java.io.BufferedReader;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ErrorHookClosure extends Closure {

    private final IO io;

    public ErrorHookClosure(final Object owner, final IO io) {
        super(owner);
        this.io = io;
    }

    public Object call(final Object[] args) {
        if (args.length > 0) {
            try {
                final Throwable e = (Throwable) args[0];
                String message = e.getMessage();
                if (null != message) {
                    message = message.replace("startup failed:", "");
                    io.err.println(message.trim());
                } else {
                    io.err.println(e);
                }

                io.err.print("Display stack trace? [yN] ");
                io.err.flush();
                String line = new BufferedReader(io.in).readLine();
                if (null == line)
                    line = "";
                io.err.print(line.trim());
                io.err.println();
                if (line.trim().equals("y") || line.trim().equals("Y")) {
                    e.printStackTrace(io.err);
                }

                return null;
            } catch (Exception e) {
                io.err.println("An undefined error has occurred: " + args[0]);
                return null;
            }
        } else {
            io.err.println("An undefined error has occurred");
            return null;
        }
    }
}

