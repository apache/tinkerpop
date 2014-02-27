package com.tinkerpop.gremlin.groovy.console;

import com.tinkerpop.gremlin.process.util.FastNoSuchElementException;
import groovy.lang.Closure;
import org.codehaus.groovy.tools.shell.IO;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ResultHookClosure extends Closure {
    private static final String NULL = "null";
    private Iterator tempIterator = Arrays.asList().iterator();
    private final String resultPrompt;
    private final IO io;

    public ResultHookClosure(final Object owner, final IO io, final String resultPrompt) {
        super(owner);
        this.io = io;
        this.resultPrompt = resultPrompt;
    }

    public Object call(final Object[] args) {
        final Object result = args[0];
        while (true) {
            if (this.tempIterator.hasNext()) {
                while (this.tempIterator.hasNext()) {
                    final Object object = this.tempIterator.next();
                    io.out.println(resultPrompt + ((null == object) ? NULL : object.toString()));
                }
                return null;
            } else {
                if (result instanceof Iterator) {
                    this.tempIterator = (Iterator) result;
                } else if (result instanceof Iterable) {
                    this.tempIterator = ((Iterable) result).iterator();
                } else if (result instanceof Object[]) {
                    this.tempIterator = new ArrayIterator((Object[]) result);
                } else if (result instanceof Map) {
                    this.tempIterator = ((Map) result).entrySet().iterator();
                } else {
                    io.out.println(resultPrompt + ((null == result) ? NULL : result.toString()));
                    return null;
                }
            }
        }
    }

    class ArrayIterator implements Iterator {

        private final Object[] array;
        private int count = 0;

        public ArrayIterator(final Object[] array) {
            this.array = array;
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }

        public Object next() {
            if (count > array.length)
                throw FastNoSuchElementException.instance();

            return array[count++];
        }

        public boolean hasNext() {
            return count < array.length;
        }
    }
}
