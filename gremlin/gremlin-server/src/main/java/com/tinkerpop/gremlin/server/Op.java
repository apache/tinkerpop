package com.tinkerpop.gremlin.server;

import com.tinkerpop.blueprints.tinkergraph.TinkerFactory;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.websocketx.ContinuationWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import org.codehaus.groovy.jsr223.GroovyScriptEngineImpl;

import javax.script.Bindings;
import javax.script.ScriptException;
import javax.script.SimpleBindings;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@FunctionalInterface
public interface Op {
    void process(Context context);

    public static class EvalOp implements Op {
        @Override
        public void process(final Context context) {
            Bindings binding = new SimpleBindings();
            binding.put("g", TinkerFactory.createClassic());

            GroovyScriptEngineImpl se = new GroovyScriptEngineImpl();
            Object o;
            try {
                o = se.eval(context.getRequestMessage().args.get("gremlin").toString(), binding);
            } catch (ScriptException ex) {
                ex.printStackTrace();
                o = null;
            }

            final ChannelHandlerContext ctx = context.getChannelHandlerContext();
            final AtomicInteger counter = new AtomicInteger(1);
            if (o instanceof Iterator) {
                ((Iterator) o).forEachRemaining(j -> ctx.channel().write(new TextWebSocketFrame(j.toString() + " " + (counter.getAndIncrement()))));
            } else if (o instanceof Iterable) {
                ((Iterable) o).forEach(j -> ctx.channel().write(new ContinuationWebSocketFrame(false, 0, j.toString())));
            } else
                ctx.channel().write(new TextWebSocketFrame(o.toString()));
        }
    }
}
