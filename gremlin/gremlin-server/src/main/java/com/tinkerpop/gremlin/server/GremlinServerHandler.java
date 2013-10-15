package com.tinkerpop.gremlin.server;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.DefaultFileRegion;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PingWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PongWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketServerHandshaker;
import io.netty.handler.codec.http.websocketx.WebSocketServerHandshakerFactory;
import io.netty.handler.stream.ChunkedFile;
import io.netty.util.CharsetUtil;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Locale;
import java.util.TimeZone;
import java.util.regex.Pattern;

import static io.netty.handler.codec.http.HttpHeaders.Names.CACHE_CONTROL;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpHeaders.Names.DATE;
import static io.netty.handler.codec.http.HttpHeaders.Names.EXPIRES;
import static io.netty.handler.codec.http.HttpHeaders.Names.HOST;
import static io.netty.handler.codec.http.HttpHeaders.Names.IF_MODIFIED_SINCE;
import static io.netty.handler.codec.http.HttpHeaders.Names.LAST_MODIFIED;
import static io.netty.handler.codec.http.HttpHeaders.isKeepAlive;
import static io.netty.handler.codec.http.HttpHeaders.setContentLength;
import static io.netty.handler.codec.http.HttpMethod.GET;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_MODIFIED;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

/**
 * Adapted from https://github.com/netty/netty/tree/netty-4.0.10.Final/example/src/main/java/io/netty/example/http/websocketx/server
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GremlinServerHandler extends SimpleChannelInboundHandler<Object> {
    //private static final Logger logger = Logger.getLogger(GremlinServerHandler.class.getName());

    private static final String HTTP_DATE_FORMAT = "EEE, dd MMM yyyy HH:mm:ss zzz";
    private static final String HTTP_DATE_GMT_TIMEZONE = "GMT";
    private static final int HTTP_CACHE_SECONDS = 60;
    private static final String HTTP_DEFAULT_CONTENT_TYPE = "text/plain";
    private static final Pattern INSECURE_URI = Pattern.compile(".*[<>&\"].*");

    private WebSocketServerHandshaker handshaker;
    private final boolean useSendFile;
    private final String webSocketRoute;
    private final String staticFilePath;

    public GremlinServerHandler() {
        this("/gremlin", "src/main/resources/web", true);
    }

    public GremlinServerHandler(final String webSocketRoute, final String staticFilePath, final boolean useSendFile) {
        this.webSocketRoute = webSocketRoute;
        this.staticFilePath = staticFilePath;
        this.useSendFile = useSendFile;
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof FullHttpRequest) {
            handleHttpRequest(ctx, (FullHttpRequest) msg);
        } else if (msg instanceof WebSocketFrame) {
            handleWebSocketFrame(ctx, (WebSocketFrame) msg);
        }
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        ctx.flush();
    }

    private void handleHttpRequest(ChannelHandlerContext ctx, FullHttpRequest req) throws Exception {
        // Handle a bad request.
        if (!req.getDecoderResult().isSuccess()) {
            sendHttpResponse(ctx, req, new DefaultFullHttpResponse(HTTP_1_1, BAD_REQUEST));
            return;
        }

        // Allow only GET methods.
        if (req.getMethod() != GET) {
            sendHttpResponse(ctx, req, new DefaultFullHttpResponse(HTTP_1_1, FORBIDDEN));
            return;
        }

        final String uri = req.getUri();

        if (uri.startsWith(webSocketRoute)) {
            // Web socket handshake
            WebSocketServerHandshakerFactory wsFactory = new WebSocketServerHandshakerFactory(
                    getWebSocketLocation(req), null, false);
            handshaker = wsFactory.newHandshaker(req);
            if (handshaker == null) {
                WebSocketServerHandshakerFactory.sendUnsupportedWebSocketVersionResponse(ctx.channel());
            } else {
                handshaker.handshake(ctx.channel(), req);
            }
        } else {
            // Static file request
            final String path = sanitizeUri(uri);

            if (path == null) {
                sendError(ctx, FORBIDDEN);
                return;
            }

            File file = new File(path);
            if (file.isHidden() || !file.exists()) {
                sendError(ctx, NOT_FOUND);
                return;
            }

            if (file.isDirectory()) {
                sendError(ctx, FORBIDDEN);
                return;
            }

            if (!file.isFile()) {
                sendError(ctx, FORBIDDEN);
                return;
            }

            // Cache Validation
            String ifModifiedSince = req.headers().get(IF_MODIFIED_SINCE);
            if (ifModifiedSince != null && !ifModifiedSince.isEmpty()) {
                SimpleDateFormat dateFormatter = new SimpleDateFormat(HTTP_DATE_FORMAT, Locale.US);
                Date ifModifiedSinceDate = dateFormatter.parse(ifModifiedSince);

                // Only compare up to the second because the datetime format we send to the client
                // does not have milliseconds
                long ifModifiedSinceDateSeconds = ifModifiedSinceDate.getTime() / 1000;
                long fileLastModifiedSeconds = file.lastModified() / 1000;
                if (ifModifiedSinceDateSeconds == fileLastModifiedSeconds) {
                    sendNotModified(ctx);
                    return;
                }
            }

            RandomAccessFile raf;
            try {
                raf = new RandomAccessFile(file, "r");
            } catch (FileNotFoundException fnfe) {
                sendError(ctx, NOT_FOUND);
                return;
            }
            long fileLength = raf.length();

            HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
            setContentLength(response, fileLength);
            setContentTypeHeader(response, file);
            setDateAndCacheHeaders(response, file);
            if (isKeepAlive(req)) {
                response.headers().set(CONNECTION, HttpHeaders.Values.KEEP_ALIVE);
            }

            // Write the initial line and the header.
            ctx.write(response);

            // Write the content.
            ChannelFuture sendFileFuture;
            if (useSendFile) {
                sendFileFuture =
                        ctx.write(new DefaultFileRegion(raf.getChannel(), 0, fileLength), ctx.newProgressivePromise());
            } else {
                sendFileFuture =
                        ctx.write(new ChunkedFile(raf, 0, fileLength, 8192), ctx.newProgressivePromise());
            }

            // Write the end marker
            ChannelFuture lastContentFuture = ctx.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);

            // Decide whether to close the connection or not.
            if (!isKeepAlive(req)) {
                // Close the connection when the whole content is written out.
                lastContentFuture.addListener(ChannelFutureListener.CLOSE);
            }
        }
    }

    private void handleWebSocketFrame(ChannelHandlerContext ctx, WebSocketFrame frame) {

        // Check for closing frame
        if (frame instanceof CloseWebSocketFrame) {
            handshaker.close(ctx.channel(), (CloseWebSocketFrame) frame.retain());
            return;
        }
        if (frame instanceof PingWebSocketFrame) {
            ctx.channel().write(new PongWebSocketFrame(frame.content().retain()));
            return;
        }
        if (!(frame instanceof TextWebSocketFrame)) {
            throw new UnsupportedOperationException(String.format("%s frame types not supported", frame.getClass()
                    .getName()));
        }

        // todo: support both text and binary where binary allows for versioning of the messages.
        final String request = ((TextWebSocketFrame) frame).text();
        final RequestMessage requestMessage = RequestMessage.Serializer.parse(request);

        // todo: write an error if the op code is not recognized
        OpProcessor.instance().select(requestMessage)
                .orElse((c) -> c.getChannelHandlerContext().write("error")).accept(new Context(requestMessage, ctx));

    }

    private static void sendHttpResponse(
        ChannelHandlerContext ctx, FullHttpRequest req, FullHttpResponse res) {

        // Generate an error page if response getStatus code is not OK (200).
        if (res.getStatus().code() != 200) {
            ByteBuf buf = Unpooled.copiedBuffer(res.getStatus().toString(), CharsetUtil.UTF_8);
            res.content().writeBytes(buf);
            buf.release();
            setContentLength(res, res.content().readableBytes());
        }

        // Send the response and close the connection if necessary.
        ChannelFuture f = ctx.channel().writeAndFlush(res);
        if (!isKeepAlive(req) || res.getStatus().code() != 200) {
            f.addListener(ChannelFutureListener.CLOSE);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        cause.printStackTrace();
        ctx.close();
    }

    private String getWebSocketLocation(FullHttpRequest req) {
        return "ws://" + req.headers().get(HOST) + webSocketRoute;
    }

    private String sanitizeUri(String uri) {
        // Decode the path.
        try {
            uri = URLDecoder.decode(uri, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            try {
                uri = URLDecoder.decode(uri, "ISO-8859-1");
            } catch (UnsupportedEncodingException e1) {
                throw new Error();
            }
        }

        if (!uri.startsWith("/")) {
            return null;
        }

        if (uri.endsWith("/")) {
            uri = "/index.html";
        }

        // Strip URL parameters
        int pos = uri.indexOf("?");
        if (pos != -1) {
            uri = uri.substring(0, pos);
        }

        // Convert file separators.
        uri = uri.replace('/', File.separatorChar);

        // Simplistic dumb security check.
        // You will have to do something serious in the production environment.
        if (uri.contains(File.separator + '.') ||
                uri.contains('.' + File.separator) ||
                uri.startsWith(".") || uri.endsWith(".") ||
                INSECURE_URI.matcher(uri).matches()) {
            return null;
        }

        // Convert to absolute path.
        return System.getProperty("user.dir") + File.separator + staticFilePath + uri;
    }

    private static void sendError(ChannelHandlerContext ctx, HttpResponseStatus status) {
        FullHttpResponse response = new DefaultFullHttpResponse(
                HTTP_1_1, status, Unpooled.copiedBuffer("Failure: " + status.toString() + "\r\n", CharsetUtil.UTF_8));
        response.headers().set(CONTENT_TYPE, "text/plain; charset=UTF-8");

        // Close the connection as soon as the error message is sent.
        ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
    }

    /**
     * When file timestamp is the same as what the browser is sending up, send a "304 Not Modified"
     *
     * @param ctx  context
     *
     */
    private static void sendNotModified(ChannelHandlerContext ctx) {
        FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, NOT_MODIFIED);
        setDateHeader(response);

        // Close the connection as soon as the error message is sent.
        ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
    }

    /**
     * Sets the Date header for the HTTP response
     *
     * @param response  HTTP response
     *
     */
    private static void setDateHeader(FullHttpResponse response) {
        SimpleDateFormat dateFormatter = new SimpleDateFormat(HTTP_DATE_FORMAT, Locale.US);
        dateFormatter.setTimeZone(TimeZone.getTimeZone(HTTP_DATE_GMT_TIMEZONE));

        Calendar time = new GregorianCalendar();
        response.headers().set(DATE, dateFormatter.format(time.getTime()));
    }

    /**
     * Sets the Date and Cache headers for the HTTP Response
     *
     * @param response  HTTP response
     * @param file      file to extract date and cache
     *
     */
    private static void setDateAndCacheHeaders(HttpResponse response, File file) {
        SimpleDateFormat dateFormatter = new SimpleDateFormat(HTTP_DATE_FORMAT, Locale.US);
        dateFormatter.setTimeZone(TimeZone.getTimeZone(HTTP_DATE_GMT_TIMEZONE));

        // Date header
        Calendar time = new GregorianCalendar();
        response.headers().set(DATE, dateFormatter.format(time.getTime()));

        // Add cache headers
        time.add(Calendar.SECOND, HTTP_CACHE_SECONDS);
        response.headers().set(EXPIRES, dateFormatter.format(time.getTime()));
        response.headers().set(CACHE_CONTROL, "private, max-age=" + HTTP_CACHE_SECONDS);
        response.headers().set(
                LAST_MODIFIED, dateFormatter.format(new Date(file.lastModified())));
    }

    /**
     * Sets the content type header for the HTTP Response
     *
     * @param response  HTTP response
     * @param file      file to extract content type
     *
     */
    private static void setContentTypeHeader(HttpResponse response, File file) {
        String path = file.getPath();

        int pos = path.lastIndexOf(".");
        if (pos > 0) {
            String ext = path.substring(pos + 1);
            String contentType = MimeType.get(ext, HTTP_DEFAULT_CONTENT_TYPE);
            response.headers().set(CONTENT_TYPE, contentType);
        } else {
            response.headers().set(CONTENT_TYPE, HTTP_DEFAULT_CONTENT_TYPE);
        }
    }
}
