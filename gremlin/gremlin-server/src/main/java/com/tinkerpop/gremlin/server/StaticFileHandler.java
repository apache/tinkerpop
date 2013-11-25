package com.tinkerpop.gremlin.server;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.DefaultFileRegion;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.LastHttpContent;
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
import java.util.Properties;
import java.util.TimeZone;
import java.util.regex.Pattern;

import static io.netty.handler.codec.http.HttpHeaders.Names.CACHE_CONTROL;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpHeaders.Names.DATE;
import static io.netty.handler.codec.http.HttpHeaders.Names.EXPIRES;
import static io.netty.handler.codec.http.HttpHeaders.Names.IF_MODIFIED_SINCE;
import static io.netty.handler.codec.http.HttpHeaders.Names.LAST_MODIFIED;
import static io.netty.handler.codec.http.HttpHeaders.isKeepAlive;
import static io.netty.handler.codec.http.HttpHeaders.setContentLength;
import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_MODIFIED;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

/**
 * Adapted from https://github.com/netty/netty/blob/master/example/src/main/java/io/netty/example/http/file/HttpStaticFileServerHandler.java
 *
 * @author Victor Su
 */
class StaticFileHandler {
    private static final String STATIC_FILE_PATH = "src/main/resources/web";
    private static final String HTTP_DATE_FORMAT = "EEE, dd MMM yyyy HH:mm:ss zzz";
    private static final String HTTP_DATE_GMT_TIMEZONE = "GMT";
    private static final int HTTP_CACHE_SECONDS = 60;
    private static final String HTTP_DEFAULT_CONTENT_TYPE = "text/plain";
    private static final Pattern INSECURE_URI = Pattern.compile(".*[<>&\"].*");

    public void handleHttpStaticFileRequest(final ChannelHandlerContext ctx, final FullHttpRequest req) throws Exception {
        final String uri = req.getUri();
        final String path = sanitizeUri(uri);

        if (path == null) {
            sendError(ctx, FORBIDDEN);
            return;
        }

        final  File file = new File(path);
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
        final String ifModifiedSince = req.headers().get(IF_MODIFIED_SINCE);
        if (ifModifiedSince != null && !ifModifiedSince.isEmpty()) {
            final SimpleDateFormat dateFormatter = new SimpleDateFormat(HTTP_DATE_FORMAT, Locale.US);
            final Date ifModifiedSinceDate = dateFormatter.parse(ifModifiedSince);

            // Only compare up to the second because the datetime format we send to the client
            // does not have milliseconds
            final long ifModifiedSinceDateSeconds = ifModifiedSinceDate.getTime() / 1000;
            final long fileLastModifiedSeconds = file.lastModified() / 1000;
            if (ifModifiedSinceDateSeconds == fileLastModifiedSeconds) {
                sendNotModified(ctx);
                return;
            }
        }

        final RandomAccessFile raf;
        try {
            raf = new RandomAccessFile(file, "r");
        } catch (FileNotFoundException fnfe) {
            sendError(ctx, NOT_FOUND);
            return;
        }
        long fileLength = raf.length();

        final HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
        setContentLength(response, fileLength);
        setContentTypeHeader(response, file);
        setDateAndCacheHeaders(response, file);
        if (isKeepAlive(req)) {
            response.headers().set(CONNECTION, HttpHeaders.Values.KEEP_ALIVE);
        }

        // Write the initial line and the header.
        ctx.write(response);

        // Write the content.
        ctx.write(new DefaultFileRegion(raf.getChannel(), 0, fileLength), ctx.newProgressivePromise());

        // Write the end marker
        final ChannelFuture lastContentFuture = ctx.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);

        // Decide whether to close the connection or not.
        if (!isKeepAlive(req)) {
            // Close the connection when the whole content is written out.
            lastContentFuture.addListener(ChannelFutureListener.CLOSE);
        }
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
        return System.getProperty("user.dir") + File.separator + STATIC_FILE_PATH + uri;
    }

    private static void sendError(final ChannelHandlerContext ctx, final HttpResponseStatus status) {
        final FullHttpResponse response = new DefaultFullHttpResponse(
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
    private static void sendNotModified(final ChannelHandlerContext ctx) {
        final FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, NOT_MODIFIED);
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
    private static void setDateHeader(final FullHttpResponse response) {
        final SimpleDateFormat dateFormatter = new SimpleDateFormat(HTTP_DATE_FORMAT, Locale.US);
        dateFormatter.setTimeZone(TimeZone.getTimeZone(HTTP_DATE_GMT_TIMEZONE));

        final Calendar time = new GregorianCalendar();
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
        final SimpleDateFormat dateFormatter = new SimpleDateFormat(HTTP_DATE_FORMAT, Locale.US);
        dateFormatter.setTimeZone(TimeZone.getTimeZone(HTTP_DATE_GMT_TIMEZONE));

        // Date header
        final Calendar time = new GregorianCalendar();
        response.headers().set(DATE, dateFormatter.format(time.getTime()));

        // Add cache headers
        time.add(Calendar.SECOND, HTTP_CACHE_SECONDS);
        response.headers().set(EXPIRES, dateFormatter.format(time.getTime()));
        response.headers().set(CACHE_CONTROL, "private, max-age=" + HTTP_CACHE_SECONDS);
        response.headers().set(LAST_MODIFIED, dateFormatter.format(new Date(file.lastModified())));
    }

    /**
     * Sets the content type header for the HTTP Response
     *
     * @param response  HTTP response
     * @param file      file to extract content type
     *
     */
    private static void setContentTypeHeader(final HttpResponse response, final File file) {
        final String path = file.getPath();

        final int pos = path.lastIndexOf(".");
        if (pos > 0) {
            final String ext = path.substring(pos + 1);
            final String contentType = MimeType.get(ext, HTTP_DEFAULT_CONTENT_TYPE);
            response.headers().set(CONTENT_TYPE, contentType);
        } else {
            response.headers().set(CONTENT_TYPE, HTTP_DEFAULT_CONTENT_TYPE);
        }
    }

    /**
     * Hardcoded mime-type supported by default.
     *
     * Adapted from https://github.com/Atmosphere/nettosphere/blob/master/server/src/main/java/org/atmosphere/nettosphere/util/MimeType.java
     *
     */
    static class MimeType {

        private final static Properties contentTypes = new Properties();

        static {
            contentTypes.put("abs", "audio/x-mpeg");
            contentTypes.put("ai", "application/postscript");
            contentTypes.put("aif", "audio/x-aiff");
            contentTypes.put("aifc", "audio/x-aiff");
            contentTypes.put("aiff", "audio/x-aiff");
            contentTypes.put("aim", "application/x-aim");
            contentTypes.put("art", "image/x-jg");
            contentTypes.put("asf", "video/x-ms-asf");
            contentTypes.put("asx", "video/x-ms-asf");
            contentTypes.put("au", "audio/basic");
            contentTypes.put("avi", "video/x-msvideo");
            contentTypes.put("avx", "video/x-rad-screenplay");
            contentTypes.put("bcpio", "application/x-bcpio");
            contentTypes.put("bin", "application/octet-stream");
            contentTypes.put("bmp", "image/bmp");
            contentTypes.put("body", "text/html");
            contentTypes.put("cdf", "application/x-cdf");
            contentTypes.put("cer", "application/x-x509-ca-cert");
            contentTypes.put("class", "application/java");
            contentTypes.put("cpio", "application/x-cpio");
            contentTypes.put("csh", "application/x-csh");
            contentTypes.put("css", "text/css");
            contentTypes.put("dib", "image/bmp");
            contentTypes.put("doc", "application/msword");
            contentTypes.put("dtd", "application/xml-dtd");
            contentTypes.put("dv", "video/x-dv");
            contentTypes.put("dvi", "application/x-dvi");
            contentTypes.put("eps", "application/postscript");
            contentTypes.put("etx", "text/x-setext");
            contentTypes.put("exe", "application/octet-stream");
            contentTypes.put("gif", "image/gif");
            contentTypes.put("gk", "application/octet-stream");
            contentTypes.put("gtar", "application/x-gtar");
            contentTypes.put("gz", "application/x-gzip");
            contentTypes.put("hdf", "application/x-hdf");
            contentTypes.put("hqx", "application/mac-binhex40");
            contentTypes.put("htc", "text/x-component");
            contentTypes.put("htm", "text/html");
            contentTypes.put("html", "text/html");
            contentTypes.put("hqx", "application/mac-binhex40");
            contentTypes.put("ief", "image/ief");
            contentTypes.put("jad", "text/vnd.sun.j2me.app-descriptor");
            contentTypes.put("jar", "application/java-archive");
            contentTypes.put("java", "text/plain");
            contentTypes.put("jnlp", "application/x-java-jnlp-file");
            contentTypes.put("jpe", "image/jpeg");
            contentTypes.put("jpeg", "image/jpeg");
            contentTypes.put("jpg", "image/jpeg");
            contentTypes.put("js", "application/javascript");
            contentTypes.put("kar", "audio/x-midi");
            contentTypes.put("latex", "application/x-latex");
            contentTypes.put("m3u", "audio/x-mpegurl");
            contentTypes.put("mac", "image/x-macpaint");
            contentTypes.put("man", "application/x-troff-man");
            contentTypes.put("mathml", "application/mathml+xml");
            contentTypes.put("me", "application/x-troff-me");
            contentTypes.put("mid", "audio/x-midi");
            contentTypes.put("midi", "audio/x-midi");
            contentTypes.put("mif", "application/x-mif");
            contentTypes.put("mov", "video/quicktime");
            contentTypes.put("movie", "video/x-sgi-movie");
            contentTypes.put("mp1", "audio/x-mpeg");
            contentTypes.put("mp2", "audio/x-mpeg");
            contentTypes.put("mp3", "audio/x-mpeg");
            contentTypes.put("mpa", "audio/x-mpeg");
            contentTypes.put("mpe", "video/mpeg");
            contentTypes.put("mpeg", "video/mpeg");
            contentTypes.put("mpega", "audio/x-mpeg");
            contentTypes.put("mpg", "video/mpeg");
            contentTypes.put("mpv2", "video/mpeg2");
            contentTypes.put("ms", "application/x-wais-source");
            contentTypes.put("nc", "application/x-netcdf");
            contentTypes.put("oda", "application/oda");
            contentTypes.put("ogg", "application/ogg");
            contentTypes.put("pbm", "image/x-portable-bitmap");
            contentTypes.put("pct", "image/pict");
            contentTypes.put("pdf", "application/pdf");
            contentTypes.put("pgm", "image/x-portable-graymap");
            contentTypes.put("pic", "image/pict");
            contentTypes.put("pict", "image/pict");
            contentTypes.put("pls", "audio/x-scpls");
            contentTypes.put("png", "image/png");
            contentTypes.put("pnm", "image/x-portable-anymap");
            contentTypes.put("pnt", "image/x-macpaint");
            contentTypes.put("ppm", "image/x-portable-pixmap");
            contentTypes.put("ppt", "application/powerpoint");
            contentTypes.put("ps", "application/postscript");
            contentTypes.put("psd", "image/x-photoshop");
            contentTypes.put("qt", "video/quicktime");
            contentTypes.put("qti", "image/x-quicktime");
            contentTypes.put("qtif", "image/x-quicktime");
            contentTypes.put("ras", "image/x-cmu-raster");
            contentTypes.put("rdf", "application/rdf+xml");
            contentTypes.put("rgb", "image/x-rgb");
            contentTypes.put("rm", "application/vnd.rn-realmedia");
            contentTypes.put("roff", "application/x-troff");
            contentTypes.put("rtf", "application/rtf");
            contentTypes.put("rtx", "text/richtext");
            contentTypes.put("sh", "application/x-sh");
            contentTypes.put("shar", "application/x-shar");
            contentTypes.put("shtml", "text/x-server-parsed-html");
            contentTypes.put("sit", "application/x-stuffit");
            contentTypes.put("smf", "audio/x-midi");
            contentTypes.put("snd", "audio/basic");
            contentTypes.put("src", "application/x-wais-source");
            contentTypes.put("sv4cpio", "application/x-sv4cpio");
            contentTypes.put("sv4crc", "application/x-sv4crc");
            contentTypes.put("svg", "image/svg+xml");
            contentTypes.put("svgz", "image/svg+xml");
            contentTypes.put("swf", "application/x-shockwave-flash");
            contentTypes.put("t", "application/x-troff");
            contentTypes.put("tar", "application/x-tar");
            contentTypes.put("tcl", "application/x-tcl");
            contentTypes.put("tex", "application/x-tex");
            contentTypes.put("texi", "application/x-texinfo");
            contentTypes.put("texinfo", "application/x-texinfo");
            contentTypes.put("tif", "image/tiff");
            contentTypes.put("tiff", "image/tiff");
            contentTypes.put("tr", "application/x-troff");
            contentTypes.put("tsv", "text/tab-separated-values");
            contentTypes.put("txt", "text/plain");
            contentTypes.put("ulw", "audio/basic");
            contentTypes.put("ustar", "application/x-ustar");
            contentTypes.put("xbm", "image/x-xbitmap");
            contentTypes.put("xml", "application/xml");
            contentTypes.put("xpm", "image/x-xpixmap");
            contentTypes.put("xsl", "application/xml");
            contentTypes.put("xslt", "application/xslt+xml");
            contentTypes.put("xwd", "image/x-xwindowdump");
            contentTypes.put("vsd", "application/x-visio");
            contentTypes.put("vxml", "application/voicexml+xml");
            contentTypes.put("wav", "audio/x-wav");
            contentTypes.put("wbmp", "image/vnd.wap.wbmp");
            contentTypes.put("wml", "text/vnd.wap.wml");
            contentTypes.put("wmlc", "application/vnd.wap.wmlc");
            contentTypes.put("wmls", "text/vnd.wap.wmls");
            contentTypes.put("wmlscriptc", "application/vnd.wap.wmlscriptc");
            contentTypes.put("woff", "application/x-font-woff");
            contentTypes.put("wrl", "x-world/x-vrml");
            contentTypes.put("xht", "application/xhtml+xml");
            contentTypes.put("xhtml", "application/xhtml+xml");
            contentTypes.put("xls", "application/vnd.ms-excel");
            contentTypes.put("xul", "application/vnd.mozilla.xul+xml");
            contentTypes.put("Z", "application/x-compress");
            contentTypes.put("z", "application/x-compress");
            contentTypes.put("zip", "application/zip");
        }


        /**
         * @param extension the extension
         * @return the content type associated with <code>extension</code>.  If
         *         no association is found, this method will return <code>text/plain</code>
         */
        public static String get(final String extension) {
            return contentTypes.getProperty(extension, "text/plain");
        }


        /**
         * @param extension the extension
         * @param defaultCt the content type to return if there is no known
         *                  association for the specified extension
         * @return the content type associated with <code>extension</code>
         *         or if no associate is found, returns <code>defaultCt</code>
         */
        public static String get(final String extension, final String defaultCt) {
            return contentTypes.getProperty(extension, defaultCt);
        }


        /**
         * @param extension the extension
         * @return <code>true</code> if the specified extension has been registered
         *         otherwise, returns <code>false</code>
         */
        public static boolean contains(final String extension) {
            return (contentTypes.getProperty(extension) != null);
        }


        /**
         * <p>
         * Associates the specified extension and content type
         * </p>
         *
         * @param extension   the extension
         * @param contentType the content type associated with the extension
         */
        public static void add(final String extension, final String contentType) {
            if (extension == null
                    || extension.length() == 0
                    || contentType == null
                    || contentType.length() == 0) {
                return;
            }
            contentTypes.put(extension, contentType);
        }
    }


}
