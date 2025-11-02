/*
 * Copyright 2024 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.storage.http.io;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.*;
import io.pixelsdb.pixels.common.utils.Constants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.net.ssl.SSLException;
import java.io.IOException;
import java.io.InputStream;
import java.security.cert.CertificateException;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class HttpInputStream extends InputStream {
    private static final Logger logger = LogManager.getLogger(HttpInputStream.class);

    /**
     * Indicates whether the stream is still open / valid
     */
    private volatile boolean open;

    /**
     * The schema of http.
     * Default value is http.
     */
    private final String schema = "http";

    /**
     * The host of http.
     */
    private String host;

    /**
     * The port of http.
     */
    private int port;

    /**
     * The uri of http.
     */
    private String uri;

    /**
     * The temporary buffer used for storing the received data chunks.
     * Using LinkedBlockingQueue with capacity limit to prevent OOM.
     */
    private final BlockingQueue<ByteBuf> contentQueue;

    /**
     * Maximum queue capacity to prevent memory overflow.
     * When queue is full, server will apply backpressure.
     * Increased to handle burst traffic - each slot is one HTTP request (typically
     * 8MB in tests).
     * 200 slots * 8MB = ~1.6GB max queue memory usage.
     */
    private static final int MAX_QUEUE_CAPACITY = 200;

    /**
     * Lock for coordinating data availability between producer (server) and
     * consumer (read).
     */
    private final Lock dataLock = new ReentrantLock();

    /**
     * Condition to signal when new data arrives, enabling efficient async
     * notification.
     */
    private final Condition dataAvailable = dataLock.newCondition();

    /**
     * Counter for tracking total bytes received.
     */
    private final AtomicLong receivedBytes = new AtomicLong(0);

    /**
     * Counter for tracking current queue size for monitoring.
     */
    private final AtomicInteger queueSize = new AtomicInteger(0);

    /**
     * Flag to indicate if the stream end signal has been received.
     */
    private volatile boolean streamEndReceived = false;

    /**
     * Latch to signal when the HTTP server is ready to accept connections.
     */
    private final CountDownLatch serverReadyLatch = new CountDownLatch(1);

    /**
     * Maximum time to wait for server to start (in seconds).
     */
    private static final int SERVER_START_TIMEOUT_SECONDS = 30;

    /**
     * The maximum tries to get data.
     */
    private final int MAX_TRIES = Constants.MAX_STREAM_RETRY_COUNT;

    /**
     * The milliseconds to sleep.
     */
    private final long DELAY_MS = Constants.STREAM_DELAY_MS;

    /**
     * The http server for receiving input http.
     */
    private final HttpServer httpServer;

    /**
     * The thread to run http server.
     */
    private final ExecutorService executorService;

    /**
     * The future of http server.
     */
    private final CompletableFuture<Void> httpServerFuture;

    public HttpInputStream(String host, int port) throws CertificateException, SSLException, IOException {
        this.open = true;
        this.contentQueue = new LinkedBlockingQueue<>(MAX_QUEUE_CAPACITY);
        this.host = host;
        this.port = port;
        this.uri = this.schema + "://" + host + ":" + port;
        this.httpServer = new HttpServer(new StreamHttpServerHandler(this.contentQueue, this.dataLock,
                this.dataAvailable, this.receivedBytes, this.queueSize, () -> this.streamEndReceived = true));

        // Set callback to be notified when server is ready
        this.httpServer.setOnServerReady(() -> {
            logger.info("HTTP server successfully bound to port {}", port);
            serverReadyLatch.countDown();
        });

        this.executorService = Executors.newFixedThreadPool(1);
        this.httpServerFuture = CompletableFuture.runAsync(() -> {
            try {
                logger.info("Starting HTTP server on port {}...", port);
                this.httpServer.serve(this.port);
            } catch (InterruptedException e) {
                logger.error("HTTP server interrupted", e);
                Thread.currentThread().interrupt();
            }
        }, this.executorService);

        // Wait for server to be ready before returning
        try {
            logger.info("Waiting for HTTP server to be ready on port {}...", port);
            if (!serverReadyLatch.await(SERVER_START_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                throw new IOException("HTTP server failed to start within " +
                        SERVER_START_TIMEOUT_SECONDS + " seconds");
            }
            logger.info("HTTP server is ready and accepting connections on port {}", port);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting for HTTP server to start", e);
        }
    }

    @Override
    public int read() throws IOException {
        assertOpen();
        if (emptyData()) {
            return -1;
        }

        ByteBuf content = this.contentQueue.peek();
        int b = -1;
        if (content != null) {
            b = content.readUnsignedByte();
            if (!content.isReadable()) {
                content.release();
                this.contentQueue.poll();
                queueSize.decrementAndGet();
            }
        }
        return b;
    }

    @Override
    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    /**
     * Attempt to read data with a maximum length of len into the position off of
     * buf.
     * Optimized for async operation with efficient buffer management.
     *
     * @param buf the buffer to read into
     * @param off the offset in the buffer
     * @param len the maximum number of bytes to read
     * @return actual number of bytes read, or -1 if end of stream
     * @throws IOException if an I/O error occurs
     */
    @Override
    public int read(byte[] buf, int off, int len) throws IOException {
        assertOpen();

        ByteBuf content;
        int readBytes = 0;
        while (readBytes < len) {
            if (emptyData()) {
                return readBytes > 0 ? readBytes : -1;
            }
            content = this.contentQueue.peek();
            if (content == null) {
                return readBytes > 0 ? readBytes : -1;
            }

            try {
                int readLen = Math.min(len - readBytes, content.readableBytes());
                content.readBytes(buf, off + readBytes, readLen);
                readBytes += readLen;
                if (!content.isReadable()) {
                    contentQueue.poll();
                    content.release();
                    queueSize.decrementAndGet();
                }
            } catch (Exception e) {
                if (!content.isReadable()) {
                    contentQueue.poll();
                    content.release();
                    queueSize.decrementAndGet();
                }
                throw e;
            }
        }

        return readBytes;
    }

    @Override
    public void close() throws IOException {
        if (this.open) {
            this.open = false;
            this.httpServerFuture.complete(null);
            this.httpServer.close();

            // Clean up remaining ByteBufs to prevent memory leak
            ByteBuf buf;
            int releasedCount = 0;
            while ((buf = contentQueue.poll()) != null) {
                buf.release();
                releasedCount++;
            }

            logger.info("HttpInputStream closed. Total received: {} bytes, released {} buffers",
                    receivedBytes.get(), releasedCount);
        }
    }

    /**
     * Checks if data is available in the queue. Uses efficient condition-based
     * waiting
     * instead of polling to reduce CPU usage.
     *
     * @return true if queue is empty and no more data expected, false otherwise
     * @throws IOException if interrupted while waiting
     */
    private boolean emptyData() throws IOException {
        if (!this.contentQueue.isEmpty()) {
            return false;
        }

        // If stream end has been signaled and queue is empty, return true
        if (streamEndReceived && this.contentQueue.isEmpty()) {
            logger.debug("Stream end received and queue is empty");
            return true;
        }

        // Use condition-based waiting for better performance
        dataLock.lock();
        try {
            long waitTimeMs = DELAY_MS * MAX_TRIES;
            long deadline = System.currentTimeMillis() + waitTimeMs;

            while (this.contentQueue.isEmpty() && !streamEndReceived && !this.httpServerFuture.isDone()) {
                long remaining = deadline - System.currentTimeMillis();
                if (remaining <= 0) {
                    logger.warn("Timeout waiting for data. Queue size: {}, stream end: {}, server done: {}",
                            queueSize.get(), streamEndReceived, httpServerFuture.isDone());
                    break;
                }

                try {
                    // Efficient waiting using condition variable
                    dataAvailable.await(remaining, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted while waiting for data", e);
                }
            }
        } finally {
            dataLock.unlock();
        }

        return this.contentQueue.isEmpty();
    }

    private void assertOpen() {
        if (!this.open) {
            throw new IllegalStateException("Closed");
        }
    }

    public static class StreamHttpServerHandler extends HttpServerHandler {
        private static final Logger logger = LogManager.getLogger(StreamHttpServerHandler.class);
        private final BlockingQueue<ByteBuf> contentQueue;
        private final Lock dataLock;
        private final Condition dataAvailable;
        private final AtomicLong receivedBytes;
        private final AtomicInteger queueSize;
        private final Runnable streamEndCallback;

        public StreamHttpServerHandler(BlockingQueue<ByteBuf> contentQueue, Lock dataLock,
                Condition dataAvailable, AtomicLong receivedBytes,
                AtomicInteger queueSize, Runnable streamEndCallback) {
            this.contentQueue = contentQueue;
            this.dataLock = dataLock;
            this.dataAvailable = dataAvailable;
            this.receivedBytes = receivedBytes;
            this.queueSize = queueSize;
            this.streamEndCallback = streamEndCallback;
        }

        @Override
        public void channelRead0(ChannelHandlerContext ctx, HttpObject msg) {
            if (!(msg instanceof HttpRequest)) {
                return;
            }
            FullHttpRequest req = (FullHttpRequest) msg;
            if (req.method() != HttpMethod.POST) {
                sendResponse(ctx, req, HttpResponseStatus.OK);
                return;
            }

            if (!req.headers().get(HttpHeaderNames.CONTENT_TYPE).equals("application/x-protobuf")) {
                sendResponse(ctx, req, HttpResponseStatus.BAD_REQUEST);
                return;
            }

            ByteBuf content = req.content();

            // Check for stream end signal (Content-Length: 0 with Connection: close)
            if (req.headers().get(HttpHeaderNames.CONNECTION).equals(HttpHeaderValues.CLOSE.toString())) {
                logger.info("Received stream end signal");
                streamEndCallback.run();
                dataLock.lock();
                try {
                    dataAvailable.signalAll();
                } finally {
                    dataLock.unlock();
                }
                sendResponse(ctx, req, HttpResponseStatus.OK);
                return;
            }

            if (content.isReadable()) {
                int contentLength = content.readableBytes();

                // Try to add to queue with backpressure handling
                content.retain();
                boolean added = contentQueue.offer(content);

                if (added) {
                    queueSize.incrementAndGet();
                    receivedBytes.addAndGet(contentLength);

                    // Signal waiting readers that data is available
                    dataLock.lock();
                    try {
                        dataAvailable.signal();
                    } finally {
                        dataLock.unlock();
                    }

                    logger.debug("Received {} bytes, queue size: {}", contentLength, queueSize.get());
                    sendResponse(ctx, req, HttpResponseStatus.OK);
                } else {
                    // Queue is full, apply backpressure
                    content.release();
                    logger.warn("Queue full ({}), rejecting request to apply backpressure", contentQueue.size());
                    sendResponse(ctx, req, HttpResponseStatus.SERVICE_UNAVAILABLE);
                }
            } else {
                sendResponse(ctx, req, HttpResponseStatus.OK);
            }
        }

        private void sendResponse(ChannelHandlerContext ctx, FullHttpRequest req, HttpResponseStatus status) {
            FullHttpResponse response = new DefaultFullHttpResponse(req.protocolVersion(), status);
            response.headers()
                    .set(HttpHeaderNames.CONTENT_TYPE, "text/plain")
                    .set(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes());

            if (req.headers().get(HttpHeaderNames.CONNECTION).equals(HttpHeaderValues.CLOSE.toString())) {
                response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE);
                response.setStatus(status);
                ctx.writeAndFlush(response);
                this.serverCloser.run();
            } else {
                response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
                response.setStatus(status);
                ctx.writeAndFlush(response);
            }
        }
    }
}
