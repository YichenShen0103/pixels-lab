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
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.pixelsdb.pixels.common.utils.Constants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.Dsl;
import org.asynchttpclient.Request;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class HttpOutputStream extends OutputStream {
    private static final Logger logger = LogManager.getLogger(HttpOutputStream.class);

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
     * The maximum retry count.
     */
    private static final int MAX_RETRIES = Constants.MAX_STREAM_RETRY_COUNT;

    /**
     * The delay between two tries.
     */
    private static final long RETRY_DELAY_MS = Constants.STREAM_DELAY_MS;

    /**
     * The temporary buffer used for storing the chunks.
     * Using Netty's PooledByteBuf for zero-copy and memory efficiency.
     */
    private ByteBuf buffer;

    /**
     * ByteBuf allocator for creating pooled buffers.
     */
    private static final PooledByteBufAllocator allocator = PooledByteBufAllocator.DEFAULT;

    /**
     * The capacity of buffer.
     */
    private final int bufferCapacity;

    /**
     * The background thread to send requests.
     */
    private final ExecutorService executorService;

    /**
     * The http client.
     */
    private final AsyncHttpClient httpClient;

    /**
     * Semaphore to limit the number of concurrent in-flight requests to prevent
     * overwhelming the server.
     * This enables true async transfer while maintaining resource control.
     */
    private final Semaphore inflightLimiter;

    /**
     * Maximum number of concurrent in-flight HTTP requests.
     */
    private static final int MAX_INFLIGHT_REQUESTS = 10;

    /**
     * Counter for tracking the number of currently in-flight requests.
     */
    private final AtomicInteger inflightCount = new AtomicInteger(0);

    /**
     * Counter for tracking total bytes sent successfully.
     */
    private final AtomicLong sentBytes = new AtomicLong(0);

    /**
     * Counter for tracking failed requests.
     */
    private final AtomicInteger failedRequests = new AtomicInteger(0);

    /**
     * The queue to put pending requests.
     * Changed from byte[] to ByteBuf to avoid memory copy.
     */
    private final BlockingQueue<ByteBuf> contentQueue;

    public HttpOutputStream(String host, int port, int bufferCapacity) {
        this.open = true;
        this.host = host;
        this.port = port;
        this.uri = this.schema + "://" + host + ":" + port;
        this.bufferCapacity = bufferCapacity;
        // Use pooled direct buffer for zero-copy performance
        this.buffer = allocator.directBuffer(bufferCapacity);
        this.httpClient = Dsl.asyncHttpClient();
        this.inflightLimiter = new Semaphore(MAX_INFLIGHT_REQUESTS);
        this.executorService = Executors.newSingleThreadExecutor();
        this.contentQueue = new LinkedBlockingQueue<>();

        // Start background thread to dispatch async send requests
        this.executorService.submit(() -> {
            while (true) {
                try {
                    ByteBuf content = contentQueue.take();
                    if (content.readableBytes() == 0) {
                        // Empty content signals stream close
                        content.release();
                        // CRITICAL: Wait for all in-flight requests before closing
                        logger.info("Close signal received, waiting for all in-flight requests to complete...");
                        try {
                            waitForInflightRequests();
                            closeStreamReader();
                        } catch (InterruptedException e) {
                            logger.error("Interrupted while waiting for in-flight requests during close", e);
                            Thread.currentThread().interrupt();
                        }
                        break;
                    }
                    sendContentAsync(content);
                } catch (InterruptedException e) {
                    logger.error("Background thread interrupted", e);
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        });
    }

    /**
     * Write an array to the HTTP output stream
     *
     * @param b the whole buffer to write
     * @throws IOException exception may be thrown by write
     */
    @Override
    public void write(byte[] b) throws IOException {
        write(b, 0, b.length);
    }

    @Override
    public void write(final byte[] buf, final int off, final int len) throws IOException {
        this.assertOpen();
        int offsetInBuf = off, remainToRead = len;
        int remainInBuffer;
        while (remainToRead > (remainInBuffer = this.buffer.writableBytes())) {
            this.buffer.writeBytes(buf, offsetInBuf, remainInBuffer);
            flushBufferAndRewind();
            offsetInBuf += remainInBuffer;
            remainToRead -= remainInBuffer;
        }
        this.buffer.writeBytes(buf, offsetInBuf, remainToRead);
    }

    @Override
    public void write(int b) throws IOException {
        this.assertOpen();
        if (this.buffer.writableBytes() == 0) {
            flushBufferAndRewind();
        }
        this.buffer.writeByte(b);
    }

    @Override
    public synchronized void flush() {
        assertOpen();
        if (this.buffer.readableBytes() > 0) {
            // Copy readable bytes to a new buffer to avoid holding the large buffer
            int readableBytes = this.buffer.readableBytes();
            ByteBuf copy = allocator.buffer(readableBytes);
            copy.writeBytes(this.buffer, this.buffer.readerIndex(), readableBytes);
            this.contentQueue.add(copy);
            this.buffer.clear();
        }
    }

    protected void flushBufferAndRewind() throws IOException {
        int readableBytes = this.buffer.readableBytes();
        logger.debug("Sending {} bytes to http", readableBytes);
        // Copy to a new buffer instead of retainedSlice to release the original buffer
        ByteBuf content = allocator.buffer(readableBytes);
        content.writeBytes(this.buffer, this.buffer.readerIndex(), readableBytes);
        this.buffer.clear();
        this.contentQueue.add(content);
    }

    @Override
    public void close() throws IOException {
        if (this.open) {
            this.open = false;
            if (this.buffer.readableBytes() > 0) {
                flush();
            }
            // Signal the background thread to close with empty buffer
            this.contentQueue.add(allocator.buffer(0));
            this.executorService.shutdown();
            try {
                // Wait for the background thread to complete (it will wait for in-flight
                // requests internally)
                if (!this.executorService.awaitTermination(120, TimeUnit.SECONDS)) {
                    logger.error("Executor service did not terminate in time");
                    this.executorService.shutdownNow();
                }
                this.httpClient.close();
                // Release the buffer
                if (this.buffer.refCnt() > 0) {
                    this.buffer.release();
                }
            } catch (InterruptedException e) {
                this.executorService.shutdownNow();
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while waiting for termination", e);
            }
            logger.info("HttpOutputStream closed. Total sent: {} bytes, failed requests: {}",
                    sentBytes.get(), failedRequests.get());
        }
    }

    /**
     * Sends content asynchronously without blocking.
     * Uses a semaphore to limit concurrent requests and prevent overwhelming the
     * server.
     * Implements automatic retry logic with exponential backoff for failed
     * requests.
     *
     * @param content the ByteBuf to send (will be released after sending)
     */
    private void sendContentAsync(ByteBuf content) {
        sendContentAsync(content, 0);
    }

    /**
     * Sends content asynchronously with retry support.
     *
     * @param content    the ByteBuf to send
     * @param retryCount current retry attempt number
     */
    private void sendContentAsync(ByteBuf content, int retryCount) {
        try {
            inflightLimiter.acquire();
            inflightCount.incrementAndGet();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Interrupted while waiting for inflight limiter", e);
            failedRequests.incrementAndGet();
            content.release();
            return;
        }

        int contentLength = content.readableBytes();

        // Convert ByteBuf to byte array for AsyncHttpClient
        // Note: This is still one copy, but we've eliminated previous copies in flush()
        byte[] data = new byte[contentLength];
        content.getBytes(content.readerIndex(), data);

        Request req = httpClient.preparePost(this.uri)
                .setBody(data)
                .addHeader(HttpHeaderNames.CONTENT_TYPE, "application/x-protobuf")
                .addHeader(HttpHeaderNames.CONTENT_LENGTH, String.valueOf(contentLength))
                .addHeader(HttpHeaderNames.CONNECTION, "keep-alive")
                .build();

        httpClient.executeRequest(req)
                .toCompletableFuture()
                .whenComplete((response, throwable) -> {
                    inflightLimiter.release();
                    inflightCount.decrementAndGet();

                    // Release the ByteBuf after sending
                    if (content.refCnt() > 0) {
                        content.release();
                    }

                    if (throwable != null) {
                        handleError(data, throwable, retryCount);
                    } else if (response.getStatusCode() == 503) {
                        // Service unavailable (backpressure) - need to retry
                        logger.debug("Received 503 Service Unavailable (backpressure), will retry");
                        handleError(data, new IOException("Service Unavailable (503) - backpressure"), retryCount);
                    } else if (response.getStatusCode() >= 400) {
                        // Other error status codes
                        logger.error("Received error status code: {}", response.getStatusCode());
                        handleError(data, new IOException("HTTP error: " + response.getStatusCode()), retryCount);
                    } else {
                        // Successfully sent (2xx status)
                        sentBytes.addAndGet(contentLength);
                        logger.debug("Successfully sent {} bytes", contentLength);
                    }
                });
    }

    /**
     * Handles errors from async send operations with retry logic.
     *
     * @param content    the content that failed to send (byte array)
     * @param throwable  the error that occurred
     * @param retryCount current retry attempt number
     */
    private void handleError(byte[] content, Throwable throwable, int retryCount) {
        Throwable cause = throwable.getCause() != null ? throwable.getCause() : throwable;
        boolean shouldRetry = cause instanceof IOException && retryCount < MAX_RETRIES;

        if (shouldRetry) {
            logger.warn("Send failed (attempt {}/{}), retrying: {}",
                    retryCount + 1, MAX_RETRIES, cause.getMessage());

            // IMPORTANT: Don't decrement inflightCount here, let the retry handle it
            // Increment it back since we're going to retry
            inflightCount.incrementAndGet();

            // Use shorter delay for backpressure (503), longer for other errors
            boolean isBackpressure = cause.getMessage() != null &&
                    cause.getMessage().contains("Service Unavailable (503)");
            long delay = isBackpressure ? 50 : // Quick retry for backpressure
                    RETRY_DELAY_MS * (1L << Math.min(retryCount, 5)); // Exponential backoff for others

            CompletableFuture.delayedExecutor(delay, TimeUnit.MILLISECONDS)
                    .execute(() -> {
                        inflightCount.decrementAndGet(); // Decrement before retry
                        // Wrap byte array back into ByteBuf for retry
                        ByteBuf retryBuf = allocator.buffer(content.length);
                        retryBuf.writeBytes(content);
                        sendContentAsync(retryBuf, retryCount + 1);
                    });
        } else {
            logger.error("Failed to send {} bytes after {} retries: {}",
                    content.length, retryCount, cause.getMessage());
            failedRequests.incrementAndGet();
            // Data lost - this is a critical error for reliability
        }
    }

    /**
     * Waits for all in-flight async requests to complete.
     * Called during stream close to ensure all data is sent before cleanup.
     *
     * @throws InterruptedException if interrupted while waiting
     */
    private void waitForInflightRequests() throws InterruptedException {
        long startTime = System.currentTimeMillis();
        int maxWaitSeconds = 120; // Increased timeout for large data transfers

        while (inflightCount.get() > 0) {
            long elapsed = (System.currentTimeMillis() - startTime) / 1000;
            if (elapsed > maxWaitSeconds) {
                logger.error("Timeout waiting for {} in-flight requests to complete after {} seconds",
                        inflightCount.get(), maxWaitSeconds);
                break;
            }
            if (elapsed % 5 == 0 && inflightCount.get() > 0) {
                logger.info("Waiting for {} in-flight requests to complete... ({} seconds elapsed)",
                        inflightCount.get(), elapsed);
            }
            Thread.sleep(100);
        }

        logger.info("All in-flight requests completed. Total in-flight peak was controlled.");
    }

    /**
     * Tell http reader that this stream closes by sending a close signal.
     * This method is more lenient with connection failures since the reader
     * may have already closed.
     */
    private void closeStreamReader() {
        Request req = httpClient.preparePost(this.uri)
                .addHeader(HttpHeaderNames.CONTENT_TYPE, "application/x-protobuf")
                .addHeader(HttpHeaderNames.CONTENT_LENGTH, "0")
                .addHeader(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE)
                .build();

        try {
            // Try to send close signal with a short timeout
            httpClient.executeRequest(req).get(2, TimeUnit.SECONDS);
            logger.info("Successfully sent close signal to HTTP reader");
        } catch (Exception e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;

            // Connection refused is expected if reader closed first - not an error
            if (cause instanceof java.net.ConnectException) {
                logger.debug("Reader already closed (connection refused) - this is normal if reader closes first");
            } else if (cause instanceof TimeoutException) {
                logger.debug("Timeout sending close signal - reader may be busy or already closing");
            } else {
                logger.warn("Could not send close signal to HTTP reader: {}", cause.getMessage());
            }
        }
    }

    private void assertOpen() {
        if (!this.open) {
            throw new IllegalStateException("Closed");
        }
    }
}
