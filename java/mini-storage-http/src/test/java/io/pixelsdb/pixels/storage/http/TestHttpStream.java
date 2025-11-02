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
package io.pixelsdb.pixels.storage.http;

import io.pixelsdb.pixels.common.physical.*;
import org.junit.Test;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class TestHttpStream {
    private volatile Exception readerException = null;
    private volatile Exception writerException = null;
    private final int sendLimit = 8 * 1024 * 1024;
    private final int sendNum = 600;

    // Performance metrics
    private static class PerformanceMetrics {
        long totalBytes = 0;
        long startTime = 0;
        long endTime = 0;
        long initialHeapUsed = 0;
        long peakHeapUsed = 0;
        long finalHeapUsed = 0;

        double getThroughputMBps() {
            long durationMs = endTime - startTime;
            if (durationMs == 0)
                return 0;
            return (totalBytes / 1024.0 / 1024.0) / (durationMs / 1000.0);
        }

        long getMemoryIncreaseMB() {
            return (finalHeapUsed - initialHeapUsed) / 1024 / 1024;
        }

        long getPeakMemoryMB() {
            return peakHeapUsed / 1024 / 1024;
        }

        @Override
        public String toString() {
            return String.format(
                    "Performance Metrics:\n" +
                            "  Total Data: %.2f MB\n" +
                            "  Duration: %.2f seconds\n" +
                            "  Throughput: %.2f MB/s\n" +
                            "  Initial Heap: %d MB\n" +
                            "  Peak Heap: %d MB\n" +
                            "  Final Heap: %d MB\n" +
                            "  Memory Increase: %d MB",
                    totalBytes / 1024.0 / 1024.0,
                    (endTime - startTime) / 1000.0,
                    getThroughputMBps(),
                    initialHeapUsed / 1024 / 1024,
                    peakHeapUsed / 1024 / 1024,
                    finalHeapUsed / 1024 / 1024,
                    getMemoryIncreaseMB());
        }
    }

    private static long getHeapUsed() {
        MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
        MemoryUsage heapUsage = memoryMXBean.getHeapMemoryUsage();
        return heapUsage.getUsed();
    }

    @Test
    public void testPhysicalReaderAndWriter() throws IOException {
        System.out.println("\n========================================");
        System.out.println("Starting HTTP Stream Performance Test");
        System.out.println("========================================");
        System.out.println("Configuration:");
        System.out.println("  Chunk size: " + (sendLimit / 1024) + " KB");
        System.out.println("  Number of chunks: " + sendNum);
        System.out.println("  Total data: " + (sendLimit * sendNum / 1024 / 1024) + " MB");
        System.out.println("========================================\n");

        // Force GC before test
        System.gc();
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        final PerformanceMetrics readerMetrics = new PerformanceMetrics();
        final PerformanceMetrics writerMetrics = new PerformanceMetrics();

        Storage httpStream = StorageFactory.Instance().getStorage(Storage.Scheme.httpstream);
        Thread readerThread = new Thread(() -> {
            try {
                readerMetrics.initialHeapUsed = getHeapUsed();
                readerMetrics.startTime = System.currentTimeMillis();

                try (PhysicalReader fsReader = PhysicalReaderUtil.newPhysicalReader(
                        httpStream, "httpstream://localhost:29920")) {
                    int num1 = fsReader.readInt(ByteOrder.BIG_ENDIAN);
                    assert num1 == 13;
                    num1 = fsReader.readInt(ByteOrder.BIG_ENDIAN);
                    assert num1 == 169;

                    long num2 = fsReader.readLong(ByteOrder.BIG_ENDIAN);
                    assert num2 == 28561L;
                    num2 = fsReader.readLong(ByteOrder.BIG_ENDIAN);
                    assert num2 == 815730721L;

                    boolean failed = false;
                    byte[] buffer = new byte[sendLimit];
                    for (int i = 0; i < sendNum; i++) {
                        fsReader.readFully(buffer);
                        readerMetrics.totalBytes += sendLimit;

                        // Track peak memory every 100 chunks
                        if (i % 100 == 0) {
                            long currentHeap = getHeapUsed();
                            if (currentHeap > readerMetrics.peakHeapUsed) {
                                readerMetrics.peakHeapUsed = currentHeap;
                            }
                        }

                        for (int j = 0; j < sendLimit; j++) {
                            byte tmp = buffer[j];
                            if (tmp != (byte) ('a' + j % 10)) {
                                System.out.println("failed sendNum " + i + " sendLen " + sendLimit + " tmp: " + tmp);
                                failed = true;
                            }
                        }
                    }
                    if (failed) {
                        throw new IOException("failed");
                    }
                }

                readerMetrics.endTime = System.currentTimeMillis();
                readerMetrics.finalHeapUsed = getHeapUsed();
                if (readerMetrics.peakHeapUsed == 0) {
                    readerMetrics.peakHeapUsed = readerMetrics.finalHeapUsed;
                }
            } catch (IOException e) {
                readerException = e;
                throw new RuntimeException(e);
            }
        });

        Thread writerThread = new Thread(() -> {
            try {
                writerMetrics.initialHeapUsed = getHeapUsed();
                writerMetrics.startTime = System.currentTimeMillis();

                try (PhysicalWriter fsWriter = PhysicalWriterUtil.newPhysicalWriter(
                        httpStream, "httpstream://localhost:29920")) {
                    ByteBuffer buffer = ByteBuffer.allocate(24);
                    buffer.putInt(13);
                    buffer.putInt(169);
                    buffer.putLong(28561L);
                    buffer.putLong(815730721L);
                    fsWriter.append(buffer);
                    fsWriter.flush();

                    buffer = ByteBuffer.allocate(sendLimit);
                    for (int j = 0; j < sendLimit; j++) {
                        buffer.put((byte) ('a' + j % 10));
                    }

                    for (int i = 0; i < sendNum; i++) {
                        fsWriter.append(buffer.array(), 0, sendLimit);
                        fsWriter.flush();
                        writerMetrics.totalBytes += sendLimit;

                        // Track peak memory every 100 chunks
                        if (i % 100 == 0) {
                            long currentHeap = getHeapUsed();
                            if (currentHeap > writerMetrics.peakHeapUsed) {
                                writerMetrics.peakHeapUsed = currentHeap;
                            }
                        }

                        Thread.sleep(1);
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

                writerMetrics.endTime = System.currentTimeMillis();
                writerMetrics.finalHeapUsed = getHeapUsed();
                if (writerMetrics.peakHeapUsed == 0) {
                    writerMetrics.peakHeapUsed = writerMetrics.finalHeapUsed;
                }
            } catch (IOException e) {
                writerException = e;
                throw new RuntimeException(e);
            }
        });

        readerThread.start();
        writerThread.start();
        try {
            readerThread.join();
            writerThread.join();
            if (this.readerException != null || this.writerException != null) {
                throw new IOException();
            }

            // Print performance metrics
            System.out.println("\n========================================");
            System.out.println("WRITER (OUTPUT) " + writerMetrics);
            System.out.println("\n========================================");
            System.out.println("READER (INPUT) " + readerMetrics);
            System.out.println("\n========================================");
            System.out.println("SUMMARY:");
            System.out.println("  Average Throughput: %.2f MB/s".formatted(
                    (writerMetrics.getThroughputMBps() + readerMetrics.getThroughputMBps()) / 2));
            System.out.println("  Total Memory Used: %d MB".formatted(
                    writerMetrics.getMemoryIncreaseMB() + readerMetrics.getMemoryIncreaseMB()));
            System.out.println("  Data Validation: PASSED");
            System.out.println("========================================\n");

            // Print optimization analysis
            System.out.println("Optimization Analysis:");
            System.out.println("  1. Async Transfer: Throughput %.2f MB/s indicates %s concurrency".formatted(
                    writerMetrics.getThroughputMBps(),
                    writerMetrics.getThroughputMBps() > 100 ? "good" : "limited"));
            System.out.println("  2. Memory Optimization: Peak memory %d MB is %s for %d MB transfer".formatted(
                    writerMetrics.getPeakMemoryMB(),
                    writerMetrics.getPeakMemoryMB() < writerMetrics.totalBytes / 1024 / 1024 / 2 ? "excellent"
                            : "acceptable",
                    (int) (writerMetrics.totalBytes / 1024 / 1024)));
            System.out.println("========================================\n");

        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
