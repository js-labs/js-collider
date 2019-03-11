/*
 * JS-Collider framework test.
 * Copyright (C) 2013 Sergey Zubarev
 * info@js-labs.org
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.jsl.tests.byte_buffer_pool;

import org.jsl.collider.RetainableByteBuffer;
import org.jsl.collider.RetainableByteBufferPool;
import org.jsl.tests.Util;

import java.nio.ByteOrder;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.logging.Logger;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;

public class Main
{
    private static final Logger s_logger = Logger.getLogger(Main.class.getName());

    private static final AtomicReferenceFieldUpdater<ReleaseThread, ReleaseThread.Node>
        s_tailUpdater = AtomicReferenceFieldUpdater.newUpdater(ReleaseThread.class, ReleaseThread.Node.class, "m_tail");

    private final int m_ops;
    private final int m_threads;
    private final int m_poolChunkSize;
    private final int m_poolCacheMaxSize;
    private final int m_poolCacheInitialSize;
    private final Semaphore m_sema;
    private ReleaseThread m_releaseThread;
    private volatile boolean m_start;

    private class AllocThread extends Thread
    {
        private final RetainableByteBufferPool m_pool;
        private final int m_ops;

        AllocThread(RetainableByteBufferPool pool, int ops)
        {
            m_pool = pool;
            m_ops = ops;
        }

        public void run()
        {
            final Random rand = new Random();

            m_sema.release();
            while (!m_start);

            final long startTime = System.nanoTime();
            for (int idx=0; idx<m_ops; idx++)
            {
                int allocSize;
                do { allocSize = rand.nextInt(256); }
                while (allocSize < 4);

                final RetainableByteBuffer buf = m_pool.alloc(allocSize);
                buf.putInt(0, allocSize);
                m_releaseThread.put(buf);
            }
            final long endTime = System.nanoTime();
            System.out.println(
                    "Allocated " + m_ops + " buffers at " +
                    Util.formatDelay(startTime, endTime) + " sec." );

            m_releaseThread.put(null);
        }
    }

    private static class ReleaseThread extends Thread
    {
        static class Node
        {
            volatile Node next;
            final RetainableByteBuffer buf;
            Node(RetainableByteBuffer buf)
            {
                this.buf = buf;
            }
        }

        private final Semaphore m_sema;
        private Node m_head;
        public volatile Node m_tail;
        private int m_stop;

        ReleaseThread(int stop)
        {
            m_sema = new Semaphore(0);
            m_stop = stop;
        }

        public void put(RetainableByteBuffer buf)
        {
            final Node node = new Node(buf);
            final Node tail = s_tailUpdater.getAndSet(this, node);
            if (tail == null)
            {
                m_head = node;
                m_sema.release();
            }
            else
                tail.next = node;
        }

        public void run()
        {
            int releasedBuffers = 0;
            loop: for (;;)
            {
                try { m_sema.acquire(); }
                catch (final InterruptedException ex) { ex.printStackTrace(); }

                for (;;)
                {
                    final Node head = m_head;
                    if (head.buf == null)
                    {
                        if (--m_stop == 0)
                            break loop;
                    }
                    else
                    {
                        /* simple consistency validation */
                        final int size = head.buf.getInt();
                        if (size != head.buf.capacity())
                            throw new RuntimeException("Invalid buffer: " + size + " != " + head.buf.capacity());
                        head.buf.release();
                        releasedBuffers++;
                    }

                    if (head.next == null)
                    {
                        m_head = null;
                        if (s_tailUpdater.compareAndSet(this, head, null))
                            break;
                        while (head.next == null);
                    }
                    m_head = head.next;
                    head.next = null;
                }
            }
            System.out.println("Released " + releasedBuffers + " buffers");
        }
    }

    private Main(int ops, int threads, int poolChunkSize, int poolCacheMaxSize, int poolCacheInitialSize)
    {
        m_ops = ops;
        m_threads = threads;
        m_poolChunkSize = poolChunkSize;
        m_poolCacheMaxSize = poolCacheMaxSize;
        m_poolCacheInitialSize = poolCacheInitialSize;
        m_sema = new Semaphore(0);
    }

    private void run()
    {
        final Thread [] thread = new Thread[m_threads];

        m_releaseThread = new ReleaseThread(thread.length);
        m_releaseThread.start();

        final RetainableByteBufferPool pool = new RetainableByteBufferPool(m_poolChunkSize,
                true, ByteOrder.nativeOrder(), m_poolCacheMaxSize, m_poolCacheInitialSize);

        for (int idx=0; idx<thread.length; idx++)
        {
            thread[idx] = new AllocThread(pool, m_ops);
            thread[idx].start();
        }

        m_sema.acquireUninterruptibly(thread.length);
        m_start = true;

        try
        {
            for (Thread t : thread)
                t.join();
            m_releaseThread.join();
        }
        catch (final InterruptedException ex)
        {
            ex.printStackTrace();
        }

        pool.release(s_logger);
    }

    public static void main(String [] args)
    {
        final int OPS = 100000;
        final int ALLOC_THREADS = 4;
        final int POOL_CHUNK_SIZE = 32*1024;
        final int POOL_CACHE_MAX_SIZE = 32;
        final int POOL_CACHE_INITIAL_SIZE = 16;
        new Main(OPS, ALLOC_THREADS, POOL_CHUNK_SIZE, POOL_CACHE_MAX_SIZE, POOL_CACHE_INITIAL_SIZE).run();
    }
}
