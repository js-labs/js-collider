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
import java.util.logging.Logger;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;

public class Main
{
    private static final Logger s_logger = Logger.getLogger( Main.class.getName() );
    private static final int OPS = 1000000;

    private final Semaphore m_sema;
    private final RetainableByteBufferPool m_pool;
    private ReleaseThread m_releaseThread;
    private volatile boolean m_start;

    private class AllocThread extends Thread
    {
        private final int m_allocSize;

        public AllocThread( int allocSize )
        {
            m_allocSize = allocSize;
        }

        public void run()
        {
            m_sema.release();
            while (!m_start);

            final long startTime = System.nanoTime();
            for (int idx=0; idx<OPS; idx++)
            {
                final RetainableByteBuffer buf = m_pool.alloc( m_allocSize );
                buf.putInt( 0, m_allocSize );
                m_releaseThread.put( buf );
            }
            final long endTime = System.nanoTime();
            System.out.println(
                    "Allocated " + OPS + " buffers (" + m_allocSize + " bytes each) at " +
                    Util.formatDelay(startTime, endTime) + " sec." );

            m_releaseThread.put( null );
        }
    }

    private static class ReleaseThread extends Thread
    {
        private static class Node
        {
            public volatile Node next;
            final RetainableByteBuffer buf;

            public Node( RetainableByteBuffer buf )
            {
                this.buf = buf;
            }
        }

        private final Semaphore m_sema;
        private Node m_head;
        private final AtomicReference<Node> m_tail;
        private int m_stop;

        public ReleaseThread( int stop )
        {
            m_sema = new Semaphore( 0 );
            m_tail = new AtomicReference<Node>();
            m_stop = stop;
        }

        public void put( RetainableByteBuffer buf )
        {
            final Node node = new Node( buf );
            final Node tail = m_tail.getAndSet( node );
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
                    if (m_head.buf == null)
                    {
                        if (--m_stop == 0)
                            break loop;
                    }
                    else
                    {
                        /* simple consistency validation */
                        final int length = m_head.buf.getInt( 0 );
                        if (length != m_head.buf.capacity())
                            throw new RuntimeException( "Invalid buffer: " + length + " != " + m_head.buf.capacity() );
                        m_head.buf.release();
                        releasedBuffers++;
                    }

                    final Node head = m_head;
                    if (head.next == null)
                    {
                        m_head = null;
                        if (m_tail.compareAndSet(head, null))
                            break;
                        while (head.next == null);
                    }
                    m_head = head.next;
                    head.next = null;
                }
            }
            System.out.println( "Released " + releasedBuffers + " buffers." );
        }
    }

    private Main()
    {
        m_sema = new Semaphore(0);
        m_pool = new RetainableByteBufferPool( 64*1024 );
    }

    private void run()
    {
        final Thread [] thread = new Thread[3];

        m_releaseThread = new ReleaseThread( thread.length );
        m_releaseThread.start();

        for (int idx=0; idx<thread.length; idx++)
        {
            thread[idx] = new AllocThread(4+idx);
            thread[idx].start();
        }

        m_sema.acquireUninterruptibly( thread.length );
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

        System.out.println( s_logger );
    }

    public static void main( String [] args )
    {
        new Main().run();
    }
}
