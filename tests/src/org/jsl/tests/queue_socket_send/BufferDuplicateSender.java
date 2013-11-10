/*
 * JS-Collider framework tests.
 * Copyright (C) 2013 Sergey Zubarev
 * info@js-labs.org
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.jsl.tests.queue_socket_send;

import org.jsl.collider.PerfCounter;
import org.jsl.collider.ThreadPool;
import org.jsl.collider.StatCounter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicReference;

public class BufferDuplicateSender extends Sender
{
    private static class SessionImpl extends Session
    {
        private static class ListItem
        {
            public volatile ListItem next;
            public final ByteBuffer buf;
            ListItem( ByteBuffer buf ) { this.buf = buf; }
        }

        private class Writer extends ThreadPool.Runnable
        {
            public void runInThreadPool()
            {
                final long startTime = System.nanoTime();

                int iovc = 0;
                for (ListItem item=m_head;;)
                {
                    m_iov[iovc] = item.buf;
                    if (++iovc == m_iov.length)
                        break;
                    item = item.next;
                    if (item == null)
                        break;
                }

                long bytesSent;
                try
                {
                    bytesSent = m_socketChannel.write( m_iov, 0, iovc );
                }
                catch (IOException ex)
                {
                    ex.printStackTrace();
                    bytesSent = 0;
                }

                iovc--;                
                int idx = 0;
                for (; idx<iovc; idx++)
                {
                    m_iov[idx] = null;
                    ListItem next = m_head.next;
                    m_head.next = null;
                    m_head = next;
                }

                m_iov[idx] = null;
                
                ListItem next = m_head.next;
                if (next == null)
                {
                    ListItem head = m_head;
                    m_head = null;
                    if (!m_tail.compareAndSet(head, null))
                    {
                        while (head.next == null);
                        m_head = head.next;
                        head.next = null;
                        m_threadPool.execute( this );
                    }
                }
                else
                {
                    m_head.next = null;
                    m_head = next;
                    m_threadPool.execute( this );
                }

                m_perfCounter.trace( startTime );
                m_statCounter.trace( bytesSent );
            }
        }

        private final ThreadPool m_threadPool;
        private final PerfCounter m_perfCounter;
        private final StatCounter m_statCounter;
        private volatile ListItem m_head;
        private final AtomicReference<ListItem> m_tail;
        private final ByteBuffer [] m_iov;
        private final Writer m_writer;

        public SessionImpl(
                SocketChannel socketChannel,
                ThreadPool threadPool,
                PerfCounter perfCounter,
                StatCounter statCounter )
        {
            super( socketChannel );
            m_threadPool = threadPool;
            m_perfCounter = perfCounter;
            m_statCounter = statCounter;
            m_head = null;
            m_tail = new AtomicReference<ListItem>();
            m_iov = new ByteBuffer[16];
            m_writer = new Writer();
        }

        public void sendData( ByteBuffer data )
        {
            ListItem node = new ListItem( data.duplicate() );
            ListItem tail = m_tail.getAndSet( node );
            if (tail == null)
            {
                m_head = node;
                m_threadPool.execute( m_writer );
            }
            else
                tail.next = node;
        }
    }

    private static class SessionImplFactory implements SessionFactory
    {
        private final ThreadPool m_threadPool;
        private final PerfCounter m_perfCounter;
        private final StatCounter m_statCounter;

        public SessionImplFactory( ThreadPool threadPool, PerfCounter perfCounter, StatCounter statCounter )
        {
            m_threadPool = threadPool;
            m_perfCounter = perfCounter;
            m_statCounter = statCounter;
        }

        public Session createSession( SocketChannel socketChannel )
        {
            return new SessionImpl(
                    socketChannel, m_threadPool, m_perfCounter, m_statCounter );
        }
    }

    public BufferDuplicateSender( int sessions, int messages, int messageLength, int socketBufferSize )
    {
        super( "BufferDuplicate", sessions, messages, messageLength, socketBufferSize );
    }

    public void run()
    {
        PerfCounter perfCounter = new PerfCounter( "BDS-PC" );
        StatCounter statCounter = new StatCounter( "BDS-SC" );
        ThreadPool threadPool = new ThreadPool( "BDS-TP", m_sessions );
        threadPool.start();

        run( new SessionImplFactory(threadPool, perfCounter, statCounter) );

        try { threadPool.stopAndWait(); }
        catch (InterruptedException ex) { ex.printStackTrace(); }

        System.out.println( perfCounter.getStats() );
        System.out.println( statCounter.getStats() );
    }
}
