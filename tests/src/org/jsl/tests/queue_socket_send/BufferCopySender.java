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

import org.jsl.collider.ThreadPool;
import org.jsl.collider.DataBlockCache;
import org.jsl.collider.PerfCounter;
import org.jsl.collider.StatCounter;
import org.jsl.tests.BinaryQueue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicLong;


public class BufferCopySender extends Sender
{
    private static class SessionImpl extends Session
    {
        private final ThreadPool m_threadPool;
        private final BinaryQueue m_outputQueue;
        private final PerfCounter m_perfCounter;
        private final StatCounter m_statCounter;
        private final AtomicLong m_state;
        private final Writer m_writer;
        private final ByteBuffer [] m_iov;

        private class Writer extends ThreadPool.Runnable
        {
            public void runInThreadPool()
            {
                try
                {
                    final long startTime = System.nanoTime();

                    long state = m_state.get();
                    m_outputQueue.getData( m_iov, state );
                    final int pos0 = m_iov[0].position();

                    int iovc = 1;
                    for (; iovc<m_iov.length && m_iov[iovc]!= null; iovc++);

                    final long bytesSent = m_socketChannel.write( m_iov, 0, iovc );

                    for (int idx=0; idx<iovc; idx++)
                        m_iov[idx] = null;

                    m_outputQueue.removeData( pos0, bytesSent );

                    for (;;)
                    {
                        assert( state >= bytesSent );
                        long newState = (state - bytesSent);
                        if (m_state.compareAndSet(state, newState))
                        {
                            if (newState > 0)
                                m_threadPool.execute( this );
                            break;
                        }
                        state = m_state.get();
                    }

                    m_perfCounter.trace( startTime );
                    m_statCounter.trace( bytesSent );
                }
                catch (IOException ex)
                {
                    ex.printStackTrace();
                }
            }
        }

        public SessionImpl( SocketChannel socketChannel,
                            ThreadPool threadPool,
                            DataBlockCache dataBlockCache,
                            PerfCounter perfCounter,
                            StatCounter statCounter )
        {
            super( socketChannel );
            m_threadPool = threadPool;
            m_outputQueue = new BinaryQueue( dataBlockCache );
            m_perfCounter = perfCounter;
            m_statCounter = statCounter;
            m_state = new AtomicLong();
            m_writer = new Writer();
            m_iov = new ByteBuffer[32];
        }

        public void sendData( ByteBuffer data )
        {
            long bytesReady = m_outputQueue.putData( data );
            if (bytesReady > 0)
            {
                for (;;)
                {
                    long state = m_state.get();
                    long newState = (state + bytesReady);
                    if (m_state.compareAndSet(state, newState))
                    {
                        if (state == 0)
                            m_threadPool.execute( m_writer );
                        break;
                    }
                }
            }
        }

        public void close()
        {
            try
            {
                m_socketChannel.close();
            }
            catch (IOException ex)
            {
                ex.printStackTrace();
            }
        }
    }

    private static class SessionImplFactory implements SessionFactory
    {
        private final ThreadPool m_threadPool;
        private final DataBlockCache m_dataBlockCache;
        private final PerfCounter m_perfCounter;
        private final StatCounter m_statCounter;

        public SessionImplFactory(
                    ThreadPool threadPool,
                    DataBlockCache dataBlockCache,
                    PerfCounter perfCounter,
                    StatCounter statCounter )
        {
            m_threadPool = threadPool;
            m_dataBlockCache = dataBlockCache;
            m_perfCounter = perfCounter;
            m_statCounter = statCounter;
        }

        public Session createSession( SocketChannel socketChannel )
        {
            return new SessionImpl(
                    socketChannel, m_threadPool, m_dataBlockCache, m_perfCounter, m_statCounter );
        }
    }

    public BufferCopySender( int sessions, int messages, int messageLength, int socketBufferSize )
    {
        super( "BufferCopy", sessions, messages, messageLength, socketBufferSize );
    }

    public void run()
    {
        ThreadPool threadPool = new ThreadPool( "BCS-TP", m_sessions );
        threadPool.start();

        DataBlockCache dataBlockCache = new DataBlockCache( true, 16*1024, 16, 64 );
        PerfCounter perfCounter = new PerfCounter( "BCS-PC" );
        StatCounter statCounter = new StatCounter( "BCS-SC" );

        run( new SessionImplFactory(threadPool, dataBlockCache, perfCounter, statCounter) );

        try { threadPool.stopAndWait(); }
        catch (InterruptedException ex) { ex.printStackTrace(); }

        System.out.println( perfCounter.getStats() );
        System.out.println( statCounter.getStats() );
    }
}
