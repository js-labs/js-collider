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

package org.jsl.tests.session_throughput;

import org.jsl.tests.StreamDefragger;
import org.jsl.tests.Util;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class Client
{
    private final int m_sessions;
    private final int m_messages;
    private final int m_messageLength;
    private final int m_socketSendBufferSize;
    private final AtomicLong m_startTime;
    private final AtomicInteger m_sessionsDone;
    private final ByteBuffer m_batch;
    private InetSocketAddress m_addr;
    private Thread [] m_threads;

    private class WriterThread extends Thread
    {
        private final SocketChannel m_socketChannel;
        private final Semaphore m_semStart;

        public WriterThread( SocketChannel socketChannel, Semaphore semStart )
        {
            m_socketChannel = socketChannel;
            m_semStart = semStart;
        }

        public void run()
        {
            try
            {
                /* Send one message at the beginning */
                ByteBuffer batch = m_batch.duplicate();
                batch.limit( m_messageLength );
                m_socketChannel.write( batch );

                batch.position( 0 );
                batch.limit( batch.capacity() );

                /* Wait while all clients connected to the server. */
                m_semStart.acquire();

                int messagesRemaining = m_messages;
                final int batchMessages = (batch.capacity() / m_messageLength);
                while (messagesRemaining > batchMessages)
                {
                    m_socketChannel.write( batch );
                    batch.position(0);
                    messagesRemaining -= batchMessages;
                }
                batch.limit( messagesRemaining * m_messageLength);
                m_socketChannel.write( batch );
            }
            catch (IOException ex)
            {
                ex.printStackTrace();
            }
            catch (InterruptedException ex)
            {
                ex.printStackTrace();
            }
        }
    }

    private class ClientThread extends Thread
    {
        public void run()
        {
            try
            {
                final SocketChannel socketChannel = SocketChannel.open( m_addr );
                final Socket socket = socketChannel.socket();
                socket.setTcpNoDelay( true );
                socket.setSendBufferSize( m_socketSendBufferSize );
                
                System.out.println(
                        "Client connected " + socket.getLocalSocketAddress() +
                        " -> " + socket.getRemoteSocketAddress() + "." );

                ByteBuffer buf = ByteBuffer.allocateDirect( 128*1024 );
                StreamDefragger streamDefragger = new StreamDefragger(4)
                {
                    protected int validateHeader( ByteBuffer header )
                    {
                        final int position = header.position();
                        final int messageLength = header.getInt( position );
                        if (messageLength != m_messageLength)
                            throw new AssertionError();
                        return messageLength;
                    }
                };

                final Semaphore semStart = new Semaphore(0);
                WriterThread writerThread = new WriterThread( socketChannel, semStart );
                writerThread.start();

                long startTime = 0;
                int messagesReceived = 0;
                for (;;)
                {
                    socketChannel.read( buf );
                    buf.flip();

                    ByteBuffer msg = streamDefragger.getNext( buf );
                    while (msg != null)
                    {
                        if (++messagesReceived == 1)
                        {
                            startTime = System.nanoTime();
                            m_startTime.compareAndSet( 0, startTime );
                            semStart.release();
                        }
                        msg = streamDefragger.getNext();
                    }
                    buf.clear();

                    if (messagesReceived == (m_sessions*m_messages+1))
                        break;
                }

                final long endTime = System.nanoTime();

                System.out.println(
                        socket.getLocalSocketAddress() + " -> " + socket.getRemoteSocketAddress() +
                        ": " + m_sessions*m_messages + " messages received at " +
                        Util.formatDelay(startTime, endTime) + " sec." );

                final int sessionsDone = m_sessionsDone.incrementAndGet();
                if (sessionsDone == m_sessions)
                {
                    System.out.println(
                            "Test done at " + Util.formatDelay(m_startTime.get(), endTime) + " sec." );
                }

                writerThread.join();
                socketChannel.close();
            }
            catch (final IOException ex)
            {
                ex.printStackTrace();
            }
            catch (final InterruptedException ex)
            {
                ex.printStackTrace();
            }
        }
    }

    public Client( int sessions, int messages, int messageLength, int socketSendBufferSize )
    {
        m_sessions = sessions;
        m_messages = messages;
        m_messageLength = ((messageLength < 8) ? 8 : messageLength);
        m_socketSendBufferSize = socketSendBufferSize;
        m_startTime = new AtomicLong();
        m_sessionsDone = new AtomicInteger();

        int batchSize = (messages * messageLength);
        if (batchSize > 1024*1024)
            batchSize = 1024*1024;

        int batchMessages = (batchSize / messageLength);
        batchSize = (batchMessages * messageLength);

        m_batch = ByteBuffer.allocateDirect( batchSize );
        for (int idx=0; idx<batchMessages; idx++)
        {
            m_batch.putInt( messageLength );
            m_batch.putInt( sessions );
            for (int cc=8; cc<messageLength; cc++)
                m_batch.put( (byte) cc );
        }
        m_batch.position(0);

        m_threads = new Thread[sessions];
        for (int idx=0; idx<sessions; idx++)
            m_threads[idx] = new ClientThread();
    }

    public void start( InetSocketAddress addr )
    {
        m_addr = addr;
        for (Thread thread : m_threads)
            thread.start();
    }

    public int getMessageLength()
    {
        return m_messageLength;
    }

    public void stopAndWait()
    {
        for (Thread thread : m_threads)
        {
            try
            {
                thread.join();
            }
            catch (InterruptedException ex)
            {
                ex.printStackTrace();
            }
        }
    }
}
