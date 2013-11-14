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

package org.jsl.tests.echo_throughput;

import org.jsl.tests.Util;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.Semaphore;

public class Client
{
    private final int m_messages;
    private final int m_messageLength;
    private final int m_sendBufferSize;
    private final int m_receiveBufferSize;

    private ByteBuffer m_messageBlock;
    private Thread [] m_threads;
    private InetSocketAddress m_addr;

    private class SenderThread extends Thread
    {
        public void run()
        {
            try
            {
                SocketChannel socketChannel = SocketChannel.open( m_addr );
                Socket socket = socketChannel.socket();
                socket.setTcpNoDelay( true );
                socket.setSendBufferSize( m_sendBufferSize );
                socket.setReceiveBufferSize( m_receiveBufferSize );

                System.out.println( "Client connected " + socket.getRemoteSocketAddress() + "." );

                Semaphore sem = new Semaphore(0);
                ReceiverThread receiverThread = new ReceiverThread( sem, socketChannel );
                receiverThread.start();

                ByteBuffer bb = m_messageBlock.duplicate();
                int messagesRemaining = m_messages;
                int blockMessages = (bb.capacity() / m_messageLength);

                try { sem.acquire(); }
                catch (InterruptedException ex) { ex.printStackTrace(); }

                final long startTime = System.nanoTime();

                while (messagesRemaining > blockMessages)
                {
                    socketChannel.write( bb );
                    bb.position(0);
                    messagesRemaining -= blockMessages;
                }
                bb.limit( messagesRemaining*m_messageLength );
                socketChannel.write(bb);

                try { receiverThread.join(); }
                catch (InterruptedException ex ) { ex.printStackTrace(); }
                socketChannel.close();

                final long endTime = receiverThread.getEndTime();
                double tm = ((endTime - startTime) / 1000);
                tm /= 1000000;
                tm = (m_messages / tm);
                System.out.println( "Processed " + m_messages + " messages (" +
                        m_messages*m_messageLength + " bytes) at " +
                        Util.formatDelay(startTime, endTime) + " sec (" +
                        (int)tm + " msgs/sec)." );
            }
            catch (IOException ex)
            {
                ex.printStackTrace();
            }
        }
    }

    private class ReceiverThread extends Thread
    {
        private final Semaphore m_sem;
        private final SocketChannel m_socketChannel;
        private long m_endTime;

        public ReceiverThread( Semaphore sem, SocketChannel socketChannel )
        {
            m_sem = sem;
            m_socketChannel = socketChannel;
        }

        public void run()
        {
            ByteBuffer bb = ByteBuffer.allocateDirect( (int)(m_receiveBufferSize * 1.2) );
            m_sem.release();

            int bytesRemaining = (m_messages * m_messageLength);
            while (bytesRemaining > 0)
            {
                int bytesReceived;
                try
                {
                    bytesReceived = m_socketChannel.read( bb );
                }
                catch (IOException ex)
                {
                    ex.printStackTrace();
                    break;
                }

                assert( bytesReceived <= bytesRemaining );
                bytesRemaining -= bytesReceived;

                bb.position(0);
            }

            m_endTime = System.nanoTime();
        }

        public final long getEndTime()
        {
            return m_endTime;
        }
    }

    public Client( int sessions,
                   int messages,
                   int messageLength,
                   int sendBufferSize,
                   int receiveBufferSize )
    {
        if (messageLength < 12)
            messageLength = 12;

        m_messages = messages;
        m_messageLength = messageLength;
        m_sendBufferSize = sendBufferSize;
        m_receiveBufferSize = receiveBufferSize;

        int blockSize = (messages * messageLength);
        if (blockSize > 1024*1024)
            blockSize = 1024*1024;

        int blockMessages = (blockSize / messageLength);
        blockSize = (blockMessages * messageLength);

        m_messageBlock = ByteBuffer.allocateDirect( blockSize );
        for (int idx=0; idx<blockMessages; idx++)
        {
            m_messageBlock.putInt( messageLength );
            m_messageBlock.putInt( sessions );
            m_messageBlock.putInt( messages );
            int cc = (messageLength - 12);
            for (; cc>0; cc--)
                m_messageBlock.put( (byte) cc );
        }
        m_messageBlock.position(0);

        m_threads = new SenderThread[sessions];
        for (int idx=0; idx<sessions; idx++)
            m_threads[idx] = new SenderThread();
    }

    public void start( InetAddress addr, int portNumber )
    {
        m_addr = new InetSocketAddress( addr, portNumber );
        for (Thread thread : m_threads)
            thread.start();
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
