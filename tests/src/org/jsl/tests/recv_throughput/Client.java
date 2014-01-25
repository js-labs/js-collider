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

package org.jsl.tests.recv_throughput;

import org.jsl.tests.Util;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class Client
{
    private InetSocketAddress m_addr;
    private final int m_messages;
    private final int m_messageLength;
    private final int m_socketSendBufferSize;
    private final ByteBuffer m_batch;
    private Thread [] m_threads;

    private class SessionThread extends Thread
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

                final int batchMessages = (m_batch.capacity() / m_messageLength);
                final long startTime = System.nanoTime();
                final ByteBuffer batch = m_batch.duplicate();
                int messagesRemaining = m_messages;

                while (messagesRemaining > batchMessages)
                {
                    socketChannel.write( batch );
                    batch.position(0);
                    messagesRemaining -= batchMessages;
                }
                batch.limit( messagesRemaining * m_messageLength );
                socketChannel.write( batch );
                final long endTime = System.nanoTime();
                socketChannel.close();

                double tm = ((endTime - startTime) / 1000);
                tm /= 1000000;
                tm = (m_messages / tm);
                System.out.println( "Sent " + m_messages + " messages (" +
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

    public Client( int sessions, int messages, int messageLength, int socketSendBufferSize )
    {
        if (messageLength < 12)
            messageLength = 12;

        m_messages = messages;
        m_messageLength = messageLength;
        m_socketSendBufferSize = socketSendBufferSize;

        int batchSize = (messages * messageLength);
        if (batchSize > 1024*1024)
            batchSize = 1024*1024;

        int batchMessages = (batchSize / messageLength);
        batchSize = (batchMessages * messageLength);

        m_batch = ByteBuffer.allocateDirect( batchSize );
        for (int idx=0; idx<batchMessages; idx++)
        {
            m_batch.putInt(messageLength);
            m_batch.putInt( messages );
            m_batch.putInt( sessions );
            for (int cc=12; cc<messageLength; cc++)
                m_batch.put( (byte) cc );
        }
        m_batch.position(0);

        m_threads = new Thread[sessions];
        for (int idx=0; idx<sessions; idx++)
            m_threads[idx] = new SessionThread();
    }

    public void start( InetSocketAddress addr )
    {
        m_addr = addr;
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

    public static void main( String [] args )
    {
        int sessions = 1;
        int messages = 1000000;
        int messageLength = 500;
        int socketSendBufferSize = (64 * 1024);

        if (args.length < 2)
        {
            System.out.println( "Usage: <server_addr> <server_port> [<sessions>] [<messages>] [<message_length>]" );
            return;
        }

        InetSocketAddress addr;
        try
        {
            final int portNumber = Integer.parseInt( args[1] );
            addr = new InetSocketAddress( InetAddress.getByName(args[0]), portNumber );
        }
        catch (UnknownHostException ex)
        {
            ex.printStackTrace();
            return;
        }

        if (args.length <= 2)
        {
            sessions = Integer.parseInt( args[2] );
            if (args.length <= 3)
            {
                messages = Integer.parseInt( args[3] );
                if (args.length <= 4)
                    messageLength = Integer.parseInt( args[4] );
            }
        }

        new Client(sessions, messages, messageLength, socketSendBufferSize).start( addr );
    }
}
