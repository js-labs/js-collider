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

package org.jsl.tests.send_throughput;

import org.jsl.tests.StreamDefragger;
import org.jsl.tests.Util;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class Client
{
    private final SessionThread [] m_session;
    private final int m_messages;
    private final int m_messageLength;
    private final int m_socketBufferSize;
    private final ByteBuffer m_startRequest;
    private SocketAddress m_addr;

    private class SessionThread extends Thread
    {
        public void run()
        {
            try
            {
                final SocketChannel socketChannel = SocketChannel.open();
                socketChannel.socket().setReceiveBufferSize(m_socketBufferSize);

                StreamDefragger streamDefragger = new StreamDefragger(4)
                {
                    protected int validateHeader( ByteBuffer header )
                    {
                        return header.getInt();
                    }
                };

                if (!socketChannel.connect(m_addr) || !socketChannel.finishConnect())
                {
                    System.out.println( "SocketChannel.connect() failed." );
                    return;
                }

                final ByteBuffer startRequest = m_startRequest.duplicate();
                final int bytesSent = socketChannel.write(startRequest);
                if (bytesSent != startRequest.capacity())
                {
                    System.out.println( "SocketChannel.send() failed." );
                    return;
                }

                final ByteBuffer bb = ByteBuffer.allocateDirect(1024*128);
                int messages = 0;
                int bytesReceivedTotal = 0;
                int bytesReceived = socketChannel.read(bb);
                if (bytesReceived > 0)
                {
                    final long startTime = System.nanoTime();
                    for (;;)
                    {
                        bytesReceivedTotal += bytesReceived;

                        bb.position(0);
                        bb.limit(bytesReceived);
                        ByteBuffer msg = streamDefragger.getNext(bb);
                        while (msg != null)
                        {
                            final int messageLength = msg.getInt();
                            assert(messageLength == m_messageLength);
                            messages++;
                            msg = streamDefragger.getNext();
                        }

                        if (messages == m_messages)
                            break;

                        bb.clear();
                        bytesReceived = socketChannel.read(bb);
                        if (bytesReceived <= 0)
                            break;
                    }
                    long entTime = System.nanoTime();

                    System.out.println(
                            "Received " + messages + " messages (" + bytesReceivedTotal +
                            " bytes) at " + Util.formatDelay(startTime, entTime));
                }
                else
                    System.out.println("Socket read error.");

                socketChannel.close();
            }
            catch (IOException ex)
            {
                ex.printStackTrace();
            }
        }
    }

    public Client(int sessions, int messages, int messageLength, int socketBufferSize)
    {
        m_session = new SessionThread[sessions];
        m_messages = messages;
        m_messageLength = messageLength;
        m_socketBufferSize = socketBufferSize;

        /* length + sessions + messages + message length */
        m_startRequest = ByteBuffer.allocateDirect(4 + 4 + 4 + 4);
        m_startRequest.putInt(16);
        m_startRequest.putInt(sessions);
        m_startRequest.putInt(messages);
        m_startRequest.putInt(messageLength);
        m_startRequest.position(0);
    }

    public void start(SocketAddress addr)
    {
        m_addr = addr;
        for (int idx=0; idx<m_session.length; idx++)
        {
            m_session[idx] = new SessionThread();
            m_session[idx].start();
        }
    }

    public void stopAndWait()
    {
        try
        {
            for (SessionThread session : m_session)
                session.join();
        }
        catch (final InterruptedException ex)
        {
            ex.printStackTrace();
        }
    }

    public static void main(String [] args)
    {
        int port;
        int sessions = 1;
        int messages = 1000000;
        int messageLength = 500;
        int socketBufferSize = (64*1024);

        if (args.length > 0)
        {
            port = Integer.parseInt(args[0]);
            if (args.length > 1)
            {
                sessions = Integer.parseInt(args[1]);
                if (args.length > 2)
                {
                    messages = Integer.parseInt(args[2]);
                    if (args.length > 3)
                    {
                        messageLength = Integer.parseInt(args[3]);
                        if (args.length > 4)
                            socketBufferSize = Integer.parseInt(args[4]);
                    }
                }
            }
        }
        else
        {
            System.out.println("Usage: <port> [sessions] [messages] [message length] [socket buffer size]");
            return;
        }

        final Client client = new Client(sessions, messages, messageLength, socketBufferSize);
        client.start(new InetSocketAddress("localhost", port));
        client.stopAndWait();
    }
}
