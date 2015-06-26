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

package org.jsl.tests.session_latency;

import org.jsl.collider.StatCounter;
import org.jsl.tests.Util;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class Client
{
    private InetSocketAddress m_addr;
    private Thread [] m_threads;

    private class ClientThread extends Thread
    {
        public void run()
        {
            try
            {
                final SocketChannel socketChannel = SocketChannel.open( m_addr );
                final Socket socket = socketChannel.socket();
                socket.setTcpNoDelay( true );
                System.out.println(
                        "Client connected " + socket.getLocalSocketAddress() +
                        " -> " + socket.getRemoteSocketAddress() + "." );

                final StatCounter statCounter = new StatCounter( socket.getLocalSocketAddress() + " latency" );
                final ByteBuffer bb = ByteBuffer.allocateDirect( 1024*16 );
                int bytesReceived = socketChannel.read( bb );
                long recvTime = System.nanoTime();
                int messages = 1;
                bb.flip();
                int messageLength = bb.getInt(0);
                assert( bytesReceived == messageLength );

                for (;;)
                {
                    final long sendTime = bb.getLong(4);

                    /* First message we receive is a server start message */
                    if (messages > 10)
                        statCounter.trace( (recvTime - sendTime) / 1000 );

                    bb.putLong( 4, System.nanoTime() );
                    final int bytesSent = socketChannel.write( bb );
                    assert( bytesSent == messageLength );

                    bb.clear();
                    bytesReceived = socketChannel.read( bb );
                    recvTime = System.nanoTime();
                    messages++;

                    if (bytesReceived == 4)
                        break;

                    bb.flip();
                    messageLength = bb.getInt(0);
                    assert( messageLength == bytesReceived );
                }
                socketChannel.close();

                System.out.println( statCounter.getStats() );
            }
            catch (final IOException ex)
            {
                ex.printStackTrace();
            }
        }
    }

    public Client( int sessions )
    {
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

    public void stopAndWait()
    {
        for (Thread thread : m_threads)
        {
            try
            {
                thread.join();
            }
            catch (final InterruptedException ex)
            {
                ex.printStackTrace();
            }
        }
    }
}
