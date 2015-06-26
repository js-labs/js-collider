/*
 * JS-Collider framework tests.
 * Copyright (C) 2015 Sergey Zubarev
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

package org.jsl.tests.echo_latency;

import org.jsl.tests.Util;
import java.io.IOException;
import java.net.Socket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class Client
{
    private InetSocketAddress m_addr;
    private final int m_messages;
    private final ByteBuffer m_msg;
    private final Thread [] m_threads;

    private class SessionThread extends Thread
    {
        public void run()
        {
            try
            {
                final ByteBuffer msg = m_msg.duplicate();
                final ByteBuffer buf = ByteBuffer.allocateDirect( m_msg.capacity() );
                final SocketChannel socketChannel = SocketChannel.open( m_addr );
                final Socket socket = socketChannel.socket();
                socket.setTcpNoDelay( true );
                System.out.println( 
                        "Client socket connected " + socket.getLocalSocketAddress() +
                        " -> " + socket.getRemoteSocketAddress() + "." );

                /* warming up */
                for (int idx=0; idx<100; idx++)
                {
                    socketChannel.write( msg );
                    msg.flip();
                    final int bytesReceived = socketChannel.read( buf );
                    assert( bytesReceived == m_msg.capacity() );
                    buf.clear();
                }

                final long startTime = System.nanoTime();
                for (int c=m_messages; c>0; c--)
                {
                    socketChannel.write( msg );
                    msg.flip();
                    final int bytesReceived = socketChannel.read( buf );
                    assert( bytesReceived == m_msg.capacity() );
                    buf.clear();
                }
                final long endTime = System.nanoTime();
                socketChannel.close();

                final int messages = (m_messages * 2);
                final long tm = ((endTime - startTime) / 1000);
                System.out.println(
                        messages + " messages exchanged at " +
                        Util.formatDelay(startTime, endTime) + " sec (" +
                        (tm / messages) + " usec/msg)" );
            }
            catch (final IOException ex)
            {
                ex.printStackTrace();
            }
        }
    }

    public Client( int sessions, int messages, int messageLength )
    {
        m_messages = messages;
        m_msg = ByteBuffer.allocateDirect( messageLength );
        m_msg.putInt( messageLength );
        m_msg.putInt( sessions );
        for (int i=8; i<messageLength; i++)
            m_msg.put( (byte) i );
        m_msg.flip();

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
            catch (final InterruptedException ex)
            {
                ex.printStackTrace();
            }
        }
    }

    public static void main( String [] args )
    {
        if (args.length != 5)
        {
            System.out.println( "Usage: <server_addr> <server_port> <sessions> <messages> <message_length>" );
            return;
        }

        try
        {
            final InetAddress addr = InetAddress.getByName( args[0] );
            int portNumber = Integer.parseInt( args[1] );
            int sessions = Integer.parseInt( args[2] );
            int messages = Integer.parseInt( args[3] );
            int messageLength = Integer.parseInt( args[4] );

            new Client(sessions, messages, messageLength).start( new InetSocketAddress(addr, portNumber) );
        }
        catch (final UnknownHostException ex)
        {
            ex.printStackTrace();
        }
    }
}
