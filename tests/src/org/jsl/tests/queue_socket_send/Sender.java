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

import org.jsl.tests.Util;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

public abstract class Sender
{
    private final String m_name;
    protected final int m_sessions;
    protected final int m_messages;
    protected final int m_messageLength;
    protected final int m_socketBufferSize;

    protected Sender( String name,
                      int sessions,
                      int messages,
                      int messageLength,
                      int socketBufferSize )
    {
        m_name = name;
        m_sessions = sessions;
        m_messages = messages;
        m_messageLength = ((messageLength < 8) ? 8 : messageLength);
        m_socketBufferSize = socketBufferSize;
    }

    protected void run( SessionFactory sessionFactory )
    {
        try
        {
            InetSocketAddress addr = new InetSocketAddress( "localhost", 0 );
            ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
            serverSocketChannel.socket().bind( addr );

            Receiver [] receiver = new Receiver[m_sessions];
            SocketAddress localAddr = serverSocketChannel.socket().getLocalSocketAddress();
            for (int idx=0; idx<m_sessions; idx++)
            {
                receiver[idx] = new Receiver( localAddr, m_socketBufferSize );
                receiver[idx].start();
            }

            Session [] session = new Session[m_sessions];
            for (int idx=0; idx<m_sessions; idx++)
            {
                SocketChannel socketChannel = serverSocketChannel.accept();
                session[idx] = sessionFactory.createSession( socketChannel );
            }

            ByteBuffer msg = ByteBuffer.allocateDirect( m_messageLength );
            msg.putInt( m_messageLength );
            msg.putInt( m_messages );
            for (int idx=0; idx<m_messageLength-8; idx++)
                msg.put( (byte) idx );
            msg.position(0);

            long startTime = System.nanoTime();
            for (int idx=0; idx<m_messages; idx++)
            {
                for (Session s : session)
                {
                    s.sendData( msg );
                    msg.position( 0 );
                }
            }
            long endTime = System.nanoTime();

            System.out.println(
                    m_name + ": sent " + m_messages*m_sessions + " messages (" + m_sessions +
                    " sessions) at " + Util.formatDelay(startTime, endTime) + " sec." );

            /*
            for (Session s : session)
                s.close();
            for (Receiver r : receiver)
                r.join()
            */
        }
        catch (IOException ex)
        {
            ex.printStackTrace();
        }
        /*
        catch (InterruptedException ex)
        {
            ex.printStackTrace();
        }
        */
    }

    public abstract void run();
}
