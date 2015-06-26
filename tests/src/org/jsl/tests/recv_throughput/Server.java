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

import org.jsl.collider.*;
import org.jsl.tests.Util;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

public class Server
{
    private final Client m_client;
    private final int m_socketRecvBufSize;
    private final AtomicInteger m_sessionsDone;

    private class ServerListener implements Session.Listener
    {
        private final Session m_session;
        private final StreamDefragger m_stream;
        private int m_callbacks;
        private int m_messagesTotal;
        private int m_messagesReceived;
        private int m_bytesTotal;
        private int m_sessionsTotal;
        private long m_startTime;

        public ServerListener( Session session )
        {
            m_session = session;
            m_stream = new StreamDefragger(4)
            {
                protected int validateHeader( ByteBuffer header )
                {
                    return header.getInt( header.position() );
                }
            };
            System.out.println(
                    session.getLocalAddress() + " -> " + session.getRemoteAddress() +
                    ": connection accepted" );
        }

        public void onDataReceived( RetainableByteBuffer data )
        {
            if (data.remaining() == 0)
                throw new AssertionError();

            m_bytesTotal += data.remaining();
            m_callbacks++;
            RetainableByteBuffer msg = m_stream.getNext( data );
            while (msg != null)
            {
                if (m_messagesTotal == 0)
                {
                    m_startTime = System.nanoTime();
                    msg.getInt(); // skip length
                    m_messagesTotal = msg.getInt();
                    m_sessionsTotal = msg.getInt();
                }
                m_messagesReceived++;
                msg = m_stream.getNext();
            }

            if (m_messagesReceived == m_messagesTotal)
            {
                final long endTime = System.nanoTime();
                double tm = (endTime - m_startTime) / 1000;
                tm /= 1000000.0;
                tm = (m_messagesReceived / tm);
                System.out.println( m_session.getRemoteAddress() + ": received " +
                        m_messagesReceived + " messages (" +
                        m_bytesTotal + " bytes) at " + Util.formatDelay(m_startTime, endTime) +
                        " sec (" + (int)tm + " msgs/sec), " + m_callbacks + " callbacks." );
            }
        }

        public void onConnectionClosed()
        {
            System.out.println(
                    m_session.getLocalAddress() + " -> " + m_session.getRemoteAddress() +
                    ": connection closed" );
            final int sessionsDone = m_sessionsDone.incrementAndGet();
            if (sessionsDone == m_sessionsTotal)
                m_session.getCollider().stop();
        }
    }

    private class TestAcceptor extends Acceptor
    {
        public TestAcceptor()
        {
            socketRecvBufSize = m_socketRecvBufSize;
        }

        public void onAcceptorStarted( Collider collider, int portNumber )
        {
            System.out.println( "Server started at port " + portNumber );
            if (m_client != null)
                m_client.start( new InetSocketAddress("localhost", portNumber) );
        }

        public Session.Listener createSessionListener( Session session )
        {
            return new ServerListener( session );
        }
    }

    public Server( Client client, int socketRecvBufSize )
    {
        m_client = client;
        m_socketRecvBufSize = socketRecvBufSize;
        m_sessionsDone = new AtomicInteger(0);
    }

    public void run()
    {
        try
        {
            final Collider collider = Collider.create();
            collider.addAcceptor( new TestAcceptor() );
            collider.run();
            m_client.stopAndWait();
        }
        catch (final IOException ex)
        {
            ex.printStackTrace();
        }
    }

    public static void main( String [] args )
    {
    }
}
