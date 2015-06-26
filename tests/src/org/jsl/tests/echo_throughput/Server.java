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

import org.jsl.collider.Collider;
import org.jsl.collider.Session;
import org.jsl.collider.StreamDefragger;
import org.jsl.collider.RetainableByteBuffer;
import org.jsl.collider.Acceptor;
import org.jsl.tests.Util;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

public class Server
{
    private final Client m_client;
    private final AtomicInteger m_sessionsDone;

    private class ServerListener implements Session.Listener
    {
        private final Session m_session;
        private final StreamDefragger m_stream;
        private int m_messagesExpected;
        private int m_sessionsExpected;
        private int m_messagesReceived;
        private int m_bytesReceived;
        private long m_startTime;

        public ServerListener( Session session )
        {
            m_session = session;
            m_stream = new StreamDefragger(4)
            {
                protected int validateHeader( ByteBuffer header )
                {
                    final int pos = header.position();
                    return header.getInt( pos );
                }
            };
            System.out.println(
                    session.getLocalAddress() + " -> " + session.getRemoteAddress() +
                    ": connection accepted" );
        }

        public void onDataReceived( RetainableByteBuffer data )
        {
            final int bytesReceived = data.remaining();
            if (bytesReceived == 0)
                throw new AssertionError();
            m_bytesReceived += bytesReceived;

            RetainableByteBuffer msg = m_stream.getNext( data );
            while (msg != null)
            {
                final int pos = msg.position();
                final int bytesReady = msg.remaining();
                final int messageLength = msg.getInt( pos );
                if (messageLength != bytesReady)
                    throw new AssertionError();

                final int sessionsExpected = msg.getInt( pos + (Integer.SIZE/Byte.SIZE) );
                final int messagesExpected = msg.getInt( pos + 2*(Integer.SIZE/Byte.SIZE) );
                if (m_messagesExpected == 0)
                {
                    m_startTime = System.nanoTime();
                    m_sessionsExpected = sessionsExpected;
                    m_messagesExpected = messagesExpected;
                }
                else
                {
                    if ((m_sessionsExpected != sessionsExpected) ||
                        (m_messagesExpected != messagesExpected))
                    {
                        throw new AssertionError();
                    }
                }

                m_messagesReceived++;

                final RetainableByteBuffer reply = msg.slice();
                m_session.sendData( reply );
                reply.release();

                msg = m_stream.getNext();
            }

            if (m_messagesReceived == m_messagesExpected)
            {
                final long endTime = System.nanoTime();
                double tm = (endTime - m_startTime) / 1000;
                tm /= 1000000.0;
                tm = (m_messagesReceived / tm);
                System.out.println( m_session.getRemoteAddress() + ": " +
                        m_messagesReceived + " messages (" +
                        m_bytesReceived + " bytes) processed at " +
                        Util.formatDelay(m_startTime, endTime) + " sec (" +
                        (int)tm + " msgs/sec). " );
            }
        }

        public void onConnectionClosed()
        {
            System.out.println(
                    m_session.getLocalAddress() + " -> " + m_session.getRemoteAddress() +
                    ": connection closed" );
            final int sessionsDone = m_sessionsDone.incrementAndGet();
            if (sessionsDone == m_sessionsExpected)
                m_session.getCollider().stop();
        }
    }

    private class TestAcceptor extends Acceptor
    {
        public TestAcceptor( int socketBufferSize )
        {
            super( new InetSocketAddress(0) );
            socketRecvBufSize = socketBufferSize;
            socketSendBufSize = socketBufferSize;
        }

        public void onAcceptorStarted( Collider collider, int localPort )
        {
            System.out.println( "Server started at port " + localPort );
            if (m_client != null)
                m_client.start( new InetSocketAddress("localhost", localPort) );
        }

        public Session.Listener createSessionListener( Session session )
        {
            return new ServerListener( session );
        }
    }

    public Server( Client client )
    {
        m_client = client;
        m_sessionsDone = new AtomicInteger(0);
    }

    public void run( int socketBufferSize )
    {
        try
        {
            final Collider collider = Collider.create();
            collider.addAcceptor(new TestAcceptor(socketBufferSize));
            collider.run();
            m_client.stopAndWait();
        }
        catch (final IOException ex)
        {
            ex.printStackTrace();
        }
    }
}
