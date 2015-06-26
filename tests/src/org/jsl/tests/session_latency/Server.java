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

import org.jsl.collider.Acceptor;
import org.jsl.collider.Collider;
import org.jsl.collider.RetainableByteBuffer;
import org.jsl.collider.Session;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class Server
{
    private final int m_sessions;
    private final int m_messages;
    private final ByteBuffer m_msg;
    private final ByteBuffer m_msgStop;
    private final Client m_client;
    private final AtomicReference<ServerListener> m_lastListener;
    private final AtomicInteger m_sessionsDone;

    private class ServerListener implements Session.Listener
    {
        private final Session m_session;
        private Session m_session2;
        private int m_messages;

        public ServerListener( Session session )
        {
            m_session = session;

            for (;;)
            {
                final ServerListener lastListener = m_lastListener.get();
                if (lastListener == null)
                {
                    if (m_lastListener.compareAndSet(null, this))
                        break;
                }
                else
                {
                    if (m_lastListener.compareAndSet(lastListener, null))
                    {
                        System.out.println(
                                session.getRemoteAddress() + " <-> " + lastListener.m_session.getRemoteAddress() );
                        lastListener.m_session2 = session;
                        m_session2 = lastListener.m_session;
                        m_session.sendData( m_msg );
                        break;
                    }
                }
            }
        }

        public void onDataReceived( RetainableByteBuffer data )
        {
            final int pos = data.position();
            final int bytesReceived = data.remaining();
            final int messageLength = data.getInt( pos );
            if (bytesReceived != messageLength)
                throw new AssertionError();

            if (++m_messages < Server.this.m_messages)
            {
                final RetainableByteBuffer reply = data.slice();
                m_session2.sendData( reply );
                reply.release();
            }
            else
            {
                m_session.sendData( m_msgStop );
                m_session.closeConnection();
                m_session2.sendData( m_msgStop );
                m_session2.closeConnection();
            }
        }

        public void onConnectionClosed()
        {
            System.out.println( "Connection closed to " + m_session.getRemoteAddress() );
            final int sessionsDone = m_sessionsDone.incrementAndGet();
            if (sessionsDone == m_sessions)
                m_session.getCollider().stop();
        }
    }

    private class TestAcceptor extends Acceptor
    {
        public TestAcceptor()
        {
            super( new InetSocketAddress(0) );
            tcpNoDelay = true;
        }

        public void onAcceptorStarted( Collider collider, int localPort )
        {
            System.out.println( "Latency test server started at port " + localPort );
            if (m_client != null)
                m_client.start( new InetSocketAddress("localhost", localPort) );
        }

        public Session.Listener createSessionListener( Session session )
        {
            return new ServerListener( session );
        }
    }

    public Server( int sessions, int messages, int messageLength, Client client )
    {
        m_sessions = sessions;
        m_messages = messages;
        m_msg = ByteBuffer.allocateDirect( messageLength );
        m_msg.putInt( 0, messageLength );
        for (int idx=4; idx<messageLength; idx++)
            m_msg.put( idx, (byte) idx );
        m_msgStop = ByteBuffer.allocateDirect( 4 );
        m_msgStop.putInt( 0, 4 );

        m_client = client;
        m_lastListener = new AtomicReference<ServerListener>();
        m_sessionsDone = new AtomicInteger();
    }

    public void run()
    {
        try
        {
            /* Would be better to avoid message fragmentation. */
            final Collider.Config config = new Collider.Config();
            if (m_msg.capacity() > config.inputQueueBlockSize)
                config.inputQueueBlockSize = m_msg.capacity();

            final Collider collider = Collider.create( config );
            collider.addAcceptor( new TestAcceptor() );
            collider.run();
        }
        catch (final IOException ex)
        {
            ex.printStackTrace();
        }

        if (m_client != null)
            m_client.stopAndWait();
    }
}
