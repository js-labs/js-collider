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
import org.jsl.collider.Session;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;


public class ColliderTest extends Test
{
    private final Collider m_collider;
    private final AtomicInteger m_sessionsDone;
    private Client m_client;

    private class ServerListener implements Session.Listener
    {
        private Session m_session;

        public ServerListener( Session session )
        {
            m_session = session;
            System.out.println( "Connection accepted from " + session.getRemoteAddress() );
        }

        public void onDataReceived( ByteBuffer data )
        {
            m_session.sendData( data );
        }

        public void onConnectionClosed()
        {
            System.out.println( "Connection closed to " + m_session.getRemoteAddress() );
            int sessionsDone = m_sessionsDone.incrementAndGet();
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

        public void onAcceptorStarted( int localPort )
        {
            System.out.println( "Server started at port " + localPort );
            m_client = new Client( localPort, m_sessions, m_messages, m_messageLength );
            m_client.start();
        }

        public Session.Listener createSessionListener( Session session )
        {
            return new ServerListener( session );
        }
    }

    public ColliderTest( int sessions, int messages, int messageLength ) throws IOException
    {
        super( sessions, messages, messageLength );
        m_collider = new Collider();
        m_sessionsDone = new AtomicInteger();
    }

    public void runTest()
    {
        m_collider.addAcceptor( new TestAcceptor() );
        m_collider.run();
        m_client.stopAndWait();
    }
}
