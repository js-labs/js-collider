/*
 * JS-Collider framework.
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

package org.jsl.tests.shmem_throughput;

import org.jsl.collider.*;
import org.jsl.tests.Util;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

public class Server
{
    public static class Acceptor extends org.jsl.collider.Acceptor
    {
        private final int m_sessions;
        private final int m_messages;
        private final int m_messageLength;
        private final AtomicInteger m_sessionsDone;

        public Acceptor( int sessions, int messages, int messageLength )
        {
            m_sessions = sessions;
            m_messages = messages;
            m_messageLength = messageLength;
            m_sessionsDone = new AtomicInteger( sessions );
        }

        public Session.Listener createSessionListener( Session session )
        {
            return new HandshakeListener( session, m_sessionsDone );
        }

        public void onAcceptorStarted( Collider collider, int portNumber )
        {
            System.out.println( "ShMem throughput test server started at port " + portNumber );
            final InetSocketAddress addr = new InetSocketAddress( "localhost", portNumber );
            for (int idx=0; idx<m_sessions; idx++)
                collider.addConnector( new Client.Connector(addr, true, m_messages, m_messageLength) );
        }
    }

    private static class HandshakeListener implements Session.Listener
    {
        private final Session m_session;
        private final AtomicInteger m_sessionsDone;

        public HandshakeListener( Session session, AtomicInteger sessionsDone )
        {
            m_session = session;
            m_sessionsDone = sessionsDone;
            System.out.println( session.getRemoteAddress() + ": connection accepted." );
        }

        public void onDataReceived( RetainableByteBuffer data )
        {
            final int bytesReceived = data.remaining();
            if (bytesReceived == 0)
                throw new AssertionError();

            final int messageLength = data.getInt();
            if (bytesReceived != messageLength)
            {
                System.out.println( m_session.getRemoteAddress() + ": invalid message received." );
                m_session.getCollider().stop();
            }

            ShMemServer shMem = null;
            if (messageLength > 4)
            {
                try { shMem = new ShMemServer( data ); }
                catch (final Exception ex) { ex.printStackTrace(); }
            }

            ByteBuffer reply = ByteBuffer.allocateDirect(4);
            if (shMem == null)
            {
                reply.putInt( 0, 0 );
                m_session.sendData( reply );
            }
            else
            {
                reply.putInt( 0, 1 );
                m_session.accelerate( shMem, reply );
            }

            m_session.replaceListener( new Listener(m_session, m_sessionsDone) );
        }

        public void onConnectionClosed()
        {
            System.out.println( m_session.getRemoteAddress() + ": connection closed unexpectedly." );
        }
    }

    private static class Listener implements Session.Listener
    {
        private final Session m_session;
        private final AtomicInteger m_sessionsDone;
        private final StreamDefragger m_stream;
        private int m_messagesReceived;
        private long m_startTime;

        public Listener( Session session, AtomicInteger sessionsDone )
        {
            m_session = session;
            m_sessionsDone = sessionsDone;

            m_stream = new StreamDefragger(4)
            {
                protected int validateHeader( ByteBuffer header )
                {
                    assert( header.remaining() >= 4 );
                    return header.getInt();
                }
            };
        }

        public void onDataReceived( RetainableByteBuffer data )
        {
            RetainableByteBuffer msg = m_stream.getNext( data );
            while (msg != null)
            {
                final int bytesReady = msg.remaining();
                final int messageLength = msg.getInt();
                final int totalMessages = msg.getInt();
                assert( bytesReady == messageLength );
                assert( msg.getInt() == Client.MSG_MAGIC );
                final int messagesReceived = ++m_messagesReceived;
                if (messagesReceived == 1)
                    m_startTime = System.nanoTime();
                else if (messagesReceived == totalMessages)
                {
                    long endTime = System.nanoTime();
                    System.out.println(
                            m_session.getRemoteAddress() + ": received " + totalMessages +
                            " messages (" + messageLength * totalMessages + " bytes) at " +
                            Util.formatDelay(m_startTime, endTime) + " sec." );
                    m_session.closeConnection();
                }
                msg = m_stream.getNext();
            }
        }

        public void onConnectionClosed()
        {
            System.out.println( m_session.getRemoteAddress() + ": connection closed." );
            int sessionsDone = m_sessionsDone.decrementAndGet();
            assert( sessionsDone >= 0 );
            if (sessionsDone == 0)
                m_session.getCollider().stop();
        }
    }
}
