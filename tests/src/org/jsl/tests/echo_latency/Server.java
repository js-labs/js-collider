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

import org.jsl.collider.*;

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
        private final StreamDefragger m_streamDefragger;
        private int m_sessionsTotal;
        private int m_messages;

        private void onMessage( RetainableByteBuffer msg )
        {
            final RetainableByteBuffer reply = msg.slice();
            final int pos = msg.position();
            final int messageLength = msg.getInt( pos );

            if (msg.remaining() != messageLength)
                throw new RuntimeException( "invalid message" );

            m_sessionsTotal = msg.getInt( pos+4 );
            m_session.sendData( reply );
            reply.release();
            m_messages++;
        }

        public ServerListener( Session session )
        {
            m_session = session;
            System.out.println(
                    session.getLocalAddress() + " -> " + session.getRemoteAddress() +
                    ": connection accepted" );

            m_streamDefragger = new StreamDefragger(4)
            {
                protected int validateHeader( ByteBuffer header )
                {
                    return header.getInt( header.position() );
                }
            };
        }

        public void onDataReceived( RetainableByteBuffer data )
        {
            final int remaining = data.remaining();
            if (remaining == 0)
                throw new RuntimeException( "zero ByteBuffer" );

            RetainableByteBuffer msg = m_streamDefragger.getNext( data );
            while (msg != null)
            {
                if (msg == StreamDefragger.INVALID_HEADER)
                    throw new RuntimeException( "Invalid packet received." );

                onMessage( msg );
                msg = m_streamDefragger.getNext();
            }
        }

        public void onConnectionClosed()
        {
            m_streamDefragger.close();

            System.out.println(
                    m_session.getLocalAddress() + " -> " + m_session.getRemoteAddress() +
                    ": connection closed (" + m_messages + ")." );

            final int sessionsDone = m_sessionsDone.incrementAndGet();
            if (sessionsDone == m_sessionsTotal)
                m_session.getCollider().stop();
        }
    }

    private class TestAcceptor extends Acceptor
    {
        public TestAcceptor()
        {
            super( new InetSocketAddress(0) );
            //inputQueueBlockSize = 700;
        }

        public void onAcceptorStarted( Collider collider, int localPort )
        {
            System.out.println( "Echo latency server started at port " + localPort );
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
        m_sessionsDone = new AtomicInteger();
    }

    public void run()
    {
        try
        {
            final Collider collider = Collider.create();
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

    public static void main( String [] args )
    {
        new Server(null).run();
    }
}
