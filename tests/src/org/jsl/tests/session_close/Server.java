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

package org.jsl.tests.session_close;

import org.jsl.collider.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

public class Server
{
    private final Client m_client;
    private final AtomicInteger m_testsDone;

    private abstract class TestListener implements Session.Listener
    {
        protected final Session m_session;
        private final StreamDefragger m_stream;

        public TestListener( Session session, StreamDefragger stream )
        {
            m_session = session;
            m_stream = stream;

            RetainableByteBuffer msg = stream.getNext();
            while (msg != null)
            {
                final int rc = onMessageReceived( msg );
                if (rc != 0)
                    break;
                msg = stream.getNext();
            }
        }

        public void onDataReceived( RetainableByteBuffer data )
        {
            if (data.remaining() == 0)
                throw new AssertionError();

            RetainableByteBuffer msg = m_stream.getNext( data );
            while (msg != null)
            {
                final int rc = onMessageReceived( msg );
                if (rc != 0)
                    break;
                msg = m_stream.getNext();
            }
        }

        public void onConnectionClosed()
        {
            if (m_testsDone.incrementAndGet() == 2)
                m_session.getCollider().stop();
        }

        public abstract int onMessageReceived( RetainableByteBuffer msg );
    }

    private class Test1Listener extends TestListener
    {
        private int m_processedMessages;

        public Test1Listener( Session session, StreamDefragger stream )
        {
            super( session, stream );
        }

        public void onConnectionClosed()
        {
            System.out.println(
                    m_session.getLocalAddress() + " -> " + m_session.getRemoteAddress() +
                    " [TEST1]: connection closed, processed " + m_processedMessages + " messages." );
            super.onConnectionClosed();
        }

        public int onMessageReceived( RetainableByteBuffer msg )
        {
            /* Just send message back */
            final ByteBuffer reply = ByteBuffer.allocateDirect( msg.remaining() );
            msg.get(reply);
            reply.flip();
            final int rc = m_session.sendData( reply );
            if (rc >= 0)
                m_processedMessages++;
            return 0;
        }
    }

    private class Test2Listener extends TestListener
    {
        private final int REPLY_MESSAGES = 5000;
        private int m_messages;

        public Test2Listener( Session session, StreamDefragger stream )
        {
            super( session, stream );
        }

        public void onConnectionClosed()
        {
            System.out.println(
                    m_session.getLocalAddress() + " -> " + m_session.getRemoteAddress() +
                    " [TEST2]: connection closed, processed " + m_messages + " messages." );
            super.onConnectionClosed();
        }

        public int onMessageReceived( RetainableByteBuffer msg )
        {
            final int messages = ++m_messages;
            if (messages < REPLY_MESSAGES)
            {
                final ByteBuffer reply = ByteBuffer.allocateDirect( msg.remaining() );
                msg.get( reply );
                reply.flip();
                final int rc = m_session.sendData( reply );
                if (rc < 0)
                    throw new AssertionError();
                return 0;
            }
            else if (messages == REPLY_MESSAGES)
            {
                final int messageLength = msg.remaining();
                assert( messageLength >= 8 );
                final ByteBuffer reply = ByteBuffer.allocateDirect( messageLength );
                msg.get( reply );
                reply.flip();
                reply.putInt(4, -1);

                int rc = m_session.sendData( reply );
                if (rc < 0)
                    throw new AssertionError();

                rc = m_session.closeConnection();
                if (rc != 0)
                    throw new AssertionError();

                rc = m_session.sendData( reply );
                if (rc != -1)
                    throw new AssertionError();

                return -1;
            }
            else
            {
                /* Listener should not receive any messages
                 * after Session.closeConnection() call.
                 */
                throw new AssertionError();
            }
        }
    }

    private class ServerListener implements Session.Listener
    {
        private final Session m_session;
        private final StreamDefragger m_stream;

        public ServerListener( Session session )
        {
            m_session = session;
            m_stream = new StreamDefragger(4)
            {
                protected int validateHeader( ByteBuffer header )
                {
                    return header.getInt();
                }
            };
            System.out.println( "Connection accepted from " + session.getRemoteAddress() );
        }

        public void onDataReceived( RetainableByteBuffer data )
        {
            if (data.remaining() == 0)
                throw new AssertionError();

            RetainableByteBuffer msg = m_stream.getNext( data );

            msg.getInt(); /* skip message length */
            final int testType = msg.getInt();
            switch (testType)
            {
                case 1:
                    m_session.replaceListener( new Test1Listener(m_session, m_stream) );
                break;

                case 2:
                    m_session.replaceListener( new Test2Listener(m_session, m_stream) );
                break;

                default:
                    throw new AssertionError();
            }
        }

        public void onConnectionClosed()
        {
            /* Should never be called in the test. */
            throw new AssertionError();
        }
    }

    private class TestAcceptor extends Acceptor
    {
        public TestAcceptor( int socketBufferSize )
        {
            tcpNoDelay = true;
            socketRecvBufSize = socketBufferSize;
            socketSendBufSize = socketBufferSize;
        }

        public void onAcceptorStarted( Collider collider, int portNumber )
        {
            System.out.println( "Server started at port " + portNumber );
            if (m_client != null)
                m_client.start( new InetSocketAddress( "localhost", portNumber ) );
        }

        public Session.Listener createSessionListener( Session session )
        {
            return new ServerListener( session );
        }
    }

    public Server( Client client )
    {
        m_client = client;
        m_testsDone = new AtomicInteger();
    }

    public void run( int socketBufferSize )
    {
        try
        {
            final Collider collider = Collider.create();
            collider.addAcceptor( new TestAcceptor(socketBufferSize) );
            collider.run();
        }
        catch (final IOException ex)
        {
            ex.printStackTrace();
        }
    }
}
