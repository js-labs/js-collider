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

package org.jsl.tests.pubsub;

import org.jsl.collider.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

public class PubClient extends Thread
{
    private final InetSocketAddress m_addr;
    private final int m_messages;
    private final int m_messageLength;
    private final int m_socketBufferSize;
    private long m_startTime;

    private class SessionListener implements Session.Listener, Runnable
    {
        private final Session m_session;
        private Thread m_thread;

        public SessionListener( Session session )
        {
            m_session = session;

            System.out.println(
                    "PubClient: connected " + session.getLocalAddress() +
                    " -> " + session.getRemoteAddress() );

            final ByteBuffer buf = ByteBuffer.allocateDirect( 5 );
            buf.putInt(5);
            buf.put( (byte) 0 );
            buf.position(0);
            session.sendData( buf );
        }

        public void onDataReceived( RetainableByteBuffer data )
        {
            /* All subscribers are connected to the server,
             * let's start.
             */
            if (data.remaining() == 0)
                throw new AssertionError();

            m_thread = new Thread( this );
            m_thread.start();
        }

        public void onConnectionClosed()
        {
            System.out.println( "PubClient: connection closed." );
            if (m_thread != null)
            {
                try
                {
                    m_thread.join();
                }
                catch (final InterruptedException ex)
                {
                    ex.printStackTrace();
                }
                m_thread = null;
            }
            m_session.getCollider().stop();
        }

        public void run()
        {
            final ByteBuffer buf = ByteBuffer.allocateDirect( m_messageLength );
            buf.putInt( m_messageLength );
            buf.putInt( m_messages );
            for (int idx=8; idx<m_messageLength; idx++)
                buf.put( (byte) idx );
            buf.position(0);

            m_startTime = System.nanoTime();
            for (int idx=0; idx<m_messages; idx++)
                m_session.sendData( buf );
            final long endTime = System.nanoTime();

            //m_session.closeConnection();

            System.out.println(
                    "PubClient: sent " + m_messages + " messages at " +
                    Util.formatDelay(m_startTime, endTime) );
        }
    }

    private class PubConnector extends Connector
    {
        public PubConnector( InetSocketAddress addr )
        {
            super( addr );
            socketSendBufSize = m_socketBufferSize;
            socketRecvBufSize = m_socketBufferSize;
            //joinMessageMaxSize = 1024;
        }

        public Session.Listener createSessionListener( Session session )
        {
            return new SessionListener( session );
        }

        public void onException( IOException ex )
        {
            ex.printStackTrace();
        }
    }

    public PubClient( InetSocketAddress addr, int messages, int messageLength, int socketBufferSize )
    {
        m_addr = addr;
        m_messages = messages;
        m_messageLength = messageLength;
        m_socketBufferSize = socketBufferSize;
    }

    public void run()
    {
        try
        {
            final Collider collider = Collider.create();
            collider.addConnector( new PubConnector( m_addr ) );
            collider.run();
        }
        catch (final IOException ex)
        {
            ex.printStackTrace();
        }
    }

    public long getStartTime()
    {
        return m_startTime;
    }
}
