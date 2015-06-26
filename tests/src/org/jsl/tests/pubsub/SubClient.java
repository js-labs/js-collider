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

import java.io.IOException;
import java.net.InetSocketAddress;

import org.jsl.collider.*;

import java.nio.ByteBuffer;

public class SubClient extends Thread
{
    private final Main m_main;
    private final InetSocketAddress m_addr;
    private final int m_socketBufferSize;

    private class SessionListener implements Session.Listener
    {
        private final Session m_session;
        private final StreamDefragger m_stream;
        private int m_messages;
        private int m_messagesReceived;

        public SessionListener( Session session )
        {
            System.out.println(
                    "SubClient connected " + session.getLocalAddress() +
                    " -> " + session.getRemoteAddress() + "." );

            m_session = session;
            m_stream = new StreamDefragger(4)
            {
                public int validateHeader( ByteBuffer header )
                {
                    return header.getInt();
                }
            };

            final ByteBuffer buf = ByteBuffer.allocateDirect( 5 );
            buf.putInt(5);
            buf.put( (byte) 1 /*subscriber*/);
            buf.position(0);
            session.sendData( buf );
        }

        public void onDataReceived( RetainableByteBuffer data )
        {
            if (data.remaining() == 0)
                throw new AssertionError();

            RetainableByteBuffer msg = m_stream.getNext( data );
            while (msg != null)
            {
                final int messagesReceived = ++m_messagesReceived;
                //System.out.println( "msg [" + messagesReceived + "]\n" + Util.hexDump(msg) );
                if (m_messages == 0)
                {
                    msg.getInt(); // skip message length
                    m_messages = msg.getInt();
                }

                if (messagesReceived == m_messages)
                {
                    m_main.onSubscriberDone();
                    m_session.getCollider().stop();
                }

                msg = m_stream.getNext();
            }
        }

        public void onConnectionClosed()
        {
            System.out.println( "SubClient: connection closed." );
        }
    }

    private class SubConnector extends Connector
    {
        public SubConnector( InetSocketAddress addr )
        {
            super( addr );
            socketRecvBufSize = m_socketBufferSize;
            socketSendBufSize = m_socketBufferSize;
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

    public SubClient( Main main, InetSocketAddress addr, int socketBufferSize )
    {
        m_main = main;
        m_addr = addr;
        m_socketBufferSize = socketBufferSize;
    }

    public void run()
    {
        try
        {
            final Collider collider = Collider.create();
            collider.addConnector( new SubConnector(m_addr) );
            collider.run();
        }
        catch (IOException ex)
        {
            ex.printStackTrace();
        }
    }
}
