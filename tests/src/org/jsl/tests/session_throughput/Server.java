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

package org.jsl.tests.session_throughput;

import org.jsl.collider.*;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.concurrent.locks.ReentrantLock;

public class Server
{
    private final Client m_client;
    private final int m_socketBufferSize;
    private final CachedByteBuffer.Cache m_byteBufferCache;

    private final ReentrantLock m_lock;
    private final HashSet<Session> m_clients;

    private class ServerListener implements Session.Listener
    {
        private final Session m_session;
        private final StreamDefragger m_streamDefragger;
        private int m_messagesReceived;
        private HashSet<Session> m_clients;

        public ServerListener( Session session )
        {
            m_session = session;
            m_streamDefragger = new StreamDefragger(4)
            {
                protected int validateHeader( ByteBuffer header )
                {
                    return header.getInt();
                }
            };
        }

        public void onDataReceived( ByteBuffer data )
        {
            ByteBuffer msg = m_streamDefragger.getNext( data );
            while (msg != null)
            {
                if (++m_messagesReceived == 1)
                {
                    final HashSet<Session> clients = Server.this.m_clients;
                    final int sessions = msg.getInt(4);
                    m_lock.lock();
                    try
                    {
                        clients.add( m_session );
                        if (clients.size() == sessions)
                            m_clients = new HashSet<Session>( clients );
                    }
                    finally
                    {
                        m_lock.unlock();
                    }

                    if (m_clients != null)
                    {
                        System.out.println( "All clients connected, starting test." );
                        CachedByteBuffer buf = m_byteBufferCache.get();
                        buf.put( msg );
                        buf.flip();

                        for (Session session : m_clients)
                            session.sendData( buf );
                        buf.release();
                    }
                }
                else
                {
                    if (m_clients == null)
                    {
                        m_lock.lock();
                        try
                        {
                            m_clients = new HashSet<Session>( Server.this.m_clients );
                        }
                        finally
                        {
                            m_lock.unlock();
                        }
                    }

                    CachedByteBuffer buf = m_byteBufferCache.get();
                    buf.put( msg );
                    buf.flip();

                    for (Session session : m_clients)
                        session.sendData( buf );

                    buf.release();
                }
                msg = m_streamDefragger.getNext();
            }
        }

        public void onConnectionClosed()
        {
            final HashSet<Session> clients = Server.this.m_clients;
            m_lock.lock();
            try
            {
                clients.remove( m_session );
                if (clients.size() == 0)
                    m_session.getCollider().stop();
            }
            finally
            {
                m_lock.unlock();
            }
        }
    }

    private class TestAcceptor extends Acceptor
    {
        public TestAcceptor()
        {
            super( new InetSocketAddress(0) );
            tcpNoDelay = true;
            socketRecvBufSize = Server.this.m_socketBufferSize;
            socketSendBufSize = Server.this.m_socketBufferSize;
        }

        public void onAcceptorStarted( Collider collider, int localPort )
        {
            System.out.println( "Session throughput server started at port " + localPort );
            if (m_client != null)
            {
                try
                {
                    final InetAddress inetAddr = InetAddress.getByAddress( new byte [] {127, 0, 0, 1} );
                    m_client.start( new InetSocketAddress(inetAddr, localPort) );
                }
                catch (UnknownHostException ex)
                {
                    ex.printStackTrace();
                }
            }
        }

        public Session.Listener createSessionListener( Session session )
        {
            return new ServerListener( session );
        }
    }

    public Server( Client client, int socketBufferSize )
    {
        m_client = client;
        m_socketBufferSize = socketBufferSize;
        m_byteBufferCache = new CachedByteBuffer.Cache( true, client.getMessageLength() );
        m_lock = new ReentrantLock();
        m_clients = new HashSet<Session>();
    }

    public void run()
    {
        Collider collider = Collider.create();
        collider.addAcceptor( new TestAcceptor() );
        collider.run();

        if (m_client != null)
            m_client.stopAndWait();
    }
}
