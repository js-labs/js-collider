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
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.concurrent.locks.ReentrantLock;

public class Server
{
    private final Client m_client;
    private final int m_socketBufferSize;
    private final PooledByteBuffer.Pool m_byteBufferPool;

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
                final int position = msg.position();
                final int bytesReady = msg.remaining();
                final int messageLength = msg.getInt();
                assert( bytesReady == messageLength );

                if (++m_messagesReceived == 1)
                {
                    final HashSet<Session> clients = Server.this.m_clients;
                    final int sessions = msg.getInt();

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
                        final PooledByteBuffer buf = m_byteBufferPool.alloc( messageLength );

                        msg.position( position );
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

                    final PooledByteBuffer buf = m_byteBufferPool.alloc( messageLength );

                    msg.position( position );
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
                m_client.start( new InetSocketAddress("localhost", localPort) );
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
        m_byteBufferPool = new PooledByteBuffer.Pool();
        m_lock = new ReentrantLock();
        m_clients = new HashSet<Session>();
    }

    public void run()
    {
        try
        {
            final Collider collider = Collider.create();
            collider.addAcceptor( new TestAcceptor() );
            collider.run();
        }
        catch (IOException ex)
        {
            ex.printStackTrace();
        }

        if (m_client != null)
            m_client.stopAndWait();

        System.out.println( m_byteBufferPool.clear() );
    }
}
