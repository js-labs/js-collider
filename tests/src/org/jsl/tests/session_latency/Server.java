package org.jsl.tests.session_latency;

import org.jsl.collider.Acceptor;
import org.jsl.collider.Collider;
import org.jsl.collider.Session;

import java.net.InetAddress;
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
        private final ByteBuffer m_byteBuffer;
        private Session m_session2;
        private int m_messages;

        public ServerListener( Session session, int messageLength )
        {
            m_session = session;
            m_byteBuffer = ByteBuffer.allocateDirect( messageLength );

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

        public void onDataReceived( ByteBuffer data )
        {
            final int pos = data.position();
            final int bytesReady = data.remaining();
            final int messageLength = data.getInt();
            assert( bytesReady == messageLength );
            assert( messageLength == m_byteBuffer.capacity() );

            data.position( pos );
            m_byteBuffer.clear();
            m_byteBuffer.put( data );
            m_byteBuffer.flip();
            m_session2.sendData( m_byteBuffer );

            if (++m_messages == Server.this.m_messages)
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

        public void onAcceptorStarted( Collider collider, int localPort )
        {
            System.out.println( "Latency test server started at port " + localPort );
            if (m_client != null)
            {
                final InetSocketAddress addr = new InetSocketAddress( InetAddress.getLoopbackAddress(), localPort );
                m_client.start( addr );
            }
        }

        public Session.Listener createSessionListener( Session session )
        {
            return new ServerListener( session, m_msg.capacity() );
        }
    }

    public Server( int sessions, int messages, int messageLength, Client client )
    {
        m_sessions = sessions;
        m_messages = messages;
        m_msg = ByteBuffer.allocateDirect( messageLength );
        m_msg.putInt( 0, messageLength );
        m_msgStop = ByteBuffer.allocateDirect( 4 );
        m_msgStop.putInt( 0, 4 );

        m_client = client;
        m_lastListener = new AtomicReference<ServerListener>();
        m_sessionsDone = new AtomicInteger();
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
