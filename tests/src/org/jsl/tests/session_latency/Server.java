package org.jsl.tests.session_latency;

import org.jsl.collider.Acceptor;
import org.jsl.collider.Collider;
import org.jsl.collider.Session;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

public class Server
{
    private final Client m_client;
    private final AtomicInteger m_sessionsTotal;
    private final AtomicInteger m_sessionsDone;

    private class ServerListener implements Session.Listener
    {
        private Session m_session;
        private int m_messages;

        public ServerListener( Session session )
        {
            m_session = session;
            System.out.println( "Connection accepted from " + session.getRemoteAddress() );
        }

        public void onDataReceived( ByteBuffer data )
        {
            if (m_messages++ == 0)
            {
                int pos = data.position();
                data.getInt(); /* skip message length */
                int sessionsTotal = data.getInt();
                m_sessionsTotal.compareAndSet( 0, sessionsTotal );
                data.position( pos );
            }
            m_session.sendData( data );
        }

        public void onConnectionClosed()
        {
            System.out.println( "Connection closed to " + m_session.getRemoteAddress() );
            int sessionsDone = m_sessionsDone.incrementAndGet();
            if (sessionsDone == m_sessionsTotal.get())
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
            System.out.println( "Latency test server started at port " + localPort );
            if (m_client != null)
                m_client.start( localPort );
        }

        public Session.Listener createSessionListener( Session session )
        {
            return new ServerListener( session );
        }
    }

    public Server( Client client )
    {
        m_client = client;
        m_sessionsTotal = new AtomicInteger();
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

    public static void main( String [] args )
    {
        new Server(null).run();
    }
}
