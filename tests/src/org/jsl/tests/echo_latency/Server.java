package org.jsl.tests.echo_latency;

import org.jsl.collider.Acceptor;
import org.jsl.collider.Collider;
import org.jsl.collider.Session;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

public class Server
{
    private final Client m_client;
    private final AtomicInteger m_sessionsDone;

    private class ServerListener implements Session.Listener
    {
        private Session m_session;
        private ByteBuffer m_msg;
        private int m_sessionsTotal;
        private int m_messages;

        public ServerListener( Session session )
        {
            m_session = session;
            System.out.println( "Connection accepted from " + session.getRemoteAddress() );
        }

        public void onDataReceived( ByteBuffer data )
        {
            /* Client will not send a new message while this message will not be sent,
             * so we can use always the same buffer.
             */
            final int pos = data.position();
            final int messageLength = data.getInt( pos );
            if (m_msg == null)
                m_msg = ByteBuffer.allocateDirect( messageLength );
            else
                m_msg.clear();
            m_msg.put( data );
            m_msg.flip();
            m_sessionsTotal = data.getInt( pos+4 );
            m_session.sendData( m_msg );
            m_messages++;
        }

        public void onConnectionClosed()
        {
            System.out.println(
                    m_session.getRemoteAddress() + ": connection closed (" + m_messages + " messages)." );
            int sessionsDone = m_sessionsDone.incrementAndGet();
            if (sessionsDone == m_sessionsTotal)
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
            System.out.println( "Echo latency server started at port " + localPort );
            if (m_client != null)
            {
                try
                {
                    final InetAddress addr = InetAddress.getByAddress( new byte [] {127, 0, 0, 1} );
                    m_client.start( new InetSocketAddress(addr, localPort) );
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

    public Server( Client client )
    {
        m_client = client;
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
