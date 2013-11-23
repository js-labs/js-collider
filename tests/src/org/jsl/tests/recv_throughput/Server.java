package org.jsl.tests.session_throughput;

import org.jsl.collider.Acceptor;
import org.jsl.collider.Collider;
import org.jsl.collider.Session;
import org.jsl.collider.StreamDefragger;
import org.jsl.tests.Util;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

public class Server
{
    private final Client m_client;
    private final int m_socketRecvBufSize;
    private final AtomicInteger m_sessionsDone;

    private class ServerListener implements Session.Listener
    {
        private final Session m_session;
        private final StreamDefragger m_stream;
        private int m_callbacks;
        private int m_messagesTotal;
        private int m_messagesReceived;
        private int m_bytesTotal;
        private int m_sessionsTotal;
        private long m_startTime;

        public ServerListener( Session session )
        {
            m_session = session;
            m_stream = new StreamDefragger(4)
            {
                protected int validateHeader(ByteBuffer header)
                {
                    return header.getInt();
                }
            };
            System.out.println( "Connection accepted from " + session.getRemoteAddress() );
        }

        public void onDataReceived( ByteBuffer data )
        {
            m_bytesTotal += data.remaining();
            m_callbacks++;
            ByteBuffer msg = m_stream.getNext( data );
            while (msg != null)
            {
                if (m_messagesTotal == 0)
                {
                    m_startTime = System.nanoTime();
                    data.getInt(); // skip length
                    m_messagesTotal = data.getInt();
                    m_sessionsTotal = data.getInt();
                }
                m_messagesReceived++;
                msg = m_stream.getNext();
            }

            if (m_messagesReceived == m_messagesTotal)
            {
                long endTime = System.nanoTime();
                double tm = (endTime - m_startTime) / 1000;
                tm /= 1000000.0;
                tm = (m_messagesReceived / tm);
                System.out.println( m_session.getRemoteAddress() + ": " +
                        m_messagesReceived + " messages (" +
                        m_bytesTotal + " bytes) received at " +
                        Util.formatDelay(m_startTime, endTime) + " sec (" +
                        (int)tm + " msgs/sec), " + m_callbacks + " callbacks." );
            }
        }

        public void onConnectionClosed()
        {
            System.out.println( "Connection closed to " + m_session.getRemoteAddress() );
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
            socketRecvBufSize = m_socketRecvBufSize;
        }

        public void onAcceptorStarted( int localPort )
        {
            System.out.println( "Server started at port " + localPort );
            if (m_client != null)
            {
                try
                {
                    final byte byteAddr[] = { 127, 0, 0, 1 };
                    InetAddress addr = InetAddress.getByAddress( byteAddr );
                    m_client.start( InetAddress.getByAddress(byteAddr), localPort );
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

    public Server( Client client, int socketRecvBufeSize )
    {
        m_client = client;
        m_socketRecvBufSize = socketRecvBufeSize;
        m_sessionsDone = new AtomicInteger(0);
    }

    public void run()
    {
        Collider collider = Collider.create();
        collider.addAcceptor( new TestAcceptor() );
        collider.run();
        m_client.stopAndWait();
    }

    public static void main( String [] args )
    {
    }
}
