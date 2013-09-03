package org.jsl.tests.session_throughput;

import org.jsl.collider.Acceptor;
import org.jsl.collider.Collider;
import org.jsl.collider.Session;
import org.jsl.collider.StreamDefragger;
import org.jsl.tests.Util;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

public class Main
{
    private final int m_sessions;
    private final int m_messages;
    private final int m_messageLength;
    private final AtomicInteger m_sessionsDone;
    private Client m_client;

    private class ServerListener implements Session.Listener
    {
        private class Stream extends StreamDefragger
        {
            public Stream()
            {
                super(4);
            }

            protected int validateHeader(ByteBuffer header)
            {
                return header.getInt();
            }
        }

        private final Session m_session;
        private final Stream m_stream;

        private int m_messagesExpected;
        private int m_messagesReceived;
        private long m_startTime;

        public ServerListener( Session session )
        {
            m_session = session;
            m_stream = new Stream();
            System.out.println( "Connection accepted from " + session.getRemoteAddress() );
        }

        public void onDataReceived( ByteBuffer data )
        {
            ByteBuffer msg = m_stream.getNext( data );
            while (msg != null)
            {
                if (m_messagesExpected == 0)
                {
                    m_startTime = System.nanoTime();
                    data.getInt(); // skip length
                    m_messagesExpected = data.getInt();
                }
                m_messagesReceived++;
                msg = m_stream.getNext();
            }

            if (m_messagesReceived == m_messagesExpected)
            {
                long endTime = System.nanoTime();
                double tm = (endTime - m_startTime) / 1000;
                tm /= 1000000.0;
                tm = (m_messages / tm);
                System.out.println( m_session.getRemoteAddress() + ": " +
                                    m_messages + " messages (" +
                                    m_messages*m_messageLength + " bytes) received at " +
                                    Util.formatDelay(m_startTime, endTime) + " sec (" +
                        (int)tm + " msgs/sec)." );
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

        public void onAcceptorStarted( int localPort )
        {
            System.out.println( "Server started at port " + localPort );
            m_client = new Client( localPort, m_sessions, m_messages, m_messageLength );
            m_client.start();
        }

        public Session.Listener createSessionListener( Session session )
        {
            return new ServerListener( session );
        }
    }

    private Main( int sessions, int messages, int messageLength )
    {
        m_sessions = sessions;
        m_messages = messages;
        m_messageLength = messageLength;
        m_sessionsDone = new AtomicInteger(0);
    }

    private void run()
    {
        try
        {
            Collider collider = new Collider();
            collider.addAcceptor( new TestAcceptor() );
            collider.run();
            m_client.stopAndWait();
        }
        catch (IOException ex)
        {
            ex.printStackTrace();
        }
    }

    public static void main( String [] args )
    {
        int sessions = 1;
        int messages = 1000000;
        int messageLength = 250;

        if (args.length > 0)
            sessions = Integer.parseInt( args[0] );
        if (args.length > 1)
            messages = Integer.parseInt( args[1] );
        if (args.length > 2)
            messageLength = Integer.parseInt( args[2] );

        new Main(sessions, messages, messageLength).run();
    }
}
