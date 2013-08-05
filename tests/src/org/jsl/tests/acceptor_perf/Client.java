package org.jsl.tests.acceptor_perf;

import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import org.jsl.tests.Util;

public class Client implements Runnable
{
    private String m_host;
    private int m_port;
    private int m_threads;
    private int m_connections;
    private ByteBuffer m_msg;

    public Client( String host, int port, int threads, int connections, int messageLen )
    {
        m_host = host;
        m_port = port;
        m_threads = threads;
        m_connections = connections;

        byte [] msg = new byte[messageLen];
        m_msg = ByteBuffer.wrap( msg );

        m_msg.putInt( messageLen );
        m_msg.putInt( threads*connections );
        for (int idx=0; idx<messageLen-8; idx++)
            m_msg.put( (byte) idx );
    }

    public void run()
    {
        ByteBuffer bb = m_msg.duplicate();
        int messageLen = m_msg.capacity();
        try
        {
            long startTime = System.nanoTime();
            for (int idx=m_connections; idx>0; idx--)
            {
                Socket socket = new Socket( m_host, m_port );
                socket.setTcpNoDelay( true );
                socket.getOutputStream().write( m_msg.array() );
                int rc = socket.getInputStream().read( bb.array() );
                if (rc == messageLen)
                {
                    bb.position( 0 );
                    bb.limit( messageLen );
                    int v = bb.getInt();
                    assert( v == messageLen );
                    v = bb.getInt();
                    assert( v == (m_connections*m_threads) );
                }
                socket.close();
            }
            long endTime = System.nanoTime();
            System.out.println( "Sent " + m_connections + " requests at " +
                                Util.formatDelay(startTime, endTime) + " sec." );
        }
        catch (IOException ex)
        {
            ex.printStackTrace();
        }
    }

    public void start()
    {
        for (int idx=0; idx<m_threads; idx++)
            new Thread(this).start();
    }
}
