/*
 * JS-Collider framework.
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
