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

package org.jsl.tests.acceptor_remove;

import org.jsl.tests.Util;

import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;


public class Client implements Runnable
{
    private int m_portNumber;
    private int m_messageSize;

    public Client( int portNumber, int messageSize )
    {
        m_portNumber = portNumber;
        m_messageSize = messageSize;
    }

    public void run()
    {
        byte [] msgOut = new byte[m_messageSize];
        byte [] msgIn = new byte[m_messageSize];

        ByteBuffer byteBuffer = ByteBuffer.wrap( msgOut );
        byteBuffer.putInt( m_messageSize );
        for (int j=0; j<m_messageSize-4; j++)
            byteBuffer.put( (byte) j );

        for (;;)
        {
            try
            {
                Socket socket = new Socket( "localhost", m_portNumber );
                System.out.println( "Client socket connected " + socket.getRemoteSocketAddress() + "." );
                socket.setTcpNoDelay( true );
                socket.getOutputStream().write( msgOut );
                int rc = socket.getInputStream().read( msgIn );
                if (rc == m_messageSize)
                {
                    for (int idx=0; idx<m_messageSize; idx++)
                        assert( msgIn[idx] == msgOut[idx] );
                }
                socket.close();
            }
            catch (IOException ignored)
            {
                break;
            }
        }
    }

    public void start( int threads )
    {
        for (int idx=0; idx<threads; idx++)
            new Thread(this).start();
    }
}
