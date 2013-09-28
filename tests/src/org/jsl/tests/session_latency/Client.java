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

package org.jsl.tests.session_latency;

import org.jsl.tests.Util;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;

public class Client
{
    private int m_portNumber;
    private int m_messages;
    private byte [] m_message;
    private Thread [] m_threads;

    private class SessionThread extends Thread
    {
        public void run()
        {
            try
            {
                byte [] bb = new byte[m_message.length];
                Socket socket = new Socket( "localhost", m_portNumber );
                System.out.println( "Client socket connected " + socket.getRemoteSocketAddress() + "." );
                socket.setTcpNoDelay( true );

                for (int idx=0; idx<10; idx++)
                {
                    socket.getOutputStream().write(m_message);
                    int rc = socket.getInputStream().read( bb );
                    if (rc != bb.length)
                        throw new IOException("Invalid message received.");
                }

                long startTime = System.nanoTime();
                for (int c=m_messages; c>0; c--)
                {
                    socket.getOutputStream().write( m_message );
                    int rc = socket.getInputStream().read( bb );
                    if (rc != bb.length)
                        throw new IOException("Invalid message received.");
                }
                long endTime = System.nanoTime();
                socket.close();

                int messages = (m_messages * 2);
                long tm = ((endTime - startTime) / 1000);
                System.out.println( messages + " messages exchanged at " +
                                    Util.formatDelay(startTime, endTime) + " sec (" +
                                    (tm / messages) + " usec/msg)" );
            }
            catch (IOException ex)
            {
                ex.printStackTrace();
            }
        }
    }

    public Client( int sessions, int messages, int messageLength )
    {
        m_messages = messages;
        m_message = new byte[messageLength];
        ByteBuffer byteBuffer = ByteBuffer.wrap( m_message );
        byteBuffer.putInt( messageLength );
        byteBuffer.putInt( sessions );

        for (int i=0; i<messageLength-8; i++)
            byteBuffer.put( (byte) i );

        m_threads = new Thread[sessions];
        for (int idx=0; idx<sessions; idx++)
            m_threads[idx] = new SessionThread();
    }

    public void start( int portNumber )
    {
        m_portNumber = portNumber;
        for (Thread thread : m_threads)
            thread.start();
    }

    public void stopAndWait()
    {
        for (Thread thread : m_threads)
        {
            try
            {
                thread.join();
            }
            catch (InterruptedException ex)
            {
                ex.printStackTrace();
            }
        }
    }

    public static void main( String [] args )
    {
        if (args.length != 4)
        {
            System.out.println( "Usage: <server_port_number> <sessions> <messages> <message_length>" );
            return;
        }
        int portNumber = Integer.parseInt( args[0] );
        int sessions = Integer.parseInt( args[1] );
        int messages = Integer.parseInt( args[2] );
        int messageLength = Integer.parseInt( args[3] );

        new Client(sessions, messages, messageLength).start( portNumber );
    }
}
