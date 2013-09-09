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

import org.jsl.collider.Acceptor;
import org.jsl.collider.Collider;
import org.jsl.collider.Session;
import org.jsl.tests.Util;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

public class Main
{
    private final AtomicInteger m_requests;
    private boolean m_runClient;
    private long m_startTime;
    private int m_expectedRequests;

    private class ServerListener implements Session.Listener
    {
        private Session m_session;

        public ServerListener( Session session )
        {
            m_session = session;
        }

        public void onDataReceived( ByteBuffer data )
        {
            int requests = m_requests.incrementAndGet();
            if (requests == 1)
            {
                m_startTime = System.nanoTime();
                int pos = data.position();
                data.getInt(); // skip message length
                m_expectedRequests = data.getInt();
                data.position(pos);
            }
            m_session.sendData( data );
            if (requests == m_expectedRequests)
            {
                long endTime = System.nanoTime();
                System.out.println( "Processed " + requests + " requests at " +
                        Util.formatDelay(m_startTime, endTime) + " sec." );
                if (m_runClient)
                    m_session.getCollider().stop();
                m_requests.set(0);
            }
        }

        public void onConnectionClosed()
        {
        }
    }

    private class TestAcceptor extends Acceptor
    {
        public TestAcceptor()
        {
            super( new InetSocketAddress(0) );
        }

        public void onAcceptorStarted( int localPort )
        {
            System.out.println( "Acceptor started at port " + localPort );
            if (m_runClient)
            {
                Client client = new Client( "localhost", localPort, 1, 20, 200 );
                client.start();
            }
        }

        public Session.Listener createSessionListener( Session session )
        {
            return new ServerListener(session);
        }
    }

    private Main( boolean runClient )
    {
        m_requests = new AtomicInteger();
        m_runClient = runClient;
    }

    private void run()
    {
        try
        {
            Collider collider = new Collider();
            collider.addAcceptor( new TestAcceptor() );
            collider.run();
        }
        catch (IOException ex)
        {
            System.out.println( ex.toString() );
        }
    }

    private static void print_usage()
    {
        System.out.println(
                "Arguments: <host> <port> <threads> <messages> <message_length>" );
    }

    public static void main( String [] args )
    {
        if (args.length == 0)
            new Main(true).run();
        else if ((args.length == 1) && (args[0].equals("server")))
            new Main(false).run();
        else if ((args.length == 6) && (args[0].equals("client")))
        {
            try
            {
                int port = Integer.parseInt( args[1] );
                int threads = Integer.parseInt( args[2] );
                int messages = Integer.parseInt( args[3] );
                int messageLen = Integer.parseInt( args[4] );
                Client client = new Client( args[0], port, threads, messages, messageLen );
                client.start();
            }
            catch (NumberFormatException ex)
            {
                ex.printStackTrace();
                print_usage();
            }
        }
        else
            print_usage();
    }
}
