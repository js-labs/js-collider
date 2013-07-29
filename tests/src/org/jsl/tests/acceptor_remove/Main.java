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

import org.jsl.collider.Acceptor;
import org.jsl.collider.Collider;
import org.jsl.collider.Session;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


public class Main
{
    private Collider m_collider;
    private ScheduledExecutorService m_scheduler;
    private Random m_random;

    private static class ServerSessionListener implements Session.Listener
    {
        private Session m_session;
        public ServerSessionListener( Session session ) { m_session = session; }
        public void onDataReceived( ByteBuffer data ) { m_session.sendData( data ); }
        public void onConnectionClosed() { }
    }

    private class TestAcceptor1 extends Acceptor
    {
        public TestAcceptor1()
        {
            super( new InetSocketAddress(0) );
        }

        public void onAcceptorStarted( int localPort )
        {
            System.out.println( "Acceptor1 listening port " + localPort );
            m_collider.removeAcceptor( this );
        }

        public Session.Listener createSessionListener( Session session )
        {
            /* Should never be called */
            assert( false );
            return null;
        }
    }


    private class TestAcceptor2 extends Acceptor
    {
        private int m_maxSessions;

        public TestAcceptor2( int maxSessions )
        {
            super( new InetSocketAddress(0) );
            m_maxSessions = maxSessions;
        }

        public void onAcceptorStarted( int localPort )
        {
            System.out.println( "Acceptor2 listening port " + localPort );
            new Client(localPort, 32).start(4);
        }

        public Session.Listener createSessionListener( Session session )
        {
            if (--m_maxSessions == 0)
            {
                //m_collider.removeAcceptor( this );
                m_collider.stop();
                //m_scheduler.schedule( new Timer(), 1, TimeUnit.SECONDS );
            }
            System.out.println( "Connection accepted " + session.getRemoteAddress() );
            return new ServerSessionListener( session );
        }
    }


    private class Timer implements Runnable
    {
        public void run()
        {
            int maxSessions = m_random.nextInt( 50 );
            System.out.println( "Staring acceptor (" + maxSessions + " sessions)..." );
            m_collider.addAcceptor( new TestAcceptor2(maxSessions) );
        }
    }


    private Main()
    {
        m_scheduler = Executors.newScheduledThreadPool(1);
        m_random = new Random();
    }


    private void run( String[] args )
    {
        try
        {
            m_collider = new Collider();
            m_scheduler.schedule( new Timer(), 1, TimeUnit.SECONDS );
            m_collider.addAcceptor( new TestAcceptor1() );
            m_collider.run();
        }
        catch (IOException ex)
        {
            System.out.println( ex.toString() );
        }
    }


    public static void main( String[] args )
    {
        new Main().run( args );
    }
}
