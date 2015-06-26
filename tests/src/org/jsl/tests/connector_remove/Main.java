/*
 * JS-Collider framework tests.
 * Copyright (C) 2015 Sergey Zubarev
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

package org.jsl.tests.connector_remove;

import org.jsl.collider.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicInteger;

public class Main
{
    /* Test 1:
     * connector will be removed right after added to the collider.
     */
    private static class Test1Connector extends Connector
    {
        public Test1Connector( InetSocketAddress addr )
        {
            super( addr );
        }

        public Session.Listener createSessionListener( Session session )
        {
            /* Should never be called. */
            throw new AssertionError();
        }

        public void onException( IOException ex )
        {
            /* Should never be called. */
            throw new AssertionError();
        }
    }

    /* Test 2:
     * createSessionListener() returns null.
     * connector will be removed in createSessionListener() callback.
     */
    private static class Test2Connector extends Connector
    {
        private final Collider m_collider;
        private final AtomicInteger m_testsRemaining;

        public Test2Connector( InetSocketAddress addr, Collider collider, AtomicInteger testsRemaining )
        {
            super( addr );
            m_collider = collider;
            m_testsRemaining = testsRemaining;
        }

        public Session.Listener createSessionListener( Session session )
        {
            try { m_collider.removeConnector( this ); }
            catch (final InterruptedException ex) { ex.printStackTrace(); }
            System.out.println( "Test2 connector removed." );

            final int testsRemaining = m_testsRemaining.decrementAndGet();
            if (testsRemaining == 0)
                m_collider.stop();

            return null;
        }

        public void onException( IOException ex )
        {
            /* should never be called */
            throw new AssertionError();
        }
    }

    /* Test 3:
     * Create connector for unreachable address.
     */
    private static class Test3Connector extends Connector
    {
        private final Collider m_collider;
        private final AtomicInteger m_testsRemaining;

        public Test3Connector( InetSocketAddress addr, Collider collider, AtomicInteger testsRemaining )
        {
            super( addr );
            m_collider = collider;
            m_testsRemaining = testsRemaining;
        }

        public Session.Listener createSessionListener( Session session )
        {
            /* Should never be called. */
            throw new AssertionError();
        }

        public void onException( IOException ex )
        {
            System.out.println( getAddr() + ": connect failed: " + ex.toString() );
            final int testsRemaining = m_testsRemaining.decrementAndGet();
            if (testsRemaining == 0)
                m_collider.stop();
        }
    }

    /* Test 4:
     * Start and stop the connector few times
     */
    private static class Test4Connector extends Connector
    {
        final AtomicInteger m_sessionsCounter;

        public Test4Connector( InetSocketAddress addr, AtomicInteger sessionsCounter )
        {
            super( addr );
            m_sessionsCounter = sessionsCounter;
        }

        public Session.Listener createSessionListener( Session session )
        {
            session.closeConnection();
            return new SessionListener( session, m_sessionsCounter );
        }

        public void onException( IOException ex )
        {
            /* Should never be called. */
            throw new AssertionError();
        }
    }

    private static class Test4Thread extends Thread
    {
        private final Collider m_collider;
        private final InetSocketAddress m_addr;
        private final AtomicInteger m_testsRemaining;
        private final AtomicInteger m_sessions;

        public Test4Thread(
                Collider collider,
                InetSocketAddress addr,
                AtomicInteger testsRemaining,
                AtomicInteger sessions )
        {
            m_collider = collider;
            m_addr = addr;
            m_testsRemaining = testsRemaining;
            m_sessions = sessions;
        }

        public void run()
        {
            for (int idx=0; idx<10; idx++)
            {
                final Test4Connector test4Connector = new Test4Connector( m_addr, m_sessions );
                m_collider.addConnector( test4Connector );
                Thread.yield();
                Thread.yield();
                try { m_collider.removeConnector( test4Connector ); }
                catch (final InterruptedException ex) { ex.printStackTrace(); }
            }

            final int testsRemaining = m_testsRemaining.decrementAndGet();
            if (testsRemaining == 0)
                m_collider.stop();
        }
    }

    private static class TestAcceptor extends Acceptor
    {
        private final AtomicInteger m_sessionsCounter;

        public TestAcceptor( AtomicInteger sessionsCounter )
        {
            m_sessionsCounter = sessionsCounter;
        }

        public void onAcceptorStarted( Collider collider, int localPort )
        {
            System.out.println( "Acceptor started at port " + localPort );
            final InetSocketAddress addr = new InetSocketAddress( "localhost", localPort );
            final AtomicInteger testsRemaining = new AtomicInteger( 3 );

            final Test2Connector test2Connector = new Test2Connector( addr, collider, testsRemaining );
            collider.addConnector( test2Connector );

            /* Most probably no Doom server on localhost. */
            final InetSocketAddress doomAddr = new InetSocketAddress( "localhost", 666 );
            final Test3Connector test3Connector = new Test3Connector( doomAddr, collider, testsRemaining );
            collider.addConnector( test3Connector );

            final Test4Thread test4Thread = new Test4Thread( collider, addr, testsRemaining, m_sessionsCounter );
            test4Thread.start();
        }

        public Session.Listener createSessionListener( Session session )
        {
            return new SessionListener( session, m_sessionsCounter );
        }
    }

    private static class SessionListener implements Session.Listener
    {
        private final Session m_session;
        private final AtomicInteger m_sessionsCounter;

        public SessionListener( Session session, AtomicInteger sessionsCounter )
        {
            m_session = session;
            m_sessionsCounter = sessionsCounter;
            sessionsCounter.incrementAndGet();
            System.out.println( m_session.getLocalAddress() + "->" + m_session.getRemoteAddress() + ": init" );
        }

        public void onDataReceived( RetainableByteBuffer data )
        {
            /* Should never be called. */
            throw new AssertionError();
        }

        public void onConnectionClosed()
        {
            m_sessionsCounter.decrementAndGet();
            System.out.println( m_session.getLocalAddress() + "->" + m_session.getRemoteAddress() + ": close" );
        }
    }

    private void run( String [] args )
    {
        try
        {
            final Collider collider = Collider.create();
            final AtomicInteger sessionsCounter = new AtomicInteger();

            /* Address does not really matter for test1,
             * connector will be removed as soon as will be added.
             */
            final InetSocketAddress addr = new InetSocketAddress( "localhost", 666 );
            final Test1Connector test1Connector = new Test1Connector( addr );

            final TestAcceptor testAcceptor = new TestAcceptor( sessionsCounter );
            collider.addAcceptor( testAcceptor );
            
            collider.addConnector( test1Connector );
            collider.removeConnector( test1Connector );
            System.out.println( "Test1 connector removed." );

            collider.run();
            assert( sessionsCounter.get() == 0 );
        }
        catch (final IOException ex)
        {
            ex.printStackTrace();
        }
        catch (final InterruptedException ex)
        {
            ex.printStackTrace();
        }
    }

    public static void main( String[] args )
    {
        new Main().run( args );
    }
}
