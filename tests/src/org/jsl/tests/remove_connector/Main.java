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

package org.jsl.tests.remove_connector;

import org.jsl.collider.Acceptor;
import org.jsl.collider.Collider;
import org.jsl.collider.Connector;
import org.jsl.collider.Session;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
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
            assert( false );
            return null;
        }

        public void onException( IOException ex )
        {
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
            catch (InterruptedException ex) { ex.printStackTrace(); }
            System.out.println( "Test2 connector removed." );

            final int testsRemaining = m_testsRemaining.decrementAndGet();
            if (testsRemaining == 0)
                m_collider.stop();

            return null;
        }

        public void onException( IOException ex )
        {
            assert( false );
        }
    }

    /*
     * Test 3:
     * Create connector for unreachable address.
     */
    private static class Test3Connector extends Connector
    {
        private final Collider m_collider;
        private final AtomicInteger m_testsRemaining;

        public Test3Connector( Collider collider, AtomicInteger testsRemaining )
        {
            /* Most probably no Doom server on localhost. */
            super( new InetSocketAddress("localhost", 666) );
            m_collider = collider;
            m_testsRemaining = testsRemaining;
        }

        public Session.Listener createSessionListener( Session session )
        {
            /* Should not be called. */
            assert( false );
            return null;
        }

        public void onException( IOException ex )
        {
            System.out.println( getAddr() + ": connect failed: " + ex.toString() );
            final int testsRemaining = m_testsRemaining.decrementAndGet();
            if (testsRemaining == 0)
                m_collider.stop();
        }
    }

    private static class TestAcceptor extends Acceptor
    {
        public void onAcceptorStarted( Collider collider, int localPort )
        {
            System.out.println( "Acceptor started at port " + localPort );
            final InetSocketAddress addr = new InetSocketAddress( "localhost", localPort );
            final AtomicInteger testsRemaining = new AtomicInteger( 2 );
            try
            {
                final Test2Connector test2Connector = new Test2Connector( addr, collider, testsRemaining );
                final Test3Connector test3Connector = new Test3Connector( collider, testsRemaining );
                collider.addConnector( test2Connector );
                collider.addConnector( test3Connector );
            }
            catch (IOException ex)
            {
                ex.printStackTrace();
                collider.stop();
            }
        }

        public Session.Listener createSessionListener( Session session )
        {
            return new ServerListener();
        }
    }

    private static class ServerListener implements Session.Listener
    {
        public void onDataReceived( ByteBuffer data )
        {
            assert( false );
        }

        public void onConnectionClosed()
        {
        }
    }

    private void run( String [] args )
    {
        try
        {
             /* Address does not really matter for test1,
              * connector will be removed as soon as will be added.
              */
            final InetSocketAddress addr = new InetSocketAddress( "localhost", 12345 );
            final Collider collider = Collider.create();
            final TestAcceptor testAcceptor = new TestAcceptor();

            final Test1Connector test1Connector = new Test1Connector( addr );
            collider.addAcceptor( testAcceptor );
            collider.addConnector( test1Connector );
            collider.removeConnector( test1Connector );
            System.out.println( "Test1 connector removed." );
            collider.run();
        }
        catch (IOException ex)
        {
            ex.printStackTrace();
        }
        catch (InterruptedException ex)
        {
            ex.printStackTrace();
        }
    }

    public static void main( String[] args )
    {
        new Main().run( args );
    }
}
