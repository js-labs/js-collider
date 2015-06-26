/*
 * JS-Collider framework test.
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

package org.jsl.tests.remove_acceptor;

import org.jsl.collider.Acceptor;
import org.jsl.collider.Collider;
import org.jsl.collider.RetainableByteBuffer;
import org.jsl.collider.Session;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;


public class Main
{
    /* Test 1:
     * acceptor will be removed right after added to the collider.
     */
    private static class Test1Acceptor extends Acceptor
    {
        public void onAcceptorStarted( Collider collider, int localPort )
        {
            /* Should never be called. */
            throw new AssertionError();
        }

        public Session.Listener createSessionListener( Session session )
        {
            /* Should never be called */
            throw new AssertionError();
        }
    }

    /* Test 2:
     * acceptor will be removed in the onAcceptorStarted() callback.
     */
    private static class Test2Acceptor extends Acceptor
    {
        public void onAcceptorStarted( Collider collider, int localPort )
        {
            System.out.println( "Test2: port " + localPort );
            try { collider.removeAcceptor( this ); }
            catch (final InterruptedException ignored) {}
        }

        public Session.Listener createSessionListener( Session session )
        {
            /* Should never be called */
            throw new AssertionError();
        }
    }

    /*
     * Test 3:
     * acceptor will be removed in a one of createSessionListener() callback.
     */
    private static class Test3Acceptor extends Acceptor
    {
        private final Collider m_collider;
        private final AtomicInteger m_testsRemaining;
        private final AtomicInteger m_sessionsRemaining;

        public Test3Acceptor( Collider collider, AtomicInteger testsRemaining )
        {
            m_collider = collider;
            m_testsRemaining = testsRemaining;

            final Random random = new Random( System.nanoTime() );
            m_sessionsRemaining = new AtomicInteger( random.nextInt(50) );
        }

        public void onAcceptorStarted( Collider collider, int localPort )
        {
            assert( m_collider == collider );
            System.out.println( "Test3: port " + localPort + " (" + m_sessionsRemaining.get() + " sessions)" );
            final InetSocketAddress addr = new InetSocketAddress( "localhost", localPort );
            new TestClient( addr, "Test3" ).start();
            new TestClient( addr, "Test3" ).start();
        }

        public Session.Listener createSessionListener( Session session )
        {
            final int sessionsRemaining = m_sessionsRemaining.decrementAndGet();
            if (sessionsRemaining > 0)
                return new Test3Listener();

            try
            {
               m_collider.removeAcceptor( this );
            }
            catch (final InterruptedException ex)
            {
                ex.printStackTrace();
            }

            final int testsRemaining = m_testsRemaining.decrementAndGet();
            if (testsRemaining == 0)
                m_collider.stop();

            return null;
        }
    }

    private static class Test3Listener implements Session.Listener
    {
        public void onDataReceived( RetainableByteBuffer data )
        {
            /* Should never be called. */
            throw new AssertionError();
        }

        public void onConnectionClosed()
        {
        }
    }

    /*
     * Test 4:
     * acceptor will be removed after some connections will be accepted.
     */
    private static class Test4Acceptor extends Acceptor
    {
        private final AtomicInteger m_testsRemaining;

        public Test4Acceptor( AtomicInteger testsRemaining )
        {
            m_testsRemaining = testsRemaining;
        }

        public void onAcceptorStarted( Collider collider, int localPort )
        {
            final Random random = new Random( System.nanoTime() );
            final int timeout = 500*random.nextInt(20);
            System.out.println( "Test4: port " + localPort + " (waiting " + timeout/1000 + " sec)" );
            final InetSocketAddress addr = new InetSocketAddress( "localhost", localPort );
            new TestClient( addr, "Test4" ).start();
            new TestClient( addr, "Test4" ).start();
            new Test4Stopper( collider, this, m_testsRemaining, timeout ).start();
        }

        public Session.Listener createSessionListener( Session session )
        {
            return new Test4Listener();
        }
    }

    private static class Test4Listener implements Session.Listener
    {
        public void onDataReceived( RetainableByteBuffer data )
        {
            /* Should never be called. */
            throw new AssertionError();
        }

        public void onConnectionClosed()
        {
        }
    }

    private static class Test4Stopper extends Thread
    {
        private final Collider m_collider;
        private final Acceptor m_acceptor;
        private final AtomicInteger m_testsRemaining;
        private final int m_timeout;

        public Test4Stopper( Collider collider, Acceptor acceptor, AtomicInteger testsRemaining, int timeout )
        {
            m_collider = collider;
            m_acceptor = acceptor;
            m_testsRemaining = testsRemaining;
            m_timeout = timeout;
        }

        public void run()
        {
            try { Thread.sleep( m_timeout ); }
            catch (final InterruptedException ex) { ex.printStackTrace(); }

            System.out.println( "Test4: removing acceptor..." );
            try { m_collider.removeAcceptor( m_acceptor ); }
            catch (final InterruptedException ex) { ex.printStackTrace(); }

            final int testsRemaining = m_testsRemaining.decrementAndGet();
            if (testsRemaining == 0)
                m_collider.stop();
        }
    }

    /* Test client implementation */

    private static class TestClient extends Thread
    {
        private final InetSocketAddress m_addr;
        private final String m_name;

        public TestClient( InetSocketAddress addr, String name )
        {
            m_addr = addr;
            m_name = name;
        }

        public void run()
        {
            List<SocketChannel> sockets = new ArrayList<SocketChannel>();
            for (;;)
            {
                try
                {
                    final SocketChannel socketChannel = SocketChannel.open( m_addr );
                    Thread.sleep( 100 );
                    sockets.add( socketChannel );
                }
                catch (final IOException ex)
                {
                    /* Expected */
                    break;
                }
                catch (final InterruptedException ex)
                {
                    ex.printStackTrace();
                }
            }

            System.out.println( m_name + " (" + m_addr + ") : " + sockets.size() + " sessions." );

            for (SocketChannel socketChannel : sockets)
            {
                try { socketChannel.close(); }
                catch (final IOException ex) { ex.printStackTrace(); }
            }
        }
    }

    private Main()
    {
    }

    private void run( String [] args )
    {
        try
        {
            final AtomicInteger testsRemaining = new AtomicInteger(2);
            final Collider collider = Collider.create();
            final Test1Acceptor test1Acceptor = new Test1Acceptor();
            final Test2Acceptor test2Acceptor = new Test2Acceptor();
            final Test3Acceptor test3Acceptor = new Test3Acceptor( collider, testsRemaining );
            final Test4Acceptor test4Acceptor = new Test4Acceptor( testsRemaining );

            collider.addAcceptor( test1Acceptor );
            try { collider.removeAcceptor( test1Acceptor ); }
            catch (final InterruptedException ex) { ex.printStackTrace(); }

            collider.addAcceptor( test2Acceptor );
            collider.addAcceptor( test3Acceptor );
            collider.addAcceptor( test4Acceptor );

            collider.run();

            collider.removeAcceptor( test1Acceptor );
            collider.removeAcceptor( test2Acceptor );
            collider.removeAcceptor( test3Acceptor );
            //collider.removeAcceptor( test4Acceptor );
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
