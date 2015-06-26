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

package org.jsl.tests.pubsub;

import org.jsl.collider.*;
import org.jsl.tests.Util;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/* ZeroMQ PUB-SUB pattern performance test */

public class Main
{
    private final int m_subscribers;
    private final int m_messages;
    private final int m_messageLength;
    private final int m_socketBufferSize;

    private final AtomicInteger m_subscribersDone;
    private Collider m_collider;
    private PubClient m_pubClient;
    private SubClient [] m_subClient;

    private final ReentrantLock m_lock;
    private Session m_pubSession;
    private Session [] m_subSession;
    private int m_subSessions;

    private class HandshakeListener implements Session.Listener
    {
        private final Session m_session;

        public HandshakeListener( Session session )
        {
            m_session = session;
        }

        public void onDataReceived( RetainableByteBuffer data )
        {
            if (data.remaining() != 5)
                throw new AssertionError();
            final int messageLength = data.getInt();
            assert( messageLength == 5 );
            final byte clientType = data.get();

            boolean readyToStart = false;

            m_lock.lock();
            try
            {
                if (clientType == 0)
                {
                    System.out.println( "Server: PubClient connection accepted." );
                    assert( m_pubSession == null );
                    m_pubSession = m_session;
                    m_pubSession.replaceListener( new PubListener() );

                    if (m_subSessions == m_subSession.length)
                        readyToStart = true;
                }
                else if (clientType == 1)
                {
                    System.out.println( "Server: SubClient connection accepted." );
                    final int idx = m_subSessions++;
                    assert( idx < m_subSession.length );
                    m_subSession[idx] = m_session;

                    if ((m_subSessions == m_subSession.length) && (m_pubSession != null))
                        readyToStart = true;
                }
                else
                    throw new AssertionError();
            }
            finally
            {
                m_lock.unlock();
            }

            if (readyToStart)
            {
                /* Send just one byte to start. */
                final ByteBuffer buf = ByteBuffer.allocateDirect( 1 );
                m_pubSession.sendData( buf );
            }
        }

        public void onConnectionClosed()
        {
        }
    }

    private class PubListener implements Session.Listener
    {
        private final StreamDefragger m_stream;
        private int m_messagesProcessed;

        public PubListener()
        {
            m_stream = new StreamDefragger( 4 )
            {
                protected int validateHeader( ByteBuffer header )
                {
                    return header.getInt();
                }
            };
        }

        public void onDataReceived( RetainableByteBuffer data )
        {
            RetainableByteBuffer msg = m_stream.getNext( data );
            while (msg != null)
            {
                final int position = msg.position();
                final int remaining = msg.remaining();
                final int messageLength = msg.getInt( position );
                if (remaining != messageLength)
                    throw new AssertionError();

                m_messagesProcessed++;
                final RetainableByteBuffer reply = msg.slice();
                for (Session s : m_subSession)
                    s.sendData( reply );
                reply.release();

                msg = m_stream.getNext();
            }
        }

        public void onConnectionClosed()
        {
            System.out.println( "PubServer: processed " + m_messagesProcessed + " messages." );
        }
    }

    private class PubSubAcceptor extends Acceptor
    {
        public PubSubAcceptor()
        {
            socketRecvBufSize = m_socketBufferSize;
            socketSendBufSize = m_socketBufferSize;
            joinMessageMaxSize = 1024;
        }

        public Session.Listener createSessionListener( Session session )
        {
            return new HandshakeListener( session );
        }

        public void onAcceptorStarted( Collider collider, int portNumber )
        {
            System.out.println( "Server started at port " + portNumber + "." );
            final InetSocketAddress addr = new InetSocketAddress( "localhost", portNumber );

            m_pubClient = new PubClient( addr, m_messages, m_messageLength, m_socketBufferSize );
            m_pubClient.start();

            for (int idx=0; idx<m_subscribers; idx++)
            {
                m_subClient[idx] = new SubClient( Main.this, addr, m_socketBufferSize );
                m_subClient[idx].start();
            }
        }
    }

    private Main( int subscribers, int messages, int messageLength, int socketBufferSize )
    {
        m_subscribers = subscribers;
        m_messages = messages;
        m_messageLength = messageLength;
        m_socketBufferSize = socketBufferSize;
        m_subscribersDone = new AtomicInteger(0);

        m_lock = new ReentrantLock();
        m_subClient = new SubClient[subscribers];
        m_subSession = new Session[subscribers];
    }

    private void run()
    {
        try
        {
            m_collider = Collider.create();
            m_collider.addAcceptor( new PubSubAcceptor() );
            m_collider.run();

            m_pubClient.join();

            assert( m_subClient.length == m_subscribers );
            for (int idx=0; idx<m_subscribers; idx++)
                m_subClient[idx].join();
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

    public void onSubscriberDone()
    {
        final int subscribersDone = m_subscribersDone.incrementAndGet();
        if (subscribersDone == m_subscribers)
        {
            final long endTime = System.nanoTime();
            System.out.println(
                    "Test done: " + Util.formatDelay(m_pubClient.getStartTime(), endTime) + " sec." );
            m_collider.stop();
        }
    }

    public static void main( String args[] )
    {
        int subscribers = 1;
        int messages = 100000;
        int messageLength = 100;
        int socketBufferSize = (64 * 1024);

        if (args.length > 0)
            subscribers = Integer.parseInt( args[0] );

        if (args.length > 1)
            messages = Integer.parseInt( args[1] );

        if (args.length > 2)
            messageLength = Integer.parseInt( args[2] );

        System.out.println(
                "PubSub throughput test: " +
                subscribers + " subscribers, " +
                messages + " messages, " +
                messageLength + " bytes/message." );

        new Main(subscribers, messages, messageLength, socketBufferSize).run();
    }
}
