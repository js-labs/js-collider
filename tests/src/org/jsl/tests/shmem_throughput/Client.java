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

package org.jsl.tests.shmem_throughput;

import org.jsl.collider.RetainableByteBuffer;
import org.jsl.collider.Session;
import org.jsl.collider.ShMemClient;
import org.jsl.tests.Util;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;


public class Client
{
    public static final int MSG_MAGIC = 0x1ABCDEF2;

    public static class Connector extends org.jsl.collider.Connector
    {
        private final boolean m_useShMem;
        private final ByteBuffer m_msg;
        private final int m_messages;

        public Connector( InetSocketAddress addr, boolean useShMem, int messages, int messageLength )
        {
            super( addr );
            m_useShMem = useShMem;
            m_messages = messages;

            /* Server expects messages with the following structure:
             * 4 bytes : message length
             * 4 bytes : total messages will be sent
             * 4 bytes : magic number
             */
            assert( messageLength >= 12 );
            m_msg = ByteBuffer.allocateDirect( messageLength );
            m_msg.putInt( messageLength );
            m_msg.putInt( messages );
            m_msg.putInt( MSG_MAGIC );
            for (int idx=12; idx<messageLength; idx++)
                m_msg.put( (byte) idx );
            m_msg.flip();
        }

        public Session.Listener createSessionListener( Session session )
        {
            return new HandshakeListener( session, m_useShMem, m_messages, m_msg );
        }

        public void onException( IOException ex )
        {
            /* should never be called */
            throw new AssertionError();
        }
    }

    private static class HandshakeListener implements Session.Listener
    {
        private final Session m_session;
        private final int m_messages;
        private final ByteBuffer m_msg;
        private ShMemClient m_shMem;

        public HandshakeListener( Session session, boolean useShMem, int messages, ByteBuffer msg )
        {
            m_session = session;
            m_messages = messages;
            m_msg = msg;

            System.out.println( session.getLocalAddress() + ": connected to server " + session.getRemoteAddress() );
            ByteBuffer buf = null;

            /* Shared memory IPC obviously can be used only for local host connections.
             * Following approach will not always work properly (for example if SSH tunnel is used),
             * but fine for the test.
             */
            if (useShMem &&
                (session.getRemoteAddress() instanceof InetSocketAddress) &&
                (session.getLocalAddress() instanceof InetSocketAddress) &&
                ((InetSocketAddress) session.getRemoteAddress()).getAddress().isLoopbackAddress())
            {
                final InetSocketAddress localAddress = (InetSocketAddress) session.getLocalAddress();
                try
                {
                    ShMemClient shMem = new ShMemClient( Integer.toString(localAddress.getPort()), 64*1024 );
                    final int descriptorLength = shMem.getDescriptorLength();
                    buf = ByteBuffer.allocateDirect( 4 + descriptorLength );
                    buf.putInt( 4 + descriptorLength );
                    shMem.getDescriptor( buf );
                    buf.flip();
                    m_shMem = shMem;
                    System.out.println( session.getLocalAddress() + ": requesting SHMEM-IPC." );
                }
                catch (IOException ex)
                {
                    ex.printStackTrace();
                    useShMem = false;
                }
            }

            if (!useShMem)
            {
                buf = ByteBuffer.allocateDirect(4);
                buf.putInt( 0, 4 );
            }

            session.sendData( buf );
        }

        public void onDataReceived( RetainableByteBuffer data )
        {
            if (data.remaining() == 0)
                throw new AssertionError();

            if (data.getInt() == 0)
            {
                /* Server rejected shared memory IPC */
                if (m_shMem != null)
                    m_shMem.close();
            }
            else
            {
                /* Server accepted shared memory IPC */
                m_session.accelerate( m_shMem, null );
            }

            m_session.replaceListener( new Listener(m_session, m_messages, m_msg) );
        }

        public void onConnectionClosed()
        {
            System.out.println( m_session.getLocalAddress() + ": connection closed unexpectedly." );
            if (m_shMem != null)
                m_shMem.close();
        }
    }

    private static class Sender extends Thread
    {
        private final Session m_session;
        private final int m_messages;
        private final ByteBuffer m_msg;

        public Sender( Session session, int messages, ByteBuffer msg )
        {
            m_session = session;
            m_msg = msg;
            m_messages = messages;
        }

        public void run()
        {
            final long startTime = System.nanoTime();
            for (int idx=0; idx<m_messages; idx++)
                m_session.sendData( m_msg );
            final long endTime = System.nanoTime();

            System.out.println(
                    m_session.getLocalAddress() + ": sent " + m_messages +
                    " messages (" + m_msg.capacity()*m_messages + " bytes) at " +
                    Util.formatDelay(startTime, endTime) + " sec." );

            m_session.closeConnection();
        }
    }

    private static class Listener implements Session.Listener
    {
        private final Session m_session;
        private final Sender m_sender;

        public Listener( Session session, int messages, ByteBuffer msg )
        {
            m_session = session;
            m_sender = new Sender( session, messages, msg );
            m_sender.start();
        }

        public void onDataReceived( RetainableByteBuffer data )
        {
            /* Should never happen by test design. */
            throw new AssertionError();
        }

        public void onConnectionClosed()
        {
            try
            {
                m_sender.join();
            }
            catch (final InterruptedException ex)
            {
                ex.printStackTrace();
            }
            System.out.println( m_session.getLocalAddress() + ": connection closed." );
        }
    }
}
