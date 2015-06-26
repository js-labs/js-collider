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

package org.jsl.tests.session_close;

import org.jsl.tests.StreamDefragger;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class Client
{
    private final static int SOCKET_BUFFER_SIZE = (64 * 1024);
    private InetSocketAddress m_addr;

    private static ByteBuffer createBatch( int messages, int messageLength, int testType )
    {
        int batchSize = (messages * messageLength);
        if (batchSize > 1024*1024)
            batchSize = 1024*1024;

        final int batchMessages = (batchSize / messageLength);
        batchSize = (batchMessages * messageLength);

        final ByteBuffer batch = ByteBuffer.allocateDirect( batchSize );
        for (int idx=0; idx<batchMessages; idx++)
        {
            batch.putInt( messageLength );
            batch.putInt( testType );
            for (int cc=8; cc<messageLength; cc++)
                batch.put( (byte) cc );
        }
        batch.position(0);
        return batch;
    }

    /* Test sends some data to the server, server responds them back,
     * but test does not receive anything, just close a connection.
     */
    private class Test1 extends Thread
    {
        private static final int MESSAGES = 1000;
        private static final int MESSAGE_LENGTH = 5000;

        public void run()
        {
            final ByteBuffer batch = createBatch( MESSAGES, MESSAGE_LENGTH, 1 );
            try
            {
                final SocketChannel socketChannel = SocketChannel.open( m_addr );
                final Socket socket = socketChannel.socket();
                socket.setTcpNoDelay( true );
                socket.setSendBufferSize( SOCKET_BUFFER_SIZE );
                socket.setReceiveBufferSize( SOCKET_BUFFER_SIZE );

                System.out.println(
                        "Test1 connected " + socket.getLocalSocketAddress() +
                        " -> " + socket.getRemoteSocketAddress() + "." );

                socketChannel.write( batch );
                socketChannel.close();
            }
            catch (IOException ex)
            {
                ex.printStackTrace();
            }
        }
    }

    /* Test sends some data to the server, server echo received data,
     * and then close connection. Test should receive back all messages
     * server sent before closeConnection() call. Last message before
     * closeConnection() call is market by (int)-1 after length.
     */
    private class Test2 extends Thread
    {
        private static final int BATCH_MESSAGES = 1000;
        private static final int MESSAGE_LENGTH = 500;
        private static final int SOCKET_BUFFER_SIZE = (64 * 1024);

        public void run()
        {
            final ByteBuffer batch = createBatch( BATCH_MESSAGES, MESSAGE_LENGTH, 2 );
            SocketChannel socketChannel = null;
            try
            {
                socketChannel = SocketChannel.open( m_addr );
                final Socket socket = socketChannel.socket();
                socket.setTcpNoDelay( true );
                socket.setSendBufferSize( SOCKET_BUFFER_SIZE );
                socket.setReceiveBufferSize( SOCKET_BUFFER_SIZE );

                System.out.println(
                        "Test2 connected " + socket.getLocalSocketAddress() +
                        " -> " + socket.getRemoteSocketAddress() + "." );

                Test2Reader reader = new Test2Reader( socketChannel );
                reader.start();

                for (;;)
                {
                    final int bytesSent = socketChannel.write( batch );
                    if (bytesSent == 0)
                        break;
                    batch.position(0);
                }

                reader.join();
            }
            catch (IOException ex)
            {
                /* IOException is expected, just skip it.
                 * ex.printStackTrace();
                 */
            }
            catch (InterruptedException ex)
            {
                ex.printStackTrace();
            }

            if (socketChannel != null)
            {
                try { socketChannel.close(); }
                catch (IOException ex) { ex.printStackTrace(); }
            }
        }
    }

    private class Test2Reader extends Thread
    {
        private final SocketChannel m_socketChannel;

        public Test2Reader( SocketChannel socketChannel )
        {
            m_socketChannel = socketChannel;
        }

        public void run()
        {
            final StreamDefragger stream = new StreamDefragger(4)
            {
                protected int validateHeader( ByteBuffer header )
                {
                    return header.getInt();
                }
            };

            final ByteBuffer byteBuffer = ByteBuffer.allocateDirect( 128*1024 );
            boolean lastMessageReceived = false;
            try
            {
                for (;;)
                {
                    final int bytesReceived = m_socketChannel.read( byteBuffer );
                    if (bytesReceived == 0)
                        break;

                    byteBuffer.flip();
                    ByteBuffer msg = stream.getNext( byteBuffer );
                    while (msg != null)
                    {
                        assert( !lastMessageReceived );
                        msg.getInt(); // skip length
                        final int messageType = msg.getInt();
                        assert( (messageType == 2) || (messageType == -1) );
                        if (messageType == -1)
                            lastMessageReceived = true;
                        msg = stream.getNext();
                    }
                    byteBuffer.clear();
                }
            }
            catch (final IOException ex)
            {
                /* IOException is expected, just skip it.
                 * ex.printStackTrace();
                 */
            }
            assert( lastMessageReceived );
        }
    }

    public Client()
    {
    }

    public void start( InetSocketAddress addr )
    {
        m_addr = addr;
        new Test1().start();
        new Test2().start();
    }
}
