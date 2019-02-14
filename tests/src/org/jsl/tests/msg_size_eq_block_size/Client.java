/*
 * JS-Collider framework tests.
 * Copyright (C) 2018 Sergey Zubarev
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

package org.jsl.tests.msg_size_eq_block_size;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class Client
{
    private final InetSocketAddress m_addr;
    private final int m_testId;
    private final int m_messageLength;
    private final Thread m_thread;

    private class ClientThread extends Thread
    {
        public void run()
        {
            try
            {
                final int messageLength = m_messageLength;
                final SocketChannel socketChannel = SocketChannel.open(m_addr);
                final Socket socket = socketChannel.socket();
                socket.setTcpNoDelay(true);

                final SocketAddress remoteAddr = socket.getRemoteSocketAddress();
                System.out.println(
                        "Client " + m_testId + ": " + socket.getLocalSocketAddress() +
                        " -> " + remoteAddr + ": connected");

                final ByteBuffer bb = ByteBuffer.allocateDirect(messageLength);
                bb.putInt(0, messageLength);
                for (int idx=4; idx<messageLength; idx++)
                    bb.put(idx, (byte) idx);

                int msgs = 0;

                for (;;)
                {
                    final int bytesSent = socketChannel.write(bb);
                    assert(bytesSent == messageLength);

                    bb.clear();
                    final int bytesReceived = socketChannel.read(bb);

                    if (m_testId == 1)
                    {
                        // for the first test client supposed to close connection
                        if (++msgs == 10)
                            break;
                    }
                    else if (m_testId == 2)
                    {
                        // second client reply all the time,
                        // server will close connection
                        if (bytesReceived <= 0)
                            break;
                    }

                    assert(messageLength == bytesReceived);
                    bb.flip();
                    assert(bb.getInt(0) == messageLength);
                }

                socketChannel.close();

                System.out.println(
                        "Client " + m_testId + ": " + socket.getLocalSocketAddress() +
                        " -> " + remoteAddr + ": connection closed");
            }
            catch (final IOException ex)
            {
                ex.printStackTrace();
            }
        }
    }

    public Client(InetSocketAddress addr, int testId, int messageLength)
    {
        m_addr = addr;
        m_testId = testId;
        m_messageLength = messageLength;
        m_thread = new ClientThread();
        m_thread.start();
    }

    public void stopAndWait()
    {
        try
        {
            m_thread.join();
        }
        catch (final InterruptedException ex)
        {
            ex.printStackTrace();
        }
    }
}
