/*
 * JS-Collider framework tests.
 * Copyright (C) 2022 Sergey Zubarev
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

import org.jsl.collider.Acceptor;
import org.jsl.collider.Collider;
import org.jsl.collider.RetainableByteBuffer;
import org.jsl.collider.Session;
import java.io.IOException;
import java.net.InetSocketAddress;

public class Server
{
    private final int m_messages;
    private final int m_messageLength;
    private InetSocketAddress m_addr;
    private int m_testId;

    private class ServerListener implements Session.Listener
    {
        private final Session m_session;
        private final int m_testId;
        private int m_messages;

        ServerListener(Session session, int testId)
        {
            m_session = session;
            m_testId = testId;
        }

        public void onDataReceived(RetainableByteBuffer data)
        {
            final int pos = data.position();
            final int bytesReceived = data.remaining();
            final int messageLength = data.getInt(pos);

            if (bytesReceived != messageLength)
                throw new AssertionError();

            // for the first test client is responsible to close connection
            final RetainableByteBuffer reply = data.slice();
            m_session.sendData(reply);
            reply.release();

            final int messages = ++m_messages;
            if (m_testId == 1)
            {
                // do nothing
            }
            else if (m_testId == 2)
            {
                // for the second test server supposed to close connection
                if (messages >= Server.this.m_messages)
                    m_session.closeConnection();
            }
        }

        public void onConnectionClosed()
        {
            System.out.println("Server: connection " +
                    m_session.getLocalAddress() + " -> " + m_session.getRemoteAddress() + ": closed");
            if (m_testId == 1)
                new Client(m_addr, /*test id*/2, m_messageLength);
            else if (m_testId == 2)
                m_session.getCollider().stop();
        }
    }

    private class TestAcceptor extends Acceptor
    {
        TestAcceptor()
        {
            super(0);
        }

        public void onAcceptorStarted(Collider collider, int localPort)
        {
            System.out.println("Server started at port " + localPort);
            final int messageLength = collider.getConfig().inputQueueBlockSize;
            m_addr = new InetSocketAddress("localhost", localPort);
            new Client(m_addr, /*test id*/1, messageLength);
        }

        public Session.Listener createSessionListener(Session session)
        {
            final int testId = ++m_testId;
            System.out.println("Server: connection " +
                    session.getLocalAddress() + " -> " + session.getRemoteAddress() + " accepted, " + testId);
            return new ServerListener(session, testId);
        }
    }

    public Server(int messages, int messageLength)
    {
        m_messages = messages;
        m_messageLength = messageLength;
        m_testId = 0;
    }

    public void run()
    {
        try
        {
            /* Would be better to avoid message fragmentation. */
            final Collider.Config config = new Collider.Config();
            config.inputQueueBlockSize = m_messageLength;

            final Collider collider = Collider.create(config);
            collider.addAcceptor(new TestAcceptor());
            collider.run();
        }
        catch (final IOException ex)
        {
            ex.printStackTrace();
        }
    }
}
