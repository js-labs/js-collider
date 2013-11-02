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

package org.jsl.tests.queue_socket_send;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;


public class BufferSmartSender extends Sender
{
    private static class SessionImpl extends Session
    {
        public SessionImpl( SocketChannel socketChannel )
        {
            super( socketChannel );
        }

        public void sendData( ByteBuffer data )
        {
        }
    }

    private static class SessionImplFactory implements SessionFactory
    {
        public Session createSession( SocketChannel socketChannel )
        {
            return new SessionImpl( socketChannel );
        }
    }

    public BufferSmartSender( int sessions, int messages, int messageLength, int socketBufferSize )
    {
        super( "BufferSmart", sessions, messages, messageLength, socketBufferSize );
    }

    public void run()
    {
    }
}
