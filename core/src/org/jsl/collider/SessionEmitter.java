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

package org.jsl.collider;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.logging.Level;
import java.util.logging.Logger;


public abstract class SessionEmitter
{
    private static final Logger s_logger = Logger.getLogger( SessionImpl.class.getName() );

    private final InetSocketAddress m_addr;

    public boolean reuseAddr;
    public boolean tcpNoDelay;

    public int socketRecvBufSize;
    public int socketSendBufSize;
    public int inputQueueMaxSize;
    public int inputQueueBlockSize;
    public int outputQueueBlockSize;

    public SessionEmitter( InetSocketAddress addr )
    {
        m_addr = addr;

        reuseAddr = false;
        tcpNoDelay = false;

        /* Use collider global settings by default */
        socketRecvBufSize = 0;
        socketSendBufSize = 0;
        inputQueueMaxSize = 0;
        inputQueueBlockSize = 0;
        outputQueueBlockSize = 0;
    }

    public InetSocketAddress getAddr()
    {
        return m_addr;
    }

    public void configureSocketChannel( Collider collider, SocketChannel socketChannel )
    {
        final Socket socket = socketChannel.socket();
        try
        {
            socket.setTcpNoDelay( tcpNoDelay );
        }
        catch (SocketException ex)
        {
            if (s_logger.isLoggable(Level.WARNING))
            {
                s_logger.log( Level.WARNING,
                        socket.getRemoteSocketAddress().toString() +
                        ": setTcpNoDelay(" + tcpNoDelay + ") failed: " + ex.toString() );
            }
        }

        try
        {
            socket.setReuseAddress( reuseAddr );
        }
        catch (SocketException ex)
        {
            if (s_logger.isLoggable(Level.WARNING))
            {
                s_logger.log( Level.WARNING,
                        socket.getRemoteSocketAddress().toString() +
                        ": setReuseAddress(" + reuseAddr + ") failed: " + ex.toString() );
            }
        }

        int bufSize = socketRecvBufSize;
        if (socketRecvBufSize == 0)
            bufSize = collider.getConfig().socketRecvBufSize;

        if (bufSize > 0)
        {
            try
            {
                socket.setReceiveBufferSize( bufSize );
            }
            catch (SocketException ex)
            {
                if (s_logger.isLoggable(Level.WARNING))
                {
                    s_logger.log( Level.WARNING,
                            socket.getRemoteSocketAddress().toString() +
                            ": setReceiveBufferSize(" + bufSize + ") failed: " + ex.toString() );
                }
            }
        }

        bufSize = socketSendBufSize;
        if (socketSendBufSize == 0)
            bufSize = collider.getConfig().socketSendBufSize;

        if (bufSize > 0)
        {
            try
            {
                socket.setSendBufferSize( bufSize );
            }
            catch (SocketException ex)
            {
                if (s_logger.isLoggable(Level.WARNING))
                {
                    s_logger.log( Level.WARNING,
                            socket.getRemoteSocketAddress().toString() +
                            ": setSendBufferSize(" + bufSize + ") failed: " + ex.toString() );
                }
            }
        }
    }

    public void onException( IOException exception ) {}

    public abstract Session.Listener createSessionListener( Session session );
}
