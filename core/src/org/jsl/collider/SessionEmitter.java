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
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
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
        inputQueueBlockSize = 0;
        outputQueueBlockSize = 0;
    }

    public InetSocketAddress getAddr()
    {
        return m_addr;
    }

    public void configureSocketChannel( Collider collider, SocketChannel socketChannel )
    {
        try
        {
            socketChannel.setOption( StandardSocketOptions.TCP_NODELAY, tcpNoDelay );
        }
        catch (IOException ex)
        {
            if (s_logger.isLoggable(Level.WARNING))
            {
                s_logger.log( Level.WARNING,
                        socketChannel.socket().getRemoteSocketAddress().toString()
                        + ": SocketChannel.setOption(TCP_NODELAY, " + tcpNoDelay
                        + ") failed: " + ex.toString() );
            }
        }

        try
        {
            socketChannel.setOption( StandardSocketOptions.SO_REUSEADDR, reuseAddr );
        }
        catch (IOException ex)
        {
            if (s_logger.isLoggable(Level.WARNING))
            {
                s_logger.log( Level.WARNING,
                        socketChannel.socket().getRemoteSocketAddress().toString()
                        + ": SocketChannel.setOption(SO_REUSEADDR, " + reuseAddr
                        + ") failed: " + ex.toString() );
            }
        }

        int bufSize = socketRecvBufSize;
        if (socketRecvBufSize == 0)
            bufSize = collider.getConfig().socketRecvBufSize;

        if (bufSize > 0)
        {
            try
            {
                socketChannel.setOption( StandardSocketOptions.SO_RCVBUF, bufSize );
            }
            catch (IOException ex)
            {
                if (s_logger.isLoggable(Level.WARNING))
                {
                    s_logger.log( Level.WARNING,
                            socketChannel.socket().getRemoteSocketAddress().toString()
                            + ": SocketChannel.setOption(SO_RCVBUF, " + bufSize
                            + ") failed: " + ex.toString() );
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
                socketChannel.setOption( StandardSocketOptions.SO_SNDBUF, bufSize );
            }
            catch (IOException ex)
            {
                if (s_logger.isLoggable(Level.WARNING))
                {
                    s_logger.log( Level.WARNING,
                            socketChannel.socket().getRemoteSocketAddress().toString()
                            + ": SocketChannel.setOption(SO_SNDBUF, " + bufSize
                            + ") failed: " + ex.toString() );
                }
            }
        }
    }

    public abstract Session.Listener createSessionListener( Session session );
}
