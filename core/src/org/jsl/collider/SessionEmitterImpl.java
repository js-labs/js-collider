/*
 * Copyright (C) 2013 Sergey Zubarev, info@js-labs.org
 *
 * This file is a part of JS-Collider framework.
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

import java.net.Socket;
import java.net.SocketException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

abstract class SessionEmitterImpl
{
    protected final ColliderImpl m_collider;
    protected final RetainableDataBlockCache m_inputQueueDataBlockCache;
    private final SessionEmitter m_sessionEmitter;
    private final int m_joinMessageMaxSize;
    private final RetainableByteBufferPool m_joinPool;
    private final int m_forwardReadMaxSize;

    protected SessionEmitterImpl(
            ColliderImpl collider,
            RetainableDataBlockCache inputQueueDataBlockCache,
            SessionEmitter sessionEmitter,
            int joinMessageMaxSize,
            RetainableByteBufferPool joinPool )
    {
        m_collider = collider;
        m_inputQueueDataBlockCache = inputQueueDataBlockCache;
        m_sessionEmitter = sessionEmitter;
        m_joinMessageMaxSize = joinMessageMaxSize;
        m_joinPool = joinPool;

        m_forwardReadMaxSize =
                ((sessionEmitter.forwardReadMaxSize == 0)
                        ? collider.getConfig().forwardReadMaxSize
                        : sessionEmitter.forwardReadMaxSize);
    }

    protected final void startSession( SocketChannel socketChannel, SelectionKey selectionKey )
    {
        final int socketSendBufferSize = configureSocketChannel( socketChannel );

        final SessionImpl sessionImpl = new SessionImpl(
                m_collider, socketChannel, selectionKey, socketSendBufferSize, m_joinMessageMaxSize, m_joinPool );

        final Thread currentThread = Thread.currentThread();
        addThread( currentThread );
        final Session.Listener sessionListener = m_sessionEmitter.createSessionListener( sessionImpl );
        removeThreadAndReleaseMonitor( currentThread );

        /* Case when a sessionListener is null
         * will be handled inside the SessionImpl.initialize()
         */
        sessionImpl.initialize(
                   m_forwardReadMaxSize, m_inputQueueDataBlockCache, sessionListener );
    }

    private int configureSocketChannel( SocketChannel socketChannel )
    {
        final Socket socket = socketChannel.socket();
        try
        {
            socket.setTcpNoDelay( m_sessionEmitter.tcpNoDelay );
        }
        catch (final SocketException ex)
        {
            logException( ex );
        }

        try
        {
            socket.setReuseAddress( m_sessionEmitter.reuseAddr );
        }
        catch (final SocketException ex)
        {
            logException( ex );
        }

        int recvBufferSize = m_sessionEmitter.socketRecvBufSize;
        if (recvBufferSize == 0)
            recvBufferSize = m_collider.getConfig().socketRecvBufSize;

        if (recvBufferSize > 0)
        {
            try
            {
                socket.setReceiveBufferSize( recvBufferSize );
            }
            catch (final SocketException ex)
            {
                logException( ex );
            }
        }

        int sendBufferSize = m_sessionEmitter.socketSendBufSize;
        if (sendBufferSize == 0)
            sendBufferSize = m_collider.getConfig().socketSendBufSize;

        if (sendBufferSize > 0)
        {
            try
            {
                socket.setSendBufferSize( sendBufferSize );
            }
            catch (final SocketException ex)
            {
                logException( ex );
            }
        }
        else
            sendBufferSize = (64 * 1024);

        return sendBufferSize;
    }

    protected abstract void addThread( Thread thread );
    protected abstract void removeThreadAndReleaseMonitor( Thread thread );
    protected abstract void logException( Exception ex );

    public abstract void stopAndWait() throws InterruptedException;
}
