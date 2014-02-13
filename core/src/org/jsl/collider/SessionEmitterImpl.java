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

public abstract class SessionEmitterImpl
{
    protected final ColliderImpl m_collider;
    protected final DataBlockCache m_inputQueueDataBlockCache;
    private final SessionEmitter m_sessionEmitter;

    protected SessionEmitterImpl(
            ColliderImpl collider,
            DataBlockCache inputQueueDataBlockCache,
            SessionEmitter sessionEmitter )
    {
        m_collider = collider;
        m_inputQueueDataBlockCache = inputQueueDataBlockCache;
        m_sessionEmitter = sessionEmitter;

        if (sessionEmitter.forwardReadMaxSize == 0)
            sessionEmitter.forwardReadMaxSize = collider.getConfig().forwardReadMaxSize;
    }

    protected final void startSession( SocketChannel socketChannel, SelectionKey selectionKey )
    {
        configureSocketChannel( socketChannel );

        final Thread currentThread = Thread.currentThread();
        final SessionImpl sessionImpl = new SessionImpl( m_collider, socketChannel, selectionKey );

        addThread( currentThread );
        final Session.Listener sessionListener = m_sessionEmitter.createSessionListener( sessionImpl );
        removeThreadAndReleaseMonitor( currentThread );

        /* Case when a sessionListener is null
         * will be handled in the SessionImpl.initialize()
         */
        sessionImpl.initialize(
                   m_sessionEmitter.forwardReadMaxSize,
                   m_inputQueueDataBlockCache,
                   sessionListener );
    }

    private void configureSocketChannel( SocketChannel socketChannel )
    {
        final Socket socket = socketChannel.socket();
        try
        {
            socket.setTcpNoDelay( m_sessionEmitter.tcpNoDelay );
        }
        catch (SocketException ex)
        {
            logException( ex );
        }

        try
        {
            socket.setReuseAddress( m_sessionEmitter.reuseAddr );
        }
        catch (SocketException ex)
        {
            logException( ex );
        }

        int bufSize = m_sessionEmitter.socketRecvBufSize;
        if (bufSize == 0)
            bufSize = m_collider.getConfig().socketRecvBufSize;

        if (bufSize > 0)
        {
            try
            {
                socket.setReceiveBufferSize( bufSize );
            }
            catch (SocketException ex)
            {
                logException( ex );
            }
        }

        bufSize = m_sessionEmitter.socketSendBufSize;
        if (bufSize == 0)
            bufSize = m_collider.getConfig().socketSendBufSize;

        if (bufSize > 0)
        {
            try
            {
                socket.setSendBufferSize( bufSize );
            }
            catch (SocketException ex)
            {
                logException( ex );
            }
        }
    }

    protected abstract void addThread( Thread thread );
    protected abstract void removeThreadAndReleaseMonitor( Thread thread );
    protected abstract void logException( Exception ex );

    public abstract void stopAndWait() throws InterruptedException;
}
