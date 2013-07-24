/*
 * JS-Collider framework.
 * Copyright (C) 2013 Sergey Zubarev
 * info@js-labs.org
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.jsl.collider;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;


public class SessionImpl extends Collider.SelectorThreadRunnable
        implements Session, Collider.ChannelHandler, Runnable
{
    private static final long LENGTH_MASK     = 0x000000FFFFFFFFFFL;
    private static final long CLOSED          = 0x0000010000000000L;
    private static final long CHANNEL_RC      = 0x0000100000000000L;
    private static final long CHANNEL_RC_MASK = 0x0000300000000000L;

    private Collider m_collider;
    private SocketChannel m_socketChannel;
    private SelectionKey m_selectionKey;

    private AtomicLong m_state;
    private InputQueue m_inputQueue;
    private OutputQueue m_outputQueue;
    private ByteBuffer [] m_iov;

    public SessionImpl(
            Collider collider,
            SocketChannel socketChannel,
            SelectionKey selectionKey )
    {
        Collider.Config colliderConfig = collider.getConfig();

        m_collider = collider;
        m_socketChannel = socketChannel;
        m_selectionKey = selectionKey;

        m_state = new AtomicLong( CHANNEL_RC + CHANNEL_RC );
        m_inputQueue = null;
        m_outputQueue = new OutputQueue(
                colliderConfig.outputQueueBlockSize, colliderConfig.useDirectBuffers );
        m_iov = new ByteBuffer[8];
    }

    public Collider getCollider() { return m_collider; }
    public SocketAddress getLocalAddress() { return m_socketChannel.socket().getLocalSocketAddress(); }
    public SocketAddress getRemoteAddress() { return m_socketChannel.socket().getRemoteSocketAddress(); }

    public int sendData( ByteBuffer data )
    {
        long state = m_state.get();
        if ((state & CLOSED) != 0)
            return -1;

        long bytesReady = m_outputQueue.addData( data );
        if (bytesReady > 0)
        {
            for (;;)
            {
                if ((state & CLOSED) != 0)
                    return -1;

                long newState = (state & LENGTH_MASK);
                newState += bytesReady;

                if (bytesReady > LENGTH_MASK)
                {
                    state = m_state.get();
                    continue;
                }

                newState |= (state & ~LENGTH_MASK);
                if (m_state.compareAndSet(state, newState))
                {
                    state = newState;
                    break;
                }

                state = m_state.get();
            }

            if ((state & LENGTH_MASK) == bytesReady)
                m_collider.executeInThreadPool( this );
        }

        return 0;
    }

    public int closeConnection()
    {
        long state = m_state.get();
        for (;;)
        {
            if ((state & CLOSED) != 0)
                return -1;

            assert( (state & CHANNEL_RC_MASK) > 0 );

            long newState = (state | CLOSED);
            if ((state & LENGTH_MASK) == 0)
                newState -= CHANNEL_RC;

            if (m_state.compareAndSet(state, newState))
            {
                state = newState;
                break;
            }

            state = m_state.get();
        }

        if (((state & LENGTH_MASK) == 0) &&
            ((state & CHANNEL_RC_MASK) == 0))
        {
            /* SocketChannel and SelectionKey not needed any more. */
        }

        return 0;
    }

    public void setListener( Listener listener )
    {
        if (listener == null)
        {
            long state = m_state.get();
            for (;;)
            {
                assert( (state & CHANNEL_RC_MASK) > 0 );
                long newState = (state - CHANNEL_RC);
                if (m_state.compareAndSet(state, newState))
                    break;
                state = m_state.get();
            }

            if ((state & CHANNEL_RC_MASK) == 0)
            {
                /* SocketChannel and SelectionKey not needed any more. */
                m_collider.executeInSelectorThread( null );
            }
        }
        else
        {
            m_inputQueue = new InputQueue( m_collider, m_socketChannel, listener );
        }
    }

    public void handleReadyOps( Executor executor )
    {
        int readyOps = m_selectionKey.readyOps();
        m_selectionKey.interestOps( m_selectionKey.interestOps() & ~readyOps );

        if ((readyOps & SelectionKey.OP_READ) != 0)
            executor.execute( m_inputQueue );

        if ((readyOps & SelectionKey.OP_WRITE) != 0)
            executor.execute( this );
    }

    public void runInSelectorThread()
    {
        int interestOps = m_selectionKey.interestOps();
        m_selectionKey.interestOps( interestOps | SelectionKey.OP_WRITE );
    }

    public void run()
    {
        long state = m_state.get();
        long bytesReady = (state & LENGTH_MASK);
        bytesReady = m_outputQueue.getData( m_iov, bytesReady );
        int pos0 = m_iov[0].position();

        int iovc = 0;
        for (; iovc<m_iov.length && m_iov[iovc]!= null; iovc++);

        long bytesSent = 0;
        try
        {
            bytesSent = m_socketChannel.write( m_iov, 0, iovc );
            for (int idx=0; idx<iovc; idx++)
                m_iov[idx] = null;
        }
        catch (IOException ignored)
        {
            return;
        }

        m_outputQueue.removeData( pos0, bytesSent );
        state = m_state.addAndGet( -bytesSent );

        if (bytesSent < bytesReady)
            m_collider.executeInSelectorThread( this );
        else if ((state & LENGTH_MASK) != 0)
            m_collider.executeInThreadPool( this );
    }
}
