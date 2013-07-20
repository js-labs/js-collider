/*
 * JS-Collider framework.
 * Copyright (C) 2013 Sergey Zubarev
 * info@js-labs.com
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

import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.concurrent.Executor;


public class SessionImpl extends Collider.SelectorThreadRunnable
        implements Session, Collider.ChannelHandler
{
    private Collider m_collider;
    private SocketChannel m_socketChannel;
    private SelectionKey m_selectionKey;
    private InputQueue m_inputQueue;
    private OutputQueue m_outputQueue;

    public SessionImpl(
            Collider collider,
            SocketChannel socketChannel,
            SelectionKey selectionKey )
    {
        m_collider = collider;
        m_socketChannel = socketChannel;
        m_selectionKey = selectionKey;
        m_outputQueue = new OutputQueue();
    }

    public Collider getCollider() { return m_collider; }
    public SocketAddress getLocalAddress() { return m_socketChannel.socket().getLocalSocketAddress(); }
    public SocketAddress getRemoteAddress() { return m_socketChannel.socket().getRemoteSocketAddress(); }

    public int sendData( ByteBuffer data )
    {
        return 0;
    }

    public int closeConnection( int flags )
    {
        return 0;
    }

    public void setListener( Listener listener )
    {
        m_inputQueue = new InputQueue( m_collider, m_socketChannel, listener );
    }

    public void handleReadyOps( Executor executor )
    {
        int readyOps = m_selectionKey.readyOps();
        m_selectionKey.interestOps( m_selectionKey.interestOps() & ~readyOps );

        if ((readyOps & SelectionKey.OP_READ) != 0)
            executor.execute( m_inputQueue );

        if ((readyOps & SelectionKey.OP_WRITE) != 0)
            executor.execute( m_outputQueue );
    }

    public void runInSelectorThread()
    {
        int interestOps = m_selectionKey.interestOps();
        interestOps |= SelectionKey.OP_READ;
        m_selectionKey.interestOps( interestOps );
    }
}
