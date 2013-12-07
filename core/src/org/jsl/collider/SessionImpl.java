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
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;


public class SessionImpl extends ThreadPool.Runnable
        implements Session, ColliderImpl.ChannelHandler, SocketChannelReader.StateListener
{
    private static final Logger s_logger = Logger.getLogger( "org.jsl.collider.Session" );
    private static final Node CLOSE_MARKER  = new Node();

    private static final int STATE_MASK   = 0x0003;
    private static final int ST_STARTING  = 0x0000;
    private static final int ST_RUNNING   = 0x0001;
    private static final int SOCK_RC_MASK = 0x0030;
    private static final int SOCK_RC      = 0x0010;
    private static final int CLOSE        = 0x0100;

    private final ColliderImpl m_collider;
    private SocketChannel m_socketChannel;
    private SelectionKey m_selectionKey;

    private final SocketAddress m_localSocketAddress;
    private final SocketAddress m_remoteSocketAddress;

    private final Starter m_starter;
    private final AtomicInteger m_state;

    private Node m_head;
    private final AtomicReference<Node> m_tail;
    private final ByteBuffer [] m_iov;
    private int m_iovc;

    private SocketChannelReader m_socketChannelReader;

    private static class DummyListener implements Listener
    {
        public void onDataReceived( ByteBuffer data ) { }
        public void onConnectionClosed() { }
    }

    private class SelectorDeregistrator extends ColliderImpl.SelectorThreadRunnable
    {
        public void runInSelectorThread()
        {
            if (s_logger.isLoggable(Level.FINE))
                s_logger.fine( m_remoteSocketAddress.toString() );

            m_selectionKey.cancel();
            m_selectionKey = null;

            try
            {
                m_socketChannel.close();
            }
            catch (IOException ex)
            {
                if (s_logger.isLoggable(Level.WARNING))
                    s_logger.warning( m_remoteSocketAddress.toString() + ": " + ex.toString() );
            }
            m_socketChannel = null;
        }
    }

    private class Starter extends ColliderImpl.SelectorThreadRunnable
    {
        public void runInSelectorThread()
        {
            int interestOps = m_selectionKey.interestOps();
            interestOps |= SelectionKey.OP_WRITE;
            m_selectionKey.interestOps( interestOps );
        }
    }

    private static class Node
    {
        public volatile Node next;
        public final ByteBuffer buf;
        public final CachedByteBuffer cachedBuf;

        public Node()
        {
            this.buf = null;
            this.cachedBuf = null;
        }

        public Node( ByteBuffer buf )
        {
            this.buf = buf;
            this.cachedBuf = null;
        }

        public Node( ByteBuffer buf, CachedByteBuffer cachedBuf )
        {
            this.buf = buf;
            this.cachedBuf = cachedBuf;
        }
    }

    private static String stateToString( int state )
    {
        String ret = "[";
        if ((state & CLOSE) != 0)
            ret += "CLOSE ";

        long sockRC = (state & SOCK_RC_MASK);

        state &= STATE_MASK;
        if (state == ST_STARTING)
            ret += "STARTING ";
        else if (state == ST_RUNNING)
            ret += "RUNNING ";
        else
            ret += "??? ";

        sockRC /= SOCK_RC;
        ret += "RC=" + sockRC + "]";
        return ret;
    }

    /* SocketChannelReader.StateListener interface implementation */

    public void handleReaderStopped()
    {
        Node tail = m_tail.get();
        for (;;)
        {
            if (tail == CLOSE_MARKER)
                break;

            if (m_tail.compareAndSet(tail, CLOSE_MARKER))
            {
                if (tail != null)
                    tail.next = CLOSE_MARKER;
                break;
            }
            tail = m_tail.get();
        }

        for (;;)
        {
            int state = m_state.get();
            assert( (state & STATE_MASK) == ST_RUNNING );
            assert( (state & SOCK_RC_MASK) > 0 );

            int newState = (state | CLOSE);
            newState -= SOCK_RC;

            if (tail == null)
                newState -= SOCK_RC;

            if (m_state.compareAndSet(state, newState))
            {
                if (s_logger.isLoggable(Level.FINER))
                {
                    s_logger.finer(
                            m_remoteSocketAddress.toString() +
                            ": " + stateToString(state) + " -> " + stateToString(newState) );
                }

                if ((newState & SOCK_RC_MASK) == 0)
                    m_collider.executeInSelectorThread( new SelectorDeregistrator() );
                break;
            }
        }
    }

    public String getPeerInfo()
    {
        return m_remoteSocketAddress.toString();
    }

    public SessionImpl(
                ColliderImpl collider,
                SocketChannel socketChannel,
                SelectionKey selectionKey )
    {
        m_collider = collider;
        m_socketChannel = socketChannel;
        m_selectionKey = selectionKey;
        m_localSocketAddress = socketChannel.socket().getLocalSocketAddress();
        m_remoteSocketAddress = socketChannel.socket().getRemoteSocketAddress();

        m_starter = new Starter();
        m_state = new AtomicInteger( ST_STARTING + SOCK_RC + SOCK_RC );
        m_head = null;
        m_tail = new AtomicReference<Node>();
        m_iov = new ByteBuffer[32];
        m_iovc = 0;

        m_selectionKey.attach( this );
    }

    public final void initialize(
            int inputQueueMaxSize,
            DataBlockCache inputQueueDataBlockCache,
            Listener listener )
    {
        if (listener == null)
            listener = new DummyListener();

        m_socketChannelReader = new SocketChannelReader(
                m_collider,
                inputQueueMaxSize,
                inputQueueDataBlockCache,
                m_socketChannel,
                m_selectionKey,
                this,
                listener );

        int state = m_state.get();
        int newState;
        for (;;)
        {
            assert( (state & STATE_MASK) == ST_STARTING );

            newState = state;

            if ((state & CLOSE) == 0)
            {
                newState &= ~STATE_MASK;
                newState |= ST_RUNNING;
                if (m_state.compareAndSet(state, newState))
                {
                    m_socketChannelReader.start();
                    break;
                }
            }
            else
            {
                newState -= SOCK_RC;
                if (m_state.compareAndSet(state, newState))
                {
                    listener.onConnectionClosed();
                    if ((newState & SOCK_RC_MASK) == 0)
                        m_collider.executeInSelectorThread( new SelectorDeregistrator() );
                    break;
                }
            }
            state = m_state.get();
        }

        if (s_logger.isLoggable(Level.FINE))
        {
            s_logger.fine(
                    m_remoteSocketAddress.toString() +
                    ": " + stateToString(state) + " -> " + stateToString(newState) + "." );
        }
    }

    public Collider getCollider() { return m_collider; }
    public SocketAddress getLocalAddress() { return m_localSocketAddress; }
    public SocketAddress getRemoteAddress() { return m_remoteSocketAddress; }

    public int sendData( ByteBuffer data )
    {
        final Node node = new Node( data );
        for (;;)
        {
            final Node tail = m_tail.get();
            if (tail == CLOSE_MARKER)
                return -1;

            if (m_tail.compareAndSet(tail, node))
            {
                if (tail == null)
                {
                    m_head = node;
                    m_collider.executeInThreadPool( this );
                }
                else
                    tail.next = node;
                return 1;
            }
        }
    }

    public int sendData( CachedByteBuffer data )
    {
        final Node node = new Node( data.getByteBuffer(), data );
        for (;;)
        {
            final Node tail = m_tail.get();
            if (tail == CLOSE_MARKER)
                return -1;

            if (m_tail.compareAndSet(tail, node))
            {
                data.retain();
                if (tail == null)
                {
                    m_head = node;
                    m_collider.executeInThreadPool( this );
                }
                else
                    tail.next = node;
                return 1;
            }
        }
    }

    public int sendDataSync( ByteBuffer data )
    {
        final Node node = new Node( data );
        for (;;)
        {
            final Node tail = m_tail.get();
            if (tail == CLOSE_MARKER)
                return -1;

            if (m_tail.compareAndSet(tail, node))
            {
                if (tail == null)
                {
                    m_head = node;
                    break;
                }
                else
                {
                    tail.next = node;
                    return 1;
                }
            }
        }

        try
        {
            m_socketChannel.write( data );
        }
        catch (IOException ex)
        {
            handleWriteException( ex );
            return -1;
        }

        if (data.remaining() > 0)
        {
            m_collider.executeInSelectorThread( m_starter );
            return 1;
        }

        removeNode( node );
        return 0;
    }

    public int closeConnection()
    {
        Node tail = m_tail.get();
        for (;;)
        {
            if (tail == CLOSE_MARKER)
                return -1;

            if (m_tail.compareAndSet(tail, CLOSE_MARKER))
            {
                if (tail != null)
                    tail.next = CLOSE_MARKER;
                break;
            }
            tail = m_tail.get();
        }

        m_socketChannelReader.stop();

        if (tail == null)
            releaseSocket( "closeConnection()" );

        return 0;
    }

    public void handleReadyOps( ThreadPool threadPool )
    {
        final int readyOps = m_selectionKey.readyOps();
        m_selectionKey.interestOps( m_selectionKey.interestOps() & ~readyOps );

        if ((readyOps & SelectionKey.OP_READ) != 0)
            threadPool.execute( m_socketChannelReader );

        if ((readyOps & SelectionKey.OP_WRITE) != 0)
            threadPool.execute( this );
    }

    public void runInThreadPool()
    {
        Node node = m_head;
        int idx = 0;
        for (; idx<m_iovc; idx++)
            node = node.next;

        for (;;)
        {
            if (m_iovc == m_iov.length)
                break;
            if ((node == null) || (node == CLOSE_MARKER))
                break;
            assert( m_iov[m_iovc] == null );
            m_iov[m_iovc] = node.buf.duplicate();
            m_iovc++;
            node = node.next;
        }

        try
        {
            final long bytesSent = m_socketChannel.write( m_iov, 0, m_iovc );
            if (bytesSent == 0)
            {
                m_collider.executeInSelectorThread( m_starter );
                return;
            }
        }
        catch (IOException ex)
        {
            handleWriteException( ex );
            return;
        }

        idx = 0;
        node = m_head;
        for (;;)
        {
            if (m_iov[idx].remaining() > 0)
            {
                final int cc = (m_iovc - idx);
                int jj = 0;
                for (; jj<cc; jj++)
                {
                    m_iov[jj] = m_iov[jj+idx];
                    m_iov[jj+idx] = null;
                }
                m_iovc = cc;
                m_head = node;
                m_collider.executeInThreadPool( this );
                return;
            }

            if (node.cachedBuf != null)
                node.cachedBuf.release();

            m_iov[idx] = null;
            if  (++idx == m_iovc)
                break;

            Node next = node.next;
            node.next = null;
            node = next;
        }

        m_iovc = 0;
        removeNode( node );
    }

    private void handleWriteException( final IOException ex )
    {
        /* Session can be already closed, but can be not.
         * Let's clean up and close output queue,
         * input queue will be closed soon as well.
         */
        for (;;)
        {
            final Node tail = m_tail.get();
            assert( tail != null );

            if (tail == CLOSE_MARKER)
                break; /* already closed */

            if (m_tail.compareAndSet(tail, CLOSE_MARKER))
            {
                tail.next = CLOSE_MARKER;
                break;
            }
        }

        Node node = m_head;
        while (node != CLOSE_MARKER)
        {
            Node next = node.next;
            node.next = null;
            node = next;
        }
        m_head = node;

        releaseSocket( ex.toString() );
    }

    private void releaseSocket( final String hint )
    {
        for (;;)
        {
            final int state = m_state.get();
            assert( (state & SOCK_RC_MASK) > 0 );
            final int newState = (state - SOCK_RC);
            if (m_state.compareAndSet(state, newState))
            {
                if (s_logger.isLoggable(Level.FINER))
                {
                    s_logger.finer(
                            m_remoteSocketAddress.toString() +
                            ": " + hint + " "
                            + stateToString(state) + " -> " + stateToString(newState) + "." );
                }
                if ((newState & SOCK_RC_MASK) == 0)
                    m_collider.executeInSelectorThread( new SelectorDeregistrator() );
                break;
            }
        }
    }

    private void removeNode( Node node )
    {
        Node next = node.next;
        if (next == null)
        {
            m_head = null;
            if (!m_tail.compareAndSet(node, null))
            {
                while (node.next == null);
                m_head = node.next;
                node.next = null;
                if (m_head == CLOSE_MARKER)
                    releaseSocket( "removeNode(CAS failed)" );
                else
                    m_collider.executeInThreadPool( this );
            }
        }
        else
        {
            node.next = null;
            m_head = next;
            if (m_head == CLOSE_MARKER)
                releaseSocket( "removeNode" );
            else
                m_collider.executeInThreadPool( this );
        }
    }
}
