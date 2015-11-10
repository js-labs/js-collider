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

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.nio.channels.NotYetConnectedException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.logging.Level;
import java.util.logging.Logger;


class SessionImpl implements Session, ColliderImpl.ChannelHandler
{
    private static final Logger s_logger = Logger.getLogger( "org.jsl.collider.Session" );
    private static final Node CLOSE_MARKER = new Node( (ByteBuffer) null );

    private static final AtomicReferenceFieldUpdater<Node, Node> s_nodeNextUpdater =
            AtomicReferenceFieldUpdater.newUpdater( Node.class, Node.class, "next" );

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

    private SocketChannelReader m_socketChannelReader;
    private ThreadPool.Runnable m_writer;

    private class SelectorDeregistrator extends ColliderImpl.SelectorThreadRunnable
    {
        public int runInSelectorThread()
        {
            if (s_logger.isLoggable(Level.FINE))
                s_logger.fine( m_localSocketAddress + " -> " + m_remoteSocketAddress );

            m_selectionKey.cancel();
            m_selectionKey = null;

            try
            {
                m_socketChannel.close();
            }
            catch (final IOException ex)
            {
                if (s_logger.isLoggable(Level.WARNING))
                {
                    s_logger.warning(
                            m_localSocketAddress + " -> " + m_remoteSocketAddress.toString() +
                            ": " + ex.toString());
                }
            }
            m_socketChannel = null;
            return 0;
        }
    }

    private class Starter extends ColliderImpl.SelectorThreadRunnable
    {
        public int runInSelectorThread()
        {
            int interestOps = m_selectionKey.interestOps();
            interestOps |= SelectionKey.OP_WRITE;
            m_selectionKey.interestOps( interestOps );
            return 0;
        }
    }

    private static class Node
    {
        public volatile Node next;
        public ByteBuffer buf;
        public RetainableByteBuffer rbuf;

        public Node( ByteBuffer buf )
        {
            this.buf = buf;
            this.rbuf = null;
        }

        public Node( RetainableByteBuffer rbuf )
        {
            this.buf = rbuf.getNioByteBuffer();
            this.rbuf = rbuf;
            rbuf.retain();
        }
    }

    private class SocketWriter extends ThreadPool.Runnable
    {
        private final int m_socketSendBufferSize;
        private final int m_joinMessageMaxSize;
        private final RetainableByteBufferPool m_pool;
        private final ByteBuffer [] m_iov;
        private int m_iovc;

        private void joinMessages()
        {
            int bytesReady = 0;
            Node prev = null;
            Node node = m_head;
            for (int idx=0; idx<m_iovc; idx++)
            {
                bytesReady += m_iov[idx].remaining();
                prev = node;
                node = node.next;
            }

            for (;;)
            {
                if (m_iovc == m_iov.length)
                    break;
                if ((node == null) || (node == CLOSE_MARKER))
                    break;
                assert( m_iov[m_iovc] == null );

                int joinNodes = 0;
                int joinBytes = 0;
                for (Node nn=node;;)
                {
                    final int nodeBytes = nn.buf.remaining();
                    if (nodeBytes >= m_joinMessageMaxSize)
                        break;
                    joinNodes++;
                    joinBytes += nodeBytes;
                    if ((bytesReady + joinBytes) > m_socketSendBufferSize)
                        break;
                    nn = nn.next;
                    if ((nn == null) || (nn == CLOSE_MARKER))
                        break;
                }

                if (joinNodes > 1)
                {
                    final RetainableByteBuffer buf = m_pool.alloc( joinBytes, m_joinMessageMaxSize*2 );
                    int space = buf.remaining();
                    int nodeBytes = node.buf.remaining();
                    for (;;)
                    {
                        assert( space >= nodeBytes );

                        buf.put( node.buf.duplicate() );
                        node.buf = null;

                        if (node.rbuf != null)
                        {
                            node.rbuf.release();
                            node.rbuf = null;
                        }

                        if (--joinNodes == 0)
                            break;

                        final Node next = node.next;
                        assert( (next != null) && (next != CLOSE_MARKER) );

                        space -= nodeBytes;
                        nodeBytes = next.buf.remaining();
                        if (space < nodeBytes)
                            break;

                        s_nodeNextUpdater.lazySet( node, null );
                        node = next;
                    }

                    assert( node.buf == null );
                    assert( node.rbuf == null );

                    buf.flip();
                    node.buf = buf.getNioByteBuffer();
                    node.rbuf = buf;

                    if (prev == null)
                        m_head = node;
                    else
                        prev.next = node;

                    m_iov[m_iovc] = node.buf;
                    m_iovc++;
                }
                else
                {
                    m_iov[m_iovc] = node.buf.duplicate();
                    m_iovc++;
                }

                bytesReady += node.buf.remaining();
                if (bytesReady > m_socketSendBufferSize)
                    break;

                prev = node;
                node = node.next;
            }
        }

        public SocketWriter(
                int socketSendBufferSize,
                int joinMessageMaxSize,
                RetainableByteBufferPool pool )
        {
            /* It makes no sense to write at once
             * significantly more than socket send buffer size.
             */
            m_socketSendBufferSize = socketSendBufferSize;
            m_joinMessageMaxSize = joinMessageMaxSize;
            m_pool = pool;
            m_iov = new ByteBuffer[32];
            m_iovc = 0;
        }

        public void runInThreadPool()
        {
            if (m_joinMessageMaxSize == 0)
            {
                Node node = m_head;
                for (int idx=0; idx<m_iovc; idx++)
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
            }
            else
                joinMessages();

            try
            {
                final long bytesSent = m_socketChannel.write( m_iov, 0, m_iovc );
                if (bytesSent == 0)
                {
                    m_collider.executeInSelectorThread( m_starter );
                    return;
                }
            }
            catch (final IOException ex)
            {
                closeAndCleanupQueue( ex );
                releaseSocket( "SocketWriter");
                return;
            }
            catch (final NotYetConnectedException ex)
            {
                closeAndCleanupQueue( ex );
                releaseSocket( "SocketWriter" );
                return;
            }

            Node node = m_head;
            for (int idx=0;;)
            {
                if (m_iov[idx].remaining() > 0)
                {
                    final int iovc = (m_iovc - idx);
                    int cc = 0;
                    for (; idx<m_iovc; idx++, cc++)
                        m_iov[cc] = m_iov[idx];
                    for (; cc<m_iovc; cc++)
                        m_iov[cc] = null;
                    m_iovc = iovc;
                    m_head = node;
                    m_collider.executeInThreadPool( this );
                    return;
                }

                node.buf = null;
                if (node.rbuf != null)
                {
                    node.rbuf.release();
                    node.rbuf = null;
                }

                m_iov[idx] = null;
                if  (++idx == m_iovc)
                    break;

                final Node next = node.next;
                s_nodeNextUpdater.lazySet( node, null );
                node = next;
            }

            m_iovc = 0;
            final Node next = node.next;
            if (next == null)
            {
                m_head = null;
                if (!m_tail.compareAndSet(node, null))
                {
                    while (node.next == null);
                    m_head = node.next;
                    s_nodeNextUpdater.lazySet( node, null );
                    if (m_head == CLOSE_MARKER)
                        releaseSocket( "SocketWriter.runInThreadPool()" );
                    else
                        m_collider.executeInThreadPool( m_writer );
                }
            }
            else
            {
                s_nodeNextUpdater.lazySet( node, null );
                m_head = next;
                if (m_head == CLOSE_MARKER)
                    releaseSocket( "SocketWriter.runInThreadPool()" );
                else
                    m_collider.executeInThreadPool( m_writer );
            }
        }
    }

    private class ShMemWriter extends ThreadPool.Runnable
    {
        private final ShMem.ChannelOut m_shm;
        private final int m_batchMaxSize;
        private final ByteBuffer m_buf;

        public ShMemWriter( ShMem.ChannelOut shm, int batchMaxSize )
        {
            m_shm = shm;
            m_batchMaxSize = batchMaxSize;
            m_buf = ByteBuffer.allocateDirect( 64 ); // Do not really need too much.
        }

        public void runInThreadPool()
        {
            /* m_buf can contain some data was not sent last time,
             * m_head should never be null.
             */
            assert( m_head != null );

            final int pos = m_buf.position();
            Node node = m_head;
            int msgs;

            if (pos == 0)
            {
                /* First message should be sent as soon as possible. */
                msgs = 1;
            }
            else
            {
                msgs = Integer.MAX_VALUE;

                if (node.buf == null)
                {
                    final Node next = node.next;
                    if ((next == null) || (next == CLOSE_MARKER))
                    {
                        try
                        {
                            m_socketChannel.write( m_buf );
                        }
                        catch (Exception ex)
                        {
                            closeAndCleanupQueue( ex );
                            releaseSocket( "ShMemWriter1" );
                        }

                        if (m_buf.remaining() > 0)
                        {
                            /* Probably can happen. */
                            m_collider.executeInSelectorThread( m_starter );
                        }
                        else
                        {
                            if (next == CLOSE_MARKER)
                            {
                                s_nodeNextUpdater.lazySet( node, null );
                                m_head = next;
                                releaseSocket( "ShMemWriter3" );
                            }
                        }
                        return;
                    }

                    s_nodeNextUpdater.lazySet( node, null );
                    node = next;
                }
            }

            boolean breakLoop = false;
            int bytesSent = 0;

            for (;;)
            {
                int bytesReady = 0;
                for (int idx=msgs;;)
                {
                    final int length = m_shm.addData( node.buf.duplicate() );
                    if (length < 0)
                    {
                        /* Only one possible reason:
                         * we failed to map block of the shared memory file.
                         * Unfortunately no chance to recover, close a connection.
                         */
                        m_head = node;
                        closeAndCleanupQueue( null );
                        m_socketChannelReader.stop();
                        releaseSocket( "ShMemWriter4" );
                        return;
                    }

                    node.buf = null;
                    if (node.rbuf != null)
                    {
                        node.rbuf.release();
                        node.rbuf = null;
                    }

                    bytesReady += length;
                    bytesSent += length;

                    if (--idx == 0)
                        break;

                    if (bytesSent >= m_batchMaxSize)
                    {
                        breakLoop = true;
                        break;
                    }

                    final Node next = node.next;
                    if ((next == null) || (next == CLOSE_MARKER))
                    {
                        breakLoop = true;
                        break;
                    }

                    s_nodeNextUpdater.lazySet( node, null );
                    node = next;
                }

                m_buf.putInt( bytesReady );
                m_buf.flip();

                try
                {
                    m_socketChannel.write( m_buf );
                }
                catch (final Exception ex)
                {
                    closeAndCleanupQueue( ex );
                    releaseSocket( "ShMemWriter5" );
                    return;
                }

                if (m_buf.remaining() > 0)
                {
                    /* Socket send buffer overflowed. */
                    if (s_logger.isLoggable(Level.FINER))
                        s_logger.finer( m_remoteSocketAddress + ": m_buf.remaining()=" + m_buf.remaining() + "." );

                    final ByteBuffer dup = m_buf.duplicate();
                    m_buf.clear();
                    m_buf.put( dup );

                   /* Now we have to wait while socket become writable,
                    * it is important do not remove the latest node
                    * to avoid scheduling the session for writing again.
                    */
                    final Node next = node.next;
                    if (next == null)
                        m_head = node;
                    else
                    {
                        s_nodeNextUpdater.lazySet( node, null );
                        m_head = next;
                    }

                    m_collider.executeInSelectorThread( m_starter );
                    return;
                }

                m_buf.clear();

                if (breakLoop)
                    break;

                final Node next = node.next;
                if ((next == null) || (next == CLOSE_MARKER))
                    break;

                s_nodeNextUpdater.lazySet( node, null );
                node = next;
                msgs *= 2;
            }

            removeNode( node );
        }
    }

    private static String stateToString( int state )
    {
        String ret = "[";

        final int st = (state & STATE_MASK);
        if (st == ST_STARTING)
            ret += "STARTING ";
        else if (st == ST_RUNNING)
            ret += "RUNNING ";
        else
            ret += "??? ";

        if ((state & CLOSE) != 0)
            ret += "CLOSE ";

        int sockRC = (state & SOCK_RC_MASK);
        sockRC /= SOCK_RC;
        ret += "RC=" + sockRC + "]";
        return ret;
    }

    public final void handleReaderStopped()
    {
        Node tail = m_tail.get();
        for (;;)
        {
            if (tail == CLOSE_MARKER)
                break;

            if (m_tail.compareAndSet(tail, CLOSE_MARKER))
            {
                if (tail == null)
                    m_head = CLOSE_MARKER;
                else
                    tail.next = CLOSE_MARKER;
                break;
            }
            tail = m_tail.get();
        }

        for (;;)
        {
            final int state = m_state.get();
            assert( (state & STATE_MASK) == ST_RUNNING );
            assert( (state & SOCK_RC_MASK) > 0 );
            assert( (state & CLOSE) == 0 );

            int newState = (state - SOCK_RC);
            if (tail == null)
            {
                assert( (newState & SOCK_RC_MASK) > 0 );
                newState -= SOCK_RC;
            }

            if (m_state.compareAndSet(state, newState))
            {
                if (s_logger.isLoggable(Level.FINER))
                {
                    s_logger.finer(
                            m_localSocketAddress + " -> " + m_remoteSocketAddress +
                            ": " + stateToString(state) + " -> " + stateToString(newState) );
                }

                if ((newState & SOCK_RC_MASK) == 0)
                    m_collider.executeInSelectorThread( new SelectorDeregistrator() );
                break;
            }
        }
    }

    public final void handleReaderStoppedST()
    {
        assert( m_tail.get() == CLOSE_MARKER );
        for (;;)
        {
            final int state = m_state.get();
            assert( (state & STATE_MASK) == ST_RUNNING );
            assert( (state & SOCK_RC_MASK) > 0 );
            assert( (state & CLOSE) == 0 );

            final int newState = (state - SOCK_RC);
            if (m_state.compareAndSet(state, newState))
            {
                if (s_logger.isLoggable(Level.FINER))
                {
                    s_logger.finer(
                            m_localSocketAddress + " -> " + m_remoteSocketAddress +
                            ": " + stateToString(state) + " -> " + stateToString(newState) );
                }

                if ((newState & SOCK_RC_MASK) == 0)
                {
                    m_selectionKey.cancel();
                    m_selectionKey = null;

                    try
                    {
                        m_socketChannel.close();
                    }
                    catch (final IOException ex)
                    {
                        if (s_logger.isLoggable(Level.WARNING))
                        {
                            s_logger.warning(
                                    m_localSocketAddress + " -> " + m_remoteSocketAddress +
                                    ": " + ex.toString() );
                        }
                    }
                    m_socketChannel = null;
                }

                break;
            }
        }
    }

    public SessionImpl(
                ColliderImpl collider,
                SocketChannel socketChannel,
                SelectionKey selectionKey,
                int socketSendBufferSize,
                int joinMessageMaxSize,
                RetainableByteBufferPool joinPool )
    {
        m_collider = collider;
        m_socketChannel = socketChannel;
        m_selectionKey = selectionKey;
        m_localSocketAddress = socketChannel.socket().getLocalSocketAddress();
        m_remoteSocketAddress = socketChannel.socket().getRemoteSocketAddress();

        m_starter = new Starter();
        m_state = new AtomicInteger( ST_STARTING + SOCK_RC );
        m_head = null;
        m_tail = new AtomicReference<Node>();
        m_writer = new SocketWriter( socketSendBufferSize, joinMessageMaxSize, joinPool );

        m_selectionKey.attach( this );
    }

    public final void initialize(
                int inputQueueMaxSize,
                RetainableDataBlockCache inputQueueDataBlockCache,
                Listener listener )
    {
        if (listener == null)
            closeConnection();
        else
        {
            /* m_socketChannelReader should be initialized before state change
             * to the ST_RUNNING, closeConnection() method relies on it.
             */
            m_socketChannelReader = new SocketChannelReader(
                    m_collider,
                    this,
                    inputQueueMaxSize,
                    inputQueueDataBlockCache,
                    m_socketChannel,
                    m_selectionKey,
                    listener );
        }

        for (;;)
        {
            final int state = m_state.get();
            assert( (state & STATE_MASK) == ST_STARTING );
            if ((state & CLOSE) == 0)
            {
                assert( (state & SOCK_RC_MASK) == SOCK_RC );
                int newState = state;
                newState &= ~STATE_MASK;
                newState |= ST_RUNNING;
                newState += SOCK_RC;
                if (m_state.compareAndSet(state, newState))
                {
                    if (s_logger.isLoggable(Level.FINE))
                    {
                        s_logger.fine(
                                m_localSocketAddress + " -> " + m_remoteSocketAddress +
                                ": " + stateToString(state) + " -> " + stateToString(newState) + "." );
                    }
                    m_socketChannelReader.start();
                    break;
                }
            }
            else
            {
                if (s_logger.isLoggable(Level.FINE))
                {
                    s_logger.fine(
                            m_localSocketAddress + " -> " + m_remoteSocketAddress +
                            ": " + stateToString(state) + "." );
                }

                if (m_socketChannelReader != null)
                {
                    /* listener != null */
                    m_socketChannelReader.reset();
                }

                break;
            }
        }
    }

    public Collider getCollider() { return m_collider; }
    public SocketAddress getLocalAddress() { return m_localSocketAddress; }
    public SocketAddress getRemoteAddress() { return m_remoteSocketAddress; }

    public int sendData( ByteBuffer data )
    {
        assert( data.remaining() > 0 );
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
                    m_collider.executeInThreadPool( m_writer );
                }
                else
                    tail.next = node;
                return 1;
            }
        }
    }

    public int sendData( RetainableByteBuffer data )
    {
        assert( data.remaining() > 0 );
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
                    m_collider.executeInThreadPool( m_writer );
                }
                else
                    tail.next = node;
                return 1;
            }
        }
    }

    public int sendDataSync( ByteBuffer data )
    {
        assert( data.remaining() > 0 );
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
        catch (final Exception ex)
        {
            closeAndCleanupQueue( ex );
            releaseSocket( "sendDataSync()" );
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
        for (;;)
        {
            final Node tail = m_tail.get();
            if (tail == CLOSE_MARKER)
                return -1;

            if (m_tail.compareAndSet(tail, CLOSE_MARKER))
            {
                if (tail == null)
                {
                    m_head = CLOSE_MARKER;
                    for (;;)
                    {
                        final int state = m_state.get();
                        if ((state & STATE_MASK) == ST_STARTING)
                        {
                            assert( (state & SOCK_RC_MASK) == SOCK_RC );
                            final int newState = ((state | CLOSE) - SOCK_RC);
                            if (m_state.compareAndSet(state, newState))
                            {
                                s_logger.finer(
                                        m_localSocketAddress + " -> " + m_remoteSocketAddress +
                                        ": " + stateToString(state) + " -> " + stateToString(newState) + " tail==null" );
                                /* It would seem worth in this case to close socket channel
                                 * in the SessionImpl.initialize(), but leaving SOCK_RC here
                                 * means there are some data is being writing to the socket,
                                 * what is wrong.
                                 */
                                m_collider.executeInSelectorThread( new SelectorDeregistrator() );
                                break;
                            }
                        }
                        else
                        {
                            assert( (state & STATE_MASK) == ST_RUNNING );
                            assert( (state & SOCK_RC_MASK) > 0 );
                            final int newState = (state - SOCK_RC);
                            if (m_state.compareAndSet(state, newState))
                            {
                                s_logger.finer(
                                        m_localSocketAddress + " -> " + m_remoteSocketAddress +
                                        ": " + stateToString(state) + " -> " + stateToString(newState) + " tail==null" );

                                m_socketChannelReader.stop();

                                if ((newState & SOCK_RC_MASK) == 0)
                                    m_collider.executeInSelectorThread( new SelectorDeregistrator() );

                                break;
                            }
                        }
                    }
                }
                else
                {
                    tail.next = CLOSE_MARKER;
                    for (;;)
                    {
                        final int state = m_state.get();
                        if ((state & STATE_MASK) == ST_STARTING)
                        {
                            assert( (state & SOCK_RC_MASK) == SOCK_RC );
                            final int newState = (state | CLOSE);
                            if (m_state.compareAndSet(state, newState))
                            {
                                s_logger.finer(
                                        m_localSocketAddress + " -> " + m_remoteSocketAddress +
                                        ": " + stateToString(state) + " -> " + stateToString(newState) + " tail!=null" );
                                break;
                            }
                        }
                        else
                        {
                            assert( (state & STATE_MASK) == ST_RUNNING );
                            s_logger.finer(
                                    m_localSocketAddress + " -> " + m_remoteSocketAddress +
                                    ": " + stateToString(state) + " tail!=null" );

                            m_socketChannelReader.stop();
                            break;
                        }
                    }
                }

                return 0;
            }
        }
    }

    public int accelerate( ShMem shMem, ByteBuffer message )
    {
        final Node node = new Node( (ByteBuffer) null );
        Node tail;
        for (;;)
        {
            tail = m_tail.get();
            if (tail == CLOSE_MARKER)
            {
                /* Session already closed, can happen. */
                shMem.close();
                return -1;
            }

            /* Session.accelerate() is not supposed to be used
             * while some other thread send data.
             */
            assert( tail == null );
            if (m_tail.compareAndSet(tail, node))
            {
                m_head = node;
                break;
            }
        }

        final int messageSize = ((message == null) ? 0 : message.remaining());
        if (messageSize > 0)
        {
            /* Asynchronous reply send implementation would be a pain,
             * let's do it synchronously, not a big problem.
             */
            try
            {
                for (;;)
                {
                    m_socketChannel.write( message );
                    if (message.remaining() == 0)
                        break;
                }
            }
            catch (Exception ex)
            {
                closeAndCleanupQueue( ex );
                releaseSocket( "accelerate()" );
                return -1;
            }
        }

        m_socketChannelReader.accelerate( shMem.getIn() );
        m_writer = new ShMemWriter( shMem.getOut(), 128*1024 );

        if (s_logger.isLoggable(Level.FINE))
        {
            /* Use a local address for client and peer address for server. */
            final String prefix = (messageSize == 0) ?
                    (m_localSocketAddress + "[C]") : (m_remoteSocketAddress + "[S]");
            s_logger.fine( prefix + ": switched to ShMem IPC (" + shMem + ")" );
        }

        removeNode( node );
        return 0;
    }

    public Listener replaceListener( Listener newListener )
    {
        return m_socketChannelReader.replaceListener( newListener );
    }

    public int handleReadyOps( ThreadPool threadPool )
    {
        final int readyOps = m_selectionKey.readyOps();
        int ret = 0;

        if ((readyOps & SelectionKey.OP_READ) != 0)
        {
            threadPool.execute( m_socketChannelReader );
            ret = 1;
        }

        if ((readyOps & SelectionKey.OP_WRITE) != 0)
            threadPool.execute( m_writer );

        /* It is safe to reset interest ops after threadPool.execute(),
         * because this code is executed in the selector thread,
         * and further interest ops updates will be definitely executed
         * later after return from this function.
         */
        m_selectionKey.interestOps( m_selectionKey.interestOps() & ~readyOps );
        return ret;
    }

    private void closeAndCleanupQueue( final Exception ex )
    {
        /* Session can be already closed, but can be not.
         * Let's clean up and close output queue,
         * socket channel reader queue will be closed soon as well.
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
            final Node next = node.next;
            if (node.rbuf != null)
                node.rbuf.release();
            s_nodeNextUpdater.lazySet( node, null );
            node = next;
        }
        m_head = node;

        if (ex != null)
        {
            /* SocketChannel.write() can throw following exceptions:
             *   - NotYetConnectedException (subclass of Exception)
             *   - ClosedChannelException (subclass of IOException)
             *   - AsynchronousCloseException (subclass of ClosedChannelException, IOException)
             *   - ClosedByInterruptException (subclass of ClosedChannelException, IOException)
             *   - IOException
             * Everything except the IOException we considering as a bug.
             */
            if (ex.getClass().equals(IOException.class))
            {
                if (s_logger.isLoggable(Level.FINER))
                {
                    s_logger.finer(
                            m_localSocketAddress + " -> " + m_remoteSocketAddress.toString() +
                            ": " + ex.toString() );
                }
            }
            else
            {
                if (s_logger.isLoggable(Level.WARNING))
                {
                    s_logger.warning(
                            m_localSocketAddress + " -> " + m_remoteSocketAddress.toString() +
                            ": " + ex.toString() );
                }
            }
        }
    }

    public final void releaseSocket( final String hint )
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
                            m_localSocketAddress + " -> " + m_remoteSocketAddress +
                            ": " + stateToString(state) + " -> " + stateToString(newState) + ": " + hint );
                }
                if ((newState & SOCK_RC_MASK) == 0)
                    m_collider.executeInSelectorThread( new SelectorDeregistrator() );
                break;
            }
        }
    }

    private void removeNode( Node node )
    {
        final Node next = node.next;
        if (next == null)
        {
            m_head = null;
            if (!m_tail.compareAndSet(node, null))
            {
                while (node.next == null);
                m_head = node.next;
                s_nodeNextUpdater.lazySet( node, null );
                if (m_head == CLOSE_MARKER)
                    releaseSocket( "removeNode(CAS failed)" );
                else
                    m_collider.executeInThreadPool( m_writer );
            }
        }
        else
        {
            s_nodeNextUpdater.lazySet( node, null );
            m_head = next;
            if (m_head == CLOSE_MARKER)
                releaseSocket( "removeNode()" );
            else
                m_collider.executeInThreadPool( m_writer );
        }
    }
}
