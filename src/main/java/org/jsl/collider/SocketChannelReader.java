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
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.logging.Level;
import java.util.logging.Logger;

class SocketChannelReader extends ThreadPool.Runnable
{
    private class Starter0 extends ColliderImpl.SelectorThreadRunnable
    {
        public int runInSelectorThread()
        {
            final int interestOps = m_selectionKey.interestOps();
            assert( (interestOps & SelectionKey.OP_READ) == 0 );
            m_selectionKey.interestOps( interestOps | SelectionKey.OP_READ );
            return 0;
        }
    }

    private class Starter1 extends ColliderImpl.SelectorThreadRunnable
    {
        public int runInSelectorThread()
        {
            final int interestOps = m_selectionKey.interestOps();
            assert( (interestOps & SelectionKey.OP_READ) == 0 );
            m_selectionKey.interestOps( interestOps | SelectionKey.OP_READ );
            return 1;
        }
    }

    private static class Suspender extends ColliderImpl.SelectorThreadRunnable
    {
        public int runInSelectorThread()
        {
            return 1;
        }
    }

    private class Stopper extends ColliderImpl.SelectorThreadRunnable
    {
        private int m_waits;

        public int runInSelectorThread()
        {
            final int interestOps = m_selectionKey.interestOps();
            if ((interestOps & SelectionKey.OP_READ) == 0)
            {
                final int state = s_stateUpdater.get(SocketChannelReader.this);
                if ((state & CLOSE) == 0)
                {
                    m_waits++;
                    m_collider.executeInSelectorThreadLater( this );
                }
                else
                {
                    if (s_logger.isLoggable(Level.FINER))
                    {
                        s_logger.finer(
                                m_session.getLocalAddress() + " -> " + m_session.getRemoteAddress() +
                                ": " + stateToString(state) + ": " + m_waits + " waits" );
                    }
                    m_selectionKey = null;
                    m_socketChannel = null;
                    m_session.handleReaderStoppedST();
                }
            }
            else
            {
                for (;;)
                {
                    final int state = s_stateUpdater.get(SocketChannelReader.this);
                    assert((state & STOP) != 0);
                    assert((state & CLOSE) == 0);

                    final int newState = (state | CLOSE);
                    if (s_stateUpdater.compareAndSet(SocketChannelReader.this, state, newState))
                    {
                        if (s_logger.isLoggable(Level.FINER))
                        {
                            s_logger.finer(
                                    m_session.getLocalAddress() + " -> " + m_session.getRemoteAddress() +
                                    ": " + stateToString(state) + " -> " + stateToString(newState) +
                                    ": " + m_waits + " waits");
                        }

                        if ((newState & LENGTH_MASK) == 0)
                            m_collider.executeInThreadPool(new CloseNotifier());

                        m_selectionKey.interestOps( interestOps - SelectionKey.OP_READ );
                        m_selectionKey = null;
                        m_socketChannel = null;
                        m_session.handleReaderStoppedST();
                        break;
                    }
                }
            }
            return 0;
        }
    }

    private class CloseNotifier extends ThreadPool.Runnable
    {
        public void runInThreadPool()
        {
            m_closeListener.onConnectionClosed();
            logStats();

            // Possible queue states here are:
            // 1 block: (m_head == m_tail)
            // 2 blocks: (m_head is empty) && (m_head != m_tail) && (m_head.next == m_tail) && (m_tail.next == null)
            //           (m_head is not empty) && (m_head == m_tail) && (m_tail.next != null)
            // 3 blocks: (m_head is empty) && (m_head != m_tail) && (m_head.next == m_tail) && (m_tail.next != null)

            assert((m_head == m_tail) ||
                   ((m_head.rd.position() < m_head.rd.capacity()) && (m_head == m_tail) && (m_tail.next != null)) ||
                   ((m_head.rd.position() == m_head.rd.capacity()) && (m_head != m_tail) && (m_head.next == m_tail)));

            resetQueue();
        }
    }

    private class ShMemListener implements Session.Listener
    {
        private final ShMem.ChannelIn m_shMem;
        private Session.Listener m_listener;

        ShMemListener(ShMem.ChannelIn shMem, Session.Listener listener)
        {
            m_shMem = shMem;
            m_listener = listener;
        }

        Session.Listener replaceListener(Session.Listener listener)
        {
            Session.Listener ret = m_listener;
            m_listener = listener;
            return ret;
        }

        public final void close()
        {
            m_shMem.close();
        }

        public void onDataReceived( RetainableByteBuffer data )
        {
            int bytesRemaining = data.remaining();

            /* Is there any probability the packet will be fragmented? */
            assert( (bytesRemaining % 4) == 0 );
            assert( bytesRemaining > 0 );

            for (; bytesRemaining>0; bytesRemaining-=4)
            {
                final int size = data.getInt();
                int ret = m_shMem.handleData( size, m_listener );
                if (ret < 0)
                {
                    /* Failed to map next shared memory block.
                     * No chance to recover, let's close session.
                     */
                    m_session.closeConnection();
                    break;
                }
            }
        }

        public void onConnectionClosed()
        {
            m_listener.onConnectionClosed();
        }
    }

    private static class DummyListener implements Session.Listener
    {
        public void onDataReceived( RetainableByteBuffer data )
        {
        }

        public void onConnectionClosed()
        {
            /* Should never be called. */
            throw new AssertionError();
        }
    }

    private static String stateToString( int state )
    {
        String ret = "[";

        if ((state & STOP) != 0)
            ret += "STOP ";

        if ((state & CLOSE) != 0)
            ret += "CLOSE ";

        ret += (state & LENGTH_MASK);
        ret += "]";
        return ret;
    }

    private static final Logger s_logger = Logger.getLogger( "org.jsl.collider.Session" );

    private static final AtomicReferenceFieldUpdater<SocketChannelReader, Session.Listener>
        s_dataListenerUpdater = AtomicReferenceFieldUpdater.newUpdater(
            SocketChannelReader.class, Session.Listener.class, "m_dataListener" );

    private static final AtomicIntegerFieldUpdater<SocketChannelReader>
        s_stateUpdater = AtomicIntegerFieldUpdater.newUpdater(SocketChannelReader.class, "m_state");

    private static final DummyListener s_dummyListener = new DummyListener();

    private static final int LENGTH_MASK = 0x0FFFFFFF;
    private static final int STOP        = 0x10000000;
    private static final int CLOSE       = 0x20000000;

    private final ColliderImpl m_collider;
    private final SessionImpl m_session;
    private final int m_forwardReadMaxSize;
    private final RetainableDataBlockCache m_dataBlockCache;
    private SocketChannel m_socketChannel;
    private SelectionKey m_selectionKey;
    private volatile Session.Listener m_dataListener;
    private Session.Listener m_closeListener;
    private ShMemListener m_shMemListener;

    private final Starter0 m_starter0;
    private final Starter1 m_starter1;
    private final Suspender m_suspender;
    private final ByteBuffer [] m_iov;

    // We could reduce contention on the 'm_state' moving it
    // into RetainableDataBlock, but in this case we will not
    // be able to implement properly <forward read max size> feature.
    private volatile int m_state;

    private RetainableDataBlock m_head;
    private RetainableDataBlock m_tail;

    private int m_statReads;
    private int m_statHandleData;

    private void resetQueue()
    {
        RetainableDataBlock dataBlock = m_head;
        for (;;)
        {
            final RetainableDataBlock next = dataBlock.next;
            dataBlock.next = null;
            dataBlock.release();
            if (next == null)
                break;
            dataBlock = next;
        }
        m_head = null;
        m_tail = null;
    }

    private void handleData( int state )
    {
        handleDataLoop: for (;;)
        {
            final int bytesReady = (state & LENGTH_MASK);
            RetainableByteBuffer rd = m_head.rd;
            int blockSize = rd.capacity();
            int pos = rd.position();

            if (pos == blockSize)
            {
                final RetainableDataBlock next = m_head.next;
                m_head.next = null;
                m_head.release();
                m_head = next;
                rd = m_head.rd;
                blockSize = rd.capacity();
                pos = rd.position();
                assert(pos == 0);
            }

            int bytesRemaining = bytesReady;
            for (;;)
            {
                final int bb = (blockSize - pos);
                if (bytesRemaining <= bb)
                {
                    final int limit = (pos + bytesRemaining);
                    rd.limit(limit);
                    m_dataListener.onDataReceived(rd);
                    /* limit can be changed by listener,
                     * let's set it again to avoid exception.
                     */
                    rd.limit(limit);
                    rd.position(limit);
                    break;
                }

                bytesRemaining -= bb;
                rd.limit(blockSize);
                m_dataListener.onDataReceived(rd);

                final RetainableDataBlock next = m_head.next;
                m_head.next = null;
                m_head.release();
                m_head = next;
                rd = m_head.rd;
                blockSize = rd.capacity();
                pos = rd.position();
                assert(pos == 0);
            }

            for (;;)
            {
                assert((state & LENGTH_MASK) >= bytesReady);
                int newState = (state - bytesReady);

                if ((newState & LENGTH_MASK) == 0)
                {
                    if (s_stateUpdater.compareAndSet(this, state, newState))
                    {
                        if ((newState & CLOSE) == 0)
                        {
                            if ((state & LENGTH_MASK) >= m_forwardReadMaxSize)
                                m_collider.executeInSelectorThread( m_starter0 );
                        }
                        else
                        {
                            m_closeListener.onConnectionClosed();
                            logStats();

                            // Possible queue states here are:
                            // 1 block:  (m_head == m_tail)
                            // 2 blocks: (m_head is not empty) && (m_head == m_tail) && (m_tail.next != null)
                            // 3 blocks: (m_head is empty) && (m_head != m_tail) &&
                            //           (m_head.next == m_tail) && (m_tail.next != null),

                            assert((m_head == m_tail) ||
                                   ((m_head.rd.position() < m_head.rd.capacity()) && (m_head == m_tail) && (m_tail.next != null)) ||
                                   ((m_head.rd.position() == m_head.rd.capacity()) && (m_head != m_tail) && (m_head.next == m_tail)));

                            resetQueue();
                        }
                        break handleDataLoop;
                    }
                }
                else
                {
                    if (s_stateUpdater.compareAndSet(this, state, newState))
                    {
                        if (((state & LENGTH_MASK) >= m_forwardReadMaxSize) &&
                            ((newState & LENGTH_MASK) < m_forwardReadMaxSize) &&
                            ((newState & CLOSE) == 0))
                        {
                            m_collider.executeInSelectorThread( m_starter0 );
                        }
                        state = newState;
                        break;
                    }
                }
                state = s_stateUpdater.get(this);
            }
        }
    }

    SocketChannelReader(
            ColliderImpl colliderImpl,
            SessionImpl session,
            int forwardReadMaxSize,
            RetainableDataBlockCache dataBlockCache,
            SocketChannel socketChannel,
            SelectionKey selectionKey,
            Session.Listener sessionListener )
    {
        m_collider = colliderImpl;
        m_session = session;
        m_forwardReadMaxSize = forwardReadMaxSize;
        m_dataBlockCache = dataBlockCache;
        m_socketChannel = socketChannel;
        m_selectionKey = selectionKey;
        m_dataListener = sessionListener;
        m_closeListener = sessionListener;
        m_starter0 = new Starter0();
        m_starter1 = new Starter1();
        m_suspender = new Suspender();
        m_iov = new ByteBuffer[2];
        m_head = m_dataBlockCache.get(2);
        m_tail = m_head;
    }

    private void logStats()
    {
        if (s_logger.isLoggable(Level.FINE))
        {
            s_logger.fine(
                    m_session.getLocalAddress() + " -> " + m_session.getRemoteAddress() +
                    ": reads=" + m_statReads + " handleData=" + m_statHandleData);
        }
    }

    public void runInThreadPool()
    {
        /* In a case if the queue is empty
         * we could try to reuse data blocks from the beginning.
         */
        /*
        if ((s_stateUpdater.get(this) & LENGTH_MASK) == 0)
        {
            // Can try to reuse all blocks we have in the queue
            RetainableDataBlock next;
            for (;;)
            {
                final boolean reuse = m_head.clearSafe();
                next = m_head.next;
                if (reuse)
                    break;
                m_head.release();
                m_head = next;
            }
        }
        */

        /* Always read 2 data blocks */
        int remaining = m_tail.wr.remaining();
        if (remaining == 0)
        {
            assert(m_tail.next == null);
            m_tail.next = m_dataBlockCache.get(2);
            m_tail = m_tail.next;
            remaining = m_tail.wr.remaining();
            assert(remaining == m_tail.wr.capacity());
        }
        else
        {
            if (m_tail.next == null)
                m_tail.next = m_dataBlockCache.get(1);
            else
                assert(m_tail.next.wr.position() == 0);
        }

        m_iov[0] = m_tail.wr;
        m_iov[1] = m_tail.next.wr;

        long bytesReceived;
        try
        {
            bytesReceived = m_socketChannel.read( m_iov, 0, 2 );
            m_statReads++;
        }
        catch (final ClosedChannelException ex)
        {
            /* Should not happen, considered as a bug. */
            if (s_logger.isLoggable(Level.WARNING))
            {
                s_logger.warning(
                        m_session.getLocalAddress() + " -> " + m_session.getRemoteAddress() +
                        ": " + ex.toString() );
            }
            bytesReceived = 0;
        }
        catch (final IOException ex)
        {
            /* Not actually a problem. */
            if (s_logger.isLoggable(Level.FINER))
            {
                s_logger.finer(
                        m_session.getLocalAddress() + " -> " + m_session.getRemoteAddress() +
                        ": " + ex.toString() );
            }
            bytesReceived = 0;
        }
        catch (final Exception ex)
        {
            /* Any other exception should not happen, considered as a bug. */
            if (s_logger.isLoggable(Level.WARNING))
            {
                s_logger.warning(
                        m_session.getLocalAddress() + " -> " + m_session.getRemoteAddress() +
                        ": " + ex.toString() );
            }
            bytesReceived = 0;
        }

        m_iov[0] = null;
        m_iov[1] = null;

        int state = s_stateUpdater.get(this);
        if (bytesReceived > 0)
        {
            if (bytesReceived >= remaining)
            {
                // m_tail must be updated before m_state
                assert(m_tail.next != null);
                assert(m_tail.wr.position() == m_tail.wr.capacity());
                m_tail = m_tail.next;
            }

            for (;;)
            {
                int newState = state;
                newState &= LENGTH_MASK;
                newState += bytesReceived;

                assert(newState < LENGTH_MASK);
                newState |= (state & ~LENGTH_MASK);

                if (s_stateUpdater.compareAndSet(this, state, newState))
                {
                    state = newState;
                    break;
                }

                state = s_stateUpdater.get(this);
            }

            // The tricky thing is the session can be closed at this point

            final int length = (state & LENGTH_MASK);
            if (length < m_forwardReadMaxSize)
                m_collider.executeInSelectorThreadNoWakeup(m_starter1);
            else
            {
                assert((length - m_forwardReadMaxSize) < m_forwardReadMaxSize);
                m_collider.executeInSelectorThreadNoWakeup(m_suspender);
            }

            if (length == bytesReceived)
            {
                handleData(state);
                m_statHandleData++;
            }
        }
        else
        {
            for (;;)
            {
                assert((state & CLOSE) == 0);
                int newState = (state | CLOSE);

                if (s_stateUpdater.compareAndSet(this, state, newState))
                {
                    if (s_logger.isLoggable(Level.FINER))
                    {
                        s_logger.finer(
                                m_session.getLocalAddress() + " -> " + m_session.getRemoteAddress() +
                                ": " + stateToString(state) + " -> " + stateToString(newState));
                    }
                    state = newState;
                    break;
                }
                state = s_stateUpdater.get(this);
            }

            m_collider.executeInSelectorThreadNoWakeup( m_suspender );
            if ((state & STOP) == 0)
                m_session.handleReaderStopped();

            if ((state & LENGTH_MASK) == 0)
            {
                m_closeListener.onConnectionClosed();
                logStats();

                // Possible queue states here are:
                // 2 blocks: (m_head is not empty) && (m_head == m_tail) && (m_tail.next != null)
                // 3 blocks: (m_head is empty) && (m_head != m_tail) && (m_head.next == m_tail) && (m_tail.next != null)

                assert(((m_head.rd.position() < m_head.rd.capacity()) && (m_head == m_tail) && (m_tail.next != null)) ||
                       ((m_head.rd.position() == m_head.rd.capacity()) && (m_head != m_tail) && (m_head.next == m_tail)));

                resetQueue();
            }
        }
    }

    public final Session.Listener replaceListener(Session.Listener newListener)
    {
        /* Supposed to be called from the SessionListener.onDataReceived() trace only,
         * but keep in mind Session.closeConnection() can be called any time.
         */
        if (m_shMemListener == null)
        {
            for (;;)
            {
                final Session.Listener dataListener = m_dataListener;
                if (dataListener == s_dummyListener)
                {
                    /* SocketChannelReader.stop() already called,
                     * let's change only m_closeListener.
                     */
                    Session.Listener ret = m_closeListener;
                    m_closeListener = newListener;
                    return ret;
                }

                if (s_dataListenerUpdater.compareAndSet(this, dataListener, newListener))
                {
                    assert(m_closeListener == dataListener);
                    m_closeListener = newListener;
                    return dataListener;
                }
            }
        }
        else
            return m_shMemListener.replaceListener( newListener );
    }

    public final void accelerate( ShMem.ChannelIn shMemIn )
    {
        /* Supposed to be called only from the Session.Listener.onDataReceived() trace only. */
        assert( m_shMemListener == null );
        final Session.Listener dataListener = m_dataListener;
        if (dataListener == s_dummyListener)
        {
            /* Session is being stopped */
            shMemIn.close();
        }
        else
        {
            final ShMemListener shMemListener = new ShMemListener( shMemIn, dataListener );
            if (s_dataListenerUpdater.compareAndSet(this, dataListener, shMemListener))
            {
                m_closeListener = shMemListener;
                m_shMemListener = shMemListener;
            }
            else
            {
                /* Session is being stopped,
                 * but owner of the shMemIn is ShMemListener now.
                 */
                shMemListener.close();
            }
        }
    }

    public final void reset()
    {
        /* Supposed to be called by SessionImpl in a case if session
         * was closed after SocketChannelReader was created,
         * but before it was started.
         */
        m_closeListener.onConnectionClosed();

        assert( m_head == m_tail );
        assert( m_head.next != null );
        m_head.next.release();
        m_head.next = null;
        m_head.release();
        m_head = null;
        m_tail = null;
    }

    public final void start()
    {
        m_collider.executeInSelectorThread( m_starter0 );
    }

    public final void stop()
    {
        /*
        if (m_shMemListener != null)
            m_shMemListener.stop();
        */
        for (;;)
        {
            final int state = s_stateUpdater.get(this);
            assert((state & STOP) == 0);

            if ((state & CLOSE) != 0)
            {
                /* Reader is already being closed,
                 * handleReaderStopped() is already called or will be called soon.
                 */
                break;
            }

            if ((state & LENGTH_MASK) >= m_forwardReadMaxSize)
            {
                final int newState = (state | CLOSE);
                if (s_stateUpdater.compareAndSet(this, state, newState))
                {
                    if (s_logger.isLoggable(Level.FINER))
                    {
                        s_logger.finer(
                                m_session.getLocalAddress() + " -> " + m_session.getRemoteAddress() +
                                ": " + stateToString(state) + " -> " + stateToString(newState));
                    }
                    m_session.releaseSocket("SocketChannelReader.stop()");
                    break;
                }
            }
            else
            {
                final int newState = (state | STOP);
                if (s_stateUpdater.compareAndSet(this, state, newState))
                {
                    if (s_logger.isLoggable(Level.FINER))
                    {
                        s_logger.finer(
                                m_session.getLocalAddress() + " -> " + m_session.getRemoteAddress() +
                                ": " + stateToString(state) + " -> " + stateToString(newState));
                    }
                    m_collider.executeInSelectorThread(new Stopper());
                    break;
                }
            }
        }

        /* Calling closeConnection() means
         * the session listener do not want to receive any further data,
         * only close notification.
         */

        for (;;)
        {
            final Session.Listener dataListener = m_dataListener;
            assert(dataListener != s_dummyListener);
            if (s_dataListenerUpdater.compareAndSet(this, dataListener, s_dummyListener))
                break;
        }
    }
}
