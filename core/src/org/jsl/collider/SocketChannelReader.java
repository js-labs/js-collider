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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SocketChannelReader extends ThreadPool.Runnable
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

    private class Suspender extends ColliderImpl.SelectorThreadRunnable
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
                final int state = m_state.get();
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
                                ": " + stateToString(state) + " (" + m_waits + " waits)." );
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
                    final int state = m_state.get();
                    assert( (state & STOP) != 0 );
                    assert( (state & CLOSE) == 0 );

                    final int newState = (state | CLOSE);
                    if (m_state.compareAndSet(state, newState))
                    {
                        if (s_logger.isLoggable(Level.FINER))
                        {
                            s_logger.finer(
                                    m_session.getLocalAddress() + " -> " + m_session.getRemoteAddress() +
                                    ": " + stateToString(state) + " -> " + stateToString(newState) +
                                    " (" + m_waits + " waits).");
                        }

                        if ((newState & LENGTH_MASK) == 0)
                            m_collider.executeInThreadPool( new CloseNotifier() );

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
            printStats();
        }
    }

    private class ShMemListener implements Session.Listener
    {
        private final ShMem.ChannelIn m_shMem;
        private Session.Listener m_listener;

        public ShMemListener( ShMem.ChannelIn shMem, Session.Listener listener )
        {
            m_shMem = shMem;
            m_listener = listener;
        }

        public final Session.Listener replaceListener( Session.Listener listener )
        {
            Session.Listener ret = m_listener;
            m_listener = listener;
            return ret;
        }

        public final void close()
        {
            m_shMem.close();
        }

        public void onDataReceived( ByteBuffer data )
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
        public void onDataReceived( ByteBuffer data )
        {
        }

        public void onConnectionClosed()
        {
            /* Should never be called. */
            assert( false );
        }
    }

    private static String stateToString( int state )
    {
        String ret = "[";
        if ((state & TAIL_LOCK) != 0)
            ret += "TAIL_LOCK ";

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

    private static final DummyListener s_dummyListener = new DummyListener();

    private static final int LENGTH_MASK = 0x0FFFFFFF;
    private static final int TAIL_LOCK   = 0x10000000;
    private static final int STOP        = 0x20000000;
    private static final int CLOSE       = 0x40000000;

    private final ColliderImpl m_collider;
    private final SessionImpl m_session;
    private final int m_forwardReadMaxSize;
    private final DataBlockCache m_dataBlockCache;
    private final int m_blockSize;
    private SocketChannel m_socketChannel;
    private SelectionKey m_selectionKey;
    private volatile Session.Listener m_dataListener;
    private Session.Listener m_closeListener;
    private ShMemListener m_shMemListener;

    private final Starter0 m_starter0;
    private final Starter1 m_starter1;
    private final Suspender m_suspender;
    private final AtomicInteger m_state;
    private final ByteBuffer [] m_iov;
    private DataBlock m_tail;

    private int m_statReads;
    private int m_statHandleData;

    private void handleData( DataBlock dataBlock, int state )
    {
        boolean tailLock;
        DataBlock freeDataBlock = null;

        handleDataLoop: for (;;)
        {
            ByteBuffer rw = dataBlock.rw;
            final int bytesReady = (state & LENGTH_MASK);
            int bytesRemaining = bytesReady;
            int pos = rw.position();
            for (;;)
            {
                final int bb = (m_blockSize - pos);
                if (bytesRemaining <= bb)
                {
                    final int limit = pos + bytesRemaining;
                    rw.limit( limit );
                    m_dataListener.onDataReceived( rw );
                    /* limit can be changed by listener,
                     * let's set it again to avoid exception.
                     */
                    rw.limit( limit );
                    rw.position( limit );
                    break;
                }

                bytesRemaining -= bb;
                rw.limit( m_blockSize );
                m_dataListener.onDataReceived( rw );

                DataBlock next = dataBlock.next;
                dataBlock.reset();

                if (freeDataBlock == null)
                {
                    dataBlock.next = null;
                    freeDataBlock = dataBlock;
                }
                else
                {
                    dataBlock.next = freeDataBlock;
                    freeDataBlock = null;
                    m_dataBlockCache.put( dataBlock );
                }

                dataBlock = next;
                rw = dataBlock.rw;
                pos = 0;
            }

            for (;;)
            {
                assert( (state & LENGTH_MASK) >= bytesReady );
                int newState = state;
                newState -= bytesReady;

                if ((newState & LENGTH_MASK) == 0)
                {
                    if ((newState & TAIL_LOCK) == 0)
                    {
                        newState |= TAIL_LOCK;
                        tailLock = true;
                    }
                    else
                        tailLock = false;

                    if (m_state.compareAndSet(state, newState))
                    {
                        if ((newState & CLOSE) == 0)
                        {
                            if ((state & LENGTH_MASK) >= m_forwardReadMaxSize)
                                m_collider.executeInSelectorThread( m_starter0 );
                        }
                        else
                        {
                            m_closeListener.onConnectionClosed();
                            printStats();
                        }
                        state = newState;

                        if (freeDataBlock != null)
                        {
                            assert( freeDataBlock.next == null );
                            m_dataBlockCache.put( freeDataBlock );
                        }

                        break handleDataLoop;
                    }
                }
                else
                {
                    if (m_state.compareAndSet(state, newState))
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
                state = m_state.get();
            }

            if (rw.limit() == rw.capacity())
            {
                DataBlock next = dataBlock.next;
                dataBlock.next = null;
                dataBlock.reset();
                m_dataBlockCache.put( dataBlock );
                dataBlock = next;
            }
        }

        if (tailLock)
        {
            assert( dataBlock == m_tail );
            final DataBlock tail = m_tail;
            m_tail = null;

            for (;;)
            {
                assert( (state & TAIL_LOCK) != 0 );
                final int newState = (state - TAIL_LOCK);
                if (m_state.compareAndSet(state, newState))
                    break;
                state = m_state.get();
            }

            assert( tail.next == null );
            m_dataBlockCache.put( tail.reset() );
        }
    }

    public SocketChannelReader(
            ColliderImpl colliderImpl,
            SessionImpl session,
            int forwardReadMaxSize,
            DataBlockCache dataBlockCache,
            SocketChannel socketChannel,
            SelectionKey selectionKey,
            Session.Listener sessionListener )
    {
        m_collider = colliderImpl;
        m_session = session;
        m_forwardReadMaxSize = forwardReadMaxSize;
        m_dataBlockCache = dataBlockCache;
        m_blockSize = dataBlockCache.getBlockSize();
        m_socketChannel = socketChannel;
        m_selectionKey = selectionKey;
        m_dataListener = sessionListener;
        m_closeListener = sessionListener;
        m_starter0 = new Starter0();
        m_starter1 = new Starter1();
        m_suspender = new Suspender();
        m_state = new AtomicInteger();
        m_iov = new ByteBuffer[2];
        m_tail = null;
    }

    private void printStats()
    {
        if (s_logger.isLoggable(Level.FINE))
        {
            s_logger.fine(
                    m_session.getLocalAddress() + " -> " + m_session.getRemoteAddress() +
                    ": reads=" + m_statReads + " handleData=" + m_statHandleData );
        }
    }

    public void runInThreadPool()
    {
        int tailLock;
        int state = m_state.get();
        for (;;)
        {
            if ((state & STOP) != 0)
            {
                for (;;)
                {
                    assert( (state & CLOSE) == 0 );
                    final int newState = (state | CLOSE);
                    if (m_state.compareAndSet(state, newState))
                    {
                        if (s_logger.isLoggable(Level.FINER))
                        {
                            s_logger.finer(
                                    m_session.getLocalAddress() + " -> " + m_session.getRemoteAddress() +
                                    ": " + stateToString(state) + " -> " + stateToString(newState) + "." );
                        }

                        if ((state & LENGTH_MASK) == 0)
                        {
                            m_closeListener.onConnectionClosed();
                            printStats();
                        }

                        m_collider.executeInSelectorThreadNoWakeup( m_suspender );
                        return;
                    }
                    state = m_state.get();
                }
            }

            if ((state & LENGTH_MASK) == 0)
            {
                if ((state & TAIL_LOCK) == 0)
                    tailLock = 0;
                else
                    tailLock = -1;
                break;
            }

            assert( (state & TAIL_LOCK) == 0 );

            final int newState = (state | TAIL_LOCK);
            if (m_state.compareAndSet(state, newState))
            {
                state = newState;
                tailLock = 1;
                break;
            }

            state = m_state.get();
        }

        int pos0;
        DataBlock prev;
        DataBlock dataBlock0;
        DataBlock dataBlock1;

        if (tailLock > 0)
        {
            dataBlock0 = m_tail;
            pos0 = dataBlock0.ww.position();
            final long space = (m_blockSize - pos0);
            if (space > 0)
            {
                prev = null;
                dataBlock1 = m_dataBlockCache.get(1);
            }
            else
            {
                prev = dataBlock0;
                dataBlock0 = m_dataBlockCache.get(2);
                dataBlock1 = dataBlock0.next;
                dataBlock0.next = null;
                pos0 = 0;
            }
        }
        else
        {
            prev = null;
            dataBlock0 = m_dataBlockCache.get(2);
            dataBlock1 = dataBlock0.next;
            dataBlock0.next = null;
            pos0 = 0;
        }

        m_iov[0] = dataBlock0.ww;
        m_iov[1] = dataBlock1.ww;

        long bytesReceived;
        try
        {
            //long startTime = System.nanoTime();
            bytesReceived = m_socketChannel.read( m_iov, 0, 2 );
            /*
            s_logger.finer(
                    m_stateListener.getPeerInfo() + ": received " + bytesReceived +
                    " space=" + space + " SR=" + m_speculativeRead + "." );
            long endTime = System.nanoTime();
            System.out.println( m_stateListener.getPeerInfo() +
                                ": " +
                                Util.formatDelay(startTime, endTime) + " sec." +
                                " space=" + space +
                                " bytesReceived=" + bytesReceived +
                                " SR=" + m_speculativeRead );
            */
            m_statReads++;
        }
        catch (ClosedChannelException ex)
        {
            /* Should not happen, considered as a bug. */
            if (s_logger.isLoggable(Level.WARNING))
            {
                s_logger.warning(
                        m_session.getLocalAddress() + " -> " + m_session.getRemoteAddress() +
                        ": " + ex.toString() + "." );
            }
            bytesReceived = 0;
        }
        catch (IOException ex)
        {
            /* Not actually a problem. */
            if (s_logger.isLoggable(Level.FINER))
            {
                s_logger.finer(
                        m_session.getLocalAddress() + " -> " + m_session.getRemoteAddress() +
                        ": " + ex.toString() + "." );
            }
            bytesReceived = 0;
        }
        catch (Exception ex)
        {
            /* Any other exception should not happen, considered as a bug. */
            if (s_logger.isLoggable(Level.WARNING))
            {
                s_logger.warning(
                        m_session.getLocalAddress() + " -> " + m_session.getRemoteAddress() +
                        ": " + ex.toString() + "." );
            }
            bytesReceived = 0;
        }

        m_iov[0] = null;
        m_iov[1] = null;

        if (bytesReceived > 0)
        {
            if (tailLock > 0)
            {
                if (prev != null)
                    prev.next = dataBlock0;
            }
            else
            {
                assert( prev == null );
                if (tailLock < 0)
                {
                    for (;;)
                    {
                        state = m_state.get();
                        if ((state & TAIL_LOCK) == 0)
                            break;
                    }
                }
            }

            if (bytesReceived > (m_blockSize - pos0))
            {
                dataBlock0.next = dataBlock1;
                m_tail = dataBlock1;
                dataBlock1 = null;
            }
            else
                m_tail = dataBlock0;

            for (;;)
            {
                int newState = state;
                newState &= LENGTH_MASK;
                newState += bytesReceived;
                assert( newState < LENGTH_MASK );

                newState |= (state & ~LENGTH_MASK);

                if (tailLock > 0)
                {
                    assert( (newState & TAIL_LOCK) != 0 );
                    newState -= TAIL_LOCK;
                }

                if (m_state.compareAndSet(state, newState))
                {
                    state = newState;
                    break;
                }

                state = m_state.get();
            }

            final int length = (state & LENGTH_MASK);
            if (length < m_forwardReadMaxSize)
                m_collider.executeInSelectorThreadNoWakeup( m_starter1 );
            else
                m_collider.executeInSelectorThreadNoWakeup( m_suspender );

            if (length == bytesReceived)
            {
                m_statHandleData++;
                handleData( dataBlock0, state );

                if (prev != null)
                {
                    prev.next = null;
                    m_dataBlockCache.put( prev.reset() );
                }
            }

            if (dataBlock1 != null)
            {
                assert( dataBlock1.ww.position() == 0 );
                assert( dataBlock1.next == null );
                m_dataBlockCache.put( dataBlock1 );
            }
        }
        else
        {
            for (;;)
            {
                assert( (state & CLOSE) == 0 );
                int newState = (state | CLOSE);

                if (tailLock > 0)
                {
                    assert( (state & TAIL_LOCK) != 0 );
                    newState -= TAIL_LOCK;
                }

                if (m_state.compareAndSet(state, newState))
                {
                    if (s_logger.isLoggable(Level.FINER))
                    {
                        s_logger.finer(
                                m_session.getLocalAddress() + " -> " + m_session.getRemoteAddress() +
                                ": " + stateToString(state) + " -> " + stateToString(newState) + "." );
                    }
                    state = newState;
                    break;
                }
                state = m_state.get();
            }

            m_collider.executeInSelectorThreadNoWakeup( m_suspender );
            if ((state & STOP) == 0)
                m_session.handleReaderStopped();

            /* assert( dataBlock0.next == null );
             * dataBlock1 will always be freed, dataBlock0 - sometimes.
             */
            assert( dataBlock1.next == null );

            if ((state & LENGTH_MASK) == 0)
            {
                m_closeListener.onConnectionClosed();
                printStats();

                if (tailLock > 0)
                {
                    if (prev == null)
                    {
                        assert( m_tail == dataBlock0 );
                        dataBlock1.next = dataBlock0.reset();
                    }
                    else
                    {
                        assert( m_tail == prev );
                        dataBlock1.next = dataBlock0;
                        dataBlock0.next = prev.reset();
                    }
                    m_tail = null;
                }
                else
                {
                    assert( prev == null );
                    dataBlock1.next = dataBlock0;
                }
            }
            else
            {
                if (tailLock > 0)
                {
                    assert( (m_tail == null) ||
                            ((prev == null) && (m_tail == dataBlock0)) ||
                            (m_tail == prev) );
                    if (prev != null)
                        dataBlock1.next = dataBlock0;
                }
                else
                {
                    assert( prev == null );
                    dataBlock1.next = dataBlock0;
                }
            }

            m_dataBlockCache.put( dataBlock1 );
        }
    }

    public final Session.Listener replaceListener( Session.Listener newListener )
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
                    assert( m_closeListener == dataListener );
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
            final int state = m_state.get();
            assert( (state & STOP) == 0 );

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
                if (m_state.compareAndSet(state, newState))
                {
                    if (s_logger.isLoggable(Level.FINER))
                    {
                        s_logger.finer(
                                m_session.getLocalAddress() + " -> " + m_session.getRemoteAddress() +
                                ": " + stateToString(state) + " -> " + stateToString(newState) + "." );
                    }
                    m_session.releaseSocket( "SocketChannelReader.stop()" );
                    break;
                }
            }
            else
            {
                final int newState = (state | STOP);
                if (m_state.compareAndSet(state, newState))
                {
                    if (s_logger.isLoggable(Level.FINER))
                    {
                        s_logger.finer(
                                m_session.getLocalAddress() + " -> " + m_session.getRemoteAddress() +
                                ": " + stateToString(state) + " -> " + stateToString(newState) + "." );
                    }
                    m_collider.executeInSelectorThread( new Stopper() );
                    break;
                }
            }
        }

        for (;;)
        {
            final Session.Listener dataListener = m_dataListener;
            assert( dataListener != s_dummyListener );
            if (s_dataListenerUpdater.compareAndSet(this, dataListener, s_dummyListener))
                break;
        }
    }
}
