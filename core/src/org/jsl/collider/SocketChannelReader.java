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

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SocketChannelReader extends ThreadPool.Runnable
{
    private class Starter extends ColliderImpl.SelectorThreadRunnable
    {
        public void runInSelectorThread()
        {
            if (m_selectionKey != null)
            {
                int interestOps = m_selectionKey.interestOps();
                assert( (interestOps & SelectionKey.OP_READ) == 0 );
                m_selectionKey.interestOps( interestOps | SelectionKey.OP_READ );
            }
            else
            {
                m_socketChannel = null;
                m_collider.executeInThreadPool( new Stopper2() );
            }
        }
    }

    private class Stopper1 extends ColliderImpl.SelectorThreadRunnable
    {
        public void runInSelectorThread()
        {
            int interestOps = m_selectionKey.interestOps();

            if (s_logger.isLoggable(Level.FINER))
                s_logger.finer( m_session.getRemoteAddress() + ": interestOps=" + interestOps );

            if ((interestOps & SelectionKey.OP_READ) == 0)
            {
                /* 3 possible cases:
                 * (starter is scheduled) or (thread reading socket) or (socket already failed)
                 */
                m_selectionKey = null;
            }
            else
            {
                /* Simplest case: session is waiting for new data,
                 * no one thread reading socket.
                 */
                interestOps &= ~SelectionKey.OP_READ;
                m_selectionKey.interestOps( interestOps );
                m_selectionKey = null;
                m_socketChannel = null;
                m_collider.executeInThreadPool( new Stopper2() );
            }
        }
    }

    private class Stopper2 extends ThreadPool.Runnable
    {
        public void runInThreadPool()
        {
            stop( m_state.get(), false );
        }
    }

    private class ShMemListener implements Session.Listener
    {
        private final ShMem.ChannelIn m_shMem;
        private Session.Listener m_listener;
        private volatile boolean m_stop;

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

        public final void stop()
        {
            m_stop = true;
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

                if (m_stop)
                    break;
            }
        }

        public void onConnectionClosed()
        {
            m_listener.onConnectionClosed();
        }
    }

    private static String stateToString( int state )
    {
        String ret = "[";
        if ((state & CLOSE) != 0)
            ret += "CLOSE ";

        if ((state & TAIL_LOCK) != 0)
            ret += "TAIL_LOCK ";

        ret += (state & LENGTH_MASK);
        ret += "]";
        return ret;
    }

    private static final Logger s_logger = Logger.getLogger( "org.jsl.collider.Session" );

    private static final int LENGTH_MASK = 0x0FFFFFFF;
    private static final int TAIL_LOCK   = 0x10000000;
    private static final int CLOSE       = 0x20000000;

    private final ColliderImpl m_collider;
    private final SessionImpl m_session;
    private final int m_forwardReadMaxSize;
    private final DataBlockCache m_dataBlockCache;
    private final int m_blockSize;
    private SocketChannel m_socketChannel;
    private SelectionKey m_selectionKey;
    private Session.Listener m_sessionListener;
    private ShMemListener m_shMemListener;

    private final Starter m_starter;
    private final AtomicInteger m_state;
    private final ByteBuffer [] m_iov;
    private boolean m_speculativeRead;
    private DataBlock m_tail;

    static public final PerfCounter s_pc = new PerfCounter("PC");
    static public final StatCounter s_sc = new StatCounter("SC");

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
                int bb = (m_blockSize - pos);
                if (bytesRemaining <= bb)
                {
                    int limit = pos + bytesRemaining;
                    rw.limit( limit );
                    m_sessionListener.onDataReceived( rw );
                    rw.position( limit );
                    break;
                }

                bytesRemaining -= bb;
                rw.limit( m_blockSize );
                m_sessionListener.onDataReceived( rw );

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
                        if ((state & LENGTH_MASK) > m_forwardReadMaxSize)
                        {
                            assert( (newState & LENGTH_MASK) <= m_forwardReadMaxSize );
                            assert( (newState & CLOSE) == 0 );
                            m_speculativeRead = true;
                            m_collider.executeInThreadPool( this );
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
                        if (((state & LENGTH_MASK) > m_forwardReadMaxSize) &&
                            ((newState & LENGTH_MASK) <= m_forwardReadMaxSize))
                        {
                            assert( (newState & CLOSE) == 0 );
                            m_speculativeRead = true;
                            m_collider.executeInThreadPool( this );
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
            DataBlock tail = m_tail;
            m_tail = null;

            for (;;)
            {
                assert( (state & TAIL_LOCK) != 0 );
                int newState = (state - TAIL_LOCK);
                if (m_state.compareAndSet(state, newState))
                {
                    state = newState;
                    break;
                }
                state = m_state.get();
            }

            assert( tail.next == null );
            m_dataBlockCache.put( tail.reset() );
        }

        if ((state & CLOSE) != 0)
        {
            m_sessionListener.onConnectionClosed();
            printStats();
        }
    }

    private int m_statReads;
    private int m_statHandleData;

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
        m_forwardReadMaxSize= forwardReadMaxSize;
        m_dataBlockCache = dataBlockCache;
        m_blockSize = dataBlockCache.getBlockSize();
        m_socketChannel = socketChannel;
        m_selectionKey = selectionKey;
        m_sessionListener = sessionListener;
        m_starter = new Starter();
        m_state = new AtomicInteger();
        m_iov = new ByteBuffer[2];
        m_tail = null;
    }

    private void printStats()
    {
        System.out.println( m_session.getRemoteAddress() + ":" +
                " reads=" + m_statReads +
                " handleData=" + m_statHandleData );
    }

    private int stop( int state, boolean releaseTailLock )
    {
        m_session.handleReaderStopped();

        int newState;
        for (;;)
        {
            assert( (state & CLOSE) == 0 );
            newState = (state | CLOSE);

            if (releaseTailLock)
            {
                assert( (state & TAIL_LOCK) != 0 );
                newState -= TAIL_LOCK;
            }

            if (m_state.compareAndSet(state, newState))
                break;
            state = m_state.get();
        }

        if (s_logger.isLoggable(Level.FINER))
        {
            s_logger.finer(
                    m_session.getRemoteAddress() +
                    ": " + stateToString(state) + " -> " + stateToString(newState) + "." );
        }

        if ((newState & LENGTH_MASK) == 0)
        {
            m_sessionListener.onConnectionClosed();
            printStats();
        }

        return newState;
    }

    public void runInThreadPool()
    {
        int tailLock;
        int state = m_state.get();
        for (;;)
        {
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
        long space;
        DataBlock prev;
        DataBlock dataBlock0;
        DataBlock dataBlock1;

        if (tailLock > 0)
        {
            dataBlock0 = m_tail;
            pos0 = dataBlock0.ww.position();
            space = (m_blockSize - pos0);
            if (space > 0)
            {
                prev = null;
                dataBlock1 = m_dataBlockCache.get(1);
                space += m_blockSize;
            }
            else
            {
                prev = dataBlock0;
                dataBlock0 = m_dataBlockCache.get(2);
                dataBlock1 = dataBlock0.next;
                dataBlock0.next = null;
                pos0 = 0;
                space = m_blockSize * 2;
            }
        }
        else
        {
            prev = null;
            dataBlock0 = m_dataBlockCache.get(2);
            dataBlock1 = dataBlock0.next;
            dataBlock0.next = null;
            pos0 = 0;
            space = m_blockSize * 2;
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
        catch (Exception ex)
        {
            if (s_logger.isLoggable(Level.WARNING))
            {
                s_logger.warning(
                        m_socketChannel.socket().getRemoteSocketAddress().toString() +
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
                {
                    assert( pos0 == 0 );
                    prev.next = dataBlock0;
                }
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

            if ((state & LENGTH_MASK) > m_forwardReadMaxSize)
            {
                /* Let's skip further reading for a while */
            }
            else if (bytesReceived == space)
            {
                m_speculativeRead = true;
                m_collider.executeInThreadPool( this );
            }
            else
            {
                m_speculativeRead = false;
                m_collider.executeInSelectorThread( m_starter );
            }

            if ((state & LENGTH_MASK) == bytesReceived)
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
        else if (m_speculativeRead)
        {
            /* dataBlock1 will always be freed, dataBlock0 - sometimes */

            assert( dataBlock0.next == null );
            assert( dataBlock1.next == null );
            assert( dataBlock1.ww.position() == 0 );

            if (tailLock > 0)
            {
                for (;;)
                {
                    assert( (state & TAIL_LOCK) != 0 );
                    int newState = (state - TAIL_LOCK);
                    if (m_state.compareAndSet(state, newState))
                    {
                        state = newState;
                        break;
                    }
                    state = m_state.get();
                }

                if ((state & LENGTH_MASK) == 0)
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
                    if (prev != null)
                        dataBlock1.next = dataBlock0;
                }
            }
            else
            {
                assert( prev == null );
                dataBlock1.next = dataBlock0;
            }

            m_dataBlockCache.put( dataBlock1 );

            m_speculativeRead = false;
            m_collider.executeInSelectorThread( m_starter );
        }
        else
        {
            final int newState = stop( state, (tailLock > 0) );

            /* assert( dataBlock0.next == null );
             * dataBlock1 will always be freed, dataBlock0 - sometimes.
             */
            assert( dataBlock1.next == null );

            if ((newState & LENGTH_MASK) == 0)
            {
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

    public final Session.Listener replaceListener( Session.Listener sessionListener )
    {
        if (m_shMemListener == null)
        {
            Session.Listener ret = m_sessionListener;
            m_sessionListener = sessionListener;
            return ret;
        }
        else
            return m_shMemListener.replaceListener( sessionListener );
    }

    public final void accelerate( ShMem.ChannelIn shMemIn )
    {
        m_shMemListener = new ShMemListener( shMemIn, m_sessionListener );
        m_sessionListener = m_shMemListener;
    }

    public final void start()
    {
        m_collider.executeInSelectorThread( m_starter );
    }

    public final void stop()
    {
        /* TODO: no more onDataReceived() notifications should happen */
        if (m_shMemListener != null)
            m_shMemListener.stop();
        m_collider.executeInSelectorThread( new Stopper1() );
    }
}
