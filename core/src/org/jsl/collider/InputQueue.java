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
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;


public class InputQueue extends ThreadPool.Runnable
{
    private static class DataBlock
    {
        public DataBlock next;
        public final ByteBuffer buf;
        public final ByteBuffer rw;
        public final ByteBuffer ww;

        public DataBlock( boolean useDirectBuffers, int blockSize )
        {
            next = null;
            if (useDirectBuffers)
                buf = ByteBuffer.allocateDirect( blockSize );
            else
                buf = ByteBuffer.allocate(blockSize);
            rw = buf.duplicate();
            ww = buf.duplicate();
            rw.limit(0);
        }

        public final DataBlock reset()
        {
            DataBlock dataBlock = next;
            next = null;
            rw.clear();
            rw.limit(0);
            ww.clear();
            return dataBlock;
        }
    }

    public static class DataBlockCache
    {
        private final boolean m_useDirectBuffers;
        private final int m_blockSize;
        private final int m_maxSize;
        private final ThreadLocal<DataBlock> m_tls;
        private final AtomicInteger m_state;
        private int m_size;
        private DataBlock m_top;

        public DataBlockCache( boolean useDirectBuffers, int blockSize, int initialSize, int maxSize )
        {
            m_useDirectBuffers = useDirectBuffers;
            m_blockSize = blockSize;
            m_maxSize = maxSize;
            m_tls = new ThreadLocal<DataBlock>();
            m_state = new AtomicInteger(0);
            m_size = initialSize;

            for (int idx=0; idx<initialSize; idx++)
            {
                DataBlock dataBlock = new DataBlock( m_useDirectBuffers, m_blockSize );
                dataBlock.next = m_top;
                m_top = dataBlock;
            }
        }

        public final int getBlockSize()
        {
            return m_blockSize;
        }

        public final void put( DataBlock dataBlock )
        {
            assert( dataBlock.next == null );

            if (m_tls.get() == null)
                m_tls.set( dataBlock );
            else
            {
                for (;;)
                {
                    if (m_state.compareAndSet(0, 1))
                        break;
                }
                try
                {
                    if (m_size < m_maxSize)
                    {
                        m_size++;
                        dataBlock.next = m_top;
                        m_top = dataBlock;
                    }
                }
                finally
                {
                    m_state.set(0);
                }
            }
        }

        public final DataBlock get()
        {
            DataBlock dataBlock = m_tls.get();
            if (dataBlock != null)
            {
                m_tls.set( null );
                return dataBlock;
            }

            for (;;)
            {
                if (m_state.compareAndSet(0, 1))
                    break;
            }

            try
            {
                if (m_top != null)
                {
                    dataBlock = m_top;
                    m_top = dataBlock.next;
                    dataBlock.next = null;
                    m_size--;
                    return dataBlock;
                }
            }
            finally
            {
                m_state.set(0);
            }

            return new DataBlock( m_useDirectBuffers, m_blockSize );
        }

        public final void get( DataBlock [] dataBlock )
        {
            int idx = 0;

            DataBlock db = m_tls.get();
            if (db != null)
            {
                m_tls.set( null );
                dataBlock[0] = db;
                idx++;
            }

            for (;;)
            {
                if (m_state.compareAndSet(0, 1))
                    break;
            }

            try
            {
                for (;;)
                {
                    if (m_top == null)
                        break;

                    db = m_top;
                    m_top = db.next;
                    db.next = null;
                    dataBlock[idx] = db;
                    m_size--;

                    if (++idx == dataBlock.length)
                        return;
                }
            }
            finally
            {
                m_state.set(0);
            }

            for (; idx<dataBlock.length; idx++)
                dataBlock[idx] = new DataBlock( m_useDirectBuffers, m_blockSize );
        }
    }

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
            {
                s_logger.finer(
                        m_session.getRemoteAddress().toString() +
                        ": interestOps=" + interestOps );
            }

            if ((interestOps & SelectionKey.OP_READ) == 0)
            {
                /* 3 possible cases:
                 * (starter is scheduled) or (thread reading socket) or (socket already failed)
                 */
                m_selectionKey = null;
            }
            else
            {
                /* Simplest case: no thread reading socket */
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
            m_session.handleReaderStopped();

            long state = m_state.get();
            long newState;
            for (;;)
            {
                assert( (state & CLOSED) == 0 );
                newState = (state | CLOSED);
                if (m_state.compareAndSet(state, newState))
                    break;
                state = m_state.get();
            }

            if (s_logger.isLoggable(Level.FINER))
            {
                s_logger.finer(
                        m_session.getRemoteAddress().toString() +
                        ": " + stateToString(state) + " -> " + stateToString(newState) + "." );
            }

            if ((newState & LENGTH_MASK) == 0)
            {
                m_listener.onConnectionClosed();
                printStats();
            }
        }
    }

    private static String stateToString( long state )
    {
        String ret = "[";
        if ((state & CLOSED) != 0)
            ret += "CLOSED ";

        if ((state & TAIL_LOCK) != 0)
            ret += "TAIL_LOCK ";

        ret += (state & LENGTH_MASK);
        ret += "]";
        return ret;
    }

    private static class Tls
    {
        public final DataBlock [] dataBlock = new DataBlock[2];
        public final ByteBuffer [] iov = new ByteBuffer[2];
    }

    private static final Logger s_logger = Logger.getLogger( InputQueue.class.getName() );

    private static final ThreadLocal<Tls> s_tls = new ThreadLocal<Tls>()
    {
        protected Tls initialValue() { return new Tls(); }
    };

    private static final long LENGTH_MASK = 0x00000000FFFFFFFFL;
    private static final long TAIL_LOCK   = 0x0000000100000000L;
    private static final long CLOSED      = 0x0000000200000000L;

    private final ColliderImpl m_collider;
    private final DataBlockCache m_dataBlockCache;
    private final int m_blockSize;
    private final SessionImpl m_session;
    private SocketChannel m_socketChannel;
    private SelectionKey m_selectionKey;
    private final Session.Listener m_listener;

    private final Starter m_starter;
    private final AtomicLong m_state;
    private boolean m_speculativeRead;
    private DataBlock m_tail;

    private void handleData( DataBlock dataBlock, long state )
    {
        boolean tailLock;
        long bytesReady = (state & LENGTH_MASK);

        for (;;)
        {
            ByteBuffer rw = dataBlock.rw;
            assert( rw.position() == rw.limit() );

            long bytesRest = bytesReady;
            int pos = rw.position();
            for (;;)
            {
                long bb = (m_blockSize - pos);
                if (bytesRest <= bb)
                {
                    int limit = pos + (int) bytesRest;
                    rw.limit( limit );
                    m_listener.onDataReceived( rw );
                    rw.position( limit );
                    break;
                }

                bytesRest -= bb;
                rw.limit( m_blockSize );
                m_listener.onDataReceived( rw );

                DataBlock next = dataBlock.reset();
                m_dataBlockCache.put( dataBlock );
                dataBlock = next;
                rw = dataBlock.rw;
                pos = 0;
            }

            for (;;)
            {
                long newState = state;
                newState -= bytesReady;

                if (((newState & LENGTH_MASK) == 0) && ((newState & TAIL_LOCK) == 0))
                {
                    newState |= TAIL_LOCK;
                    tailLock = true;
                }
                else
                    tailLock = false;

                if (m_state.compareAndSet(state, newState))
                {
                    state = newState;
                    break;
                }

                state = m_state.get();
            }

            bytesReady = (state & LENGTH_MASK);
            if (bytesReady == 0)
                break;
        }

        if (tailLock)
        {
            DataBlock tail = m_tail;
            m_tail = null;

            for (;;)
            {
                assert( (state & TAIL_LOCK) != 0 );
                long newState = (state - TAIL_LOCK);
                if (m_state.compareAndSet(state, newState))
                {
                    state = newState;
                    break;
                }
                state = m_state.get();
            }

            DataBlock next = tail.reset();
            assert( next == null );
            m_dataBlockCache.put( tail );
        }

        if ((state & CLOSED) != 0)
        {
            m_listener.onConnectionClosed();
            printStats();
        }
    }

    private int m_statReads;
    private int m_statHandleData;

    public InputQueue(
            ColliderImpl colliderImpl,
            DataBlockCache dataBlockCache,
            SessionImpl session,
            SocketChannel socketChannel,
            SelectionKey selectionKey,
            Session.Listener listener )
    {
        m_collider = colliderImpl;
        m_dataBlockCache = dataBlockCache;
        m_blockSize = dataBlockCache.getBlockSize();
        m_session = session;
        m_socketChannel = socketChannel;
        m_selectionKey = selectionKey;
        m_listener = listener;
        m_starter = new Starter();
        m_state = new AtomicLong();
        m_tail = null;
    }

    private void printStats()
    {
        System.out.println( m_session.getRemoteAddress() + ":" +
                " reads=" + m_statReads +
                " handleData=" + m_statHandleData );
    }

    public void runInThreadPool()
    {
        int tailLock;
        long state = m_state.get();
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

            long newState = state;
            newState |= TAIL_LOCK;
            tailLock = 1;

            if (m_state.compareAndSet(state, newState))
            {
                state = newState;
                break;
            }

            state = m_state.get();
        }

        int pos0;
        long space;
        DataBlock prev;
        DataBlock dataBlock0;
        DataBlock dataBlock1;

        Tls tls = s_tls.get();

        if (tailLock > 0)
        {
            dataBlock0 = m_tail;
            pos0 = dataBlock0.ww.position();
            space = (m_blockSize - pos0);
            if (space > 0)
            {
                prev = null;
                dataBlock1 = m_dataBlockCache.get();
                space += m_blockSize;
            }
            else
            {
                prev = dataBlock0;
                m_dataBlockCache.get( tls.dataBlock );
                dataBlock0 = tls.dataBlock[0];
                dataBlock1 = tls.dataBlock[1];
                tls.dataBlock[0] = null;
                tls.dataBlock[1] = null;
                pos0 = 0;
                space = m_blockSize * 2;
            }
        }
        else
        {
            prev = null;
            m_dataBlockCache.get( tls.dataBlock );
            dataBlock0 = tls.dataBlock[0];
            dataBlock1 = tls.dataBlock[1];
            tls.dataBlock[0] = null;
            tls.dataBlock[1] = null;
            pos0 = 0;
            space = m_blockSize * 2;
        }

        tls.iov[0] = dataBlock0.ww;
        tls.iov[1] = dataBlock1.ww;

        long bytesReceived;
        try
        {
            //long startTime = System.nanoTime();
            bytesReceived = m_socketChannel.read( tls.iov, 0, 2 );
            /*
            long endTime = System.nanoTime();
            System.out.println( m_session.getRemoteAddress() +
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

        if (bytesReceived > 0)
        {
            if (prev != null)
                prev.next = dataBlock0;

            if (tailLock < 0)
            {
                for (;;)
                {
                    state = m_state.get();
                    if ((state & TAIL_LOCK) == 0)
                        break;
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
                long newState = state;
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

            if (bytesReceived == space)
            {
                m_speculativeRead = true;
                m_collider.executeInThreadPool( this );
            }
            else
            {
                m_speculativeRead = false;
                m_collider.executeInSelectorThread( m_starter );
            }

            /*
            System.out.println( m_session.getRemoteAddress() +
                    " space=" + space +
                    " bytesReceived=" + bytesReceived +
                    " length=" + (state & LENGTH_MASK) +
                    (m_speculativeRead ? " SR" : "") );
            */

            if ((state & LENGTH_MASK) == bytesReceived)
            {
                m_statHandleData++;
                handleData( dataBlock0, state );

                if (prev != null)
                {
                    prev.reset();
                    m_dataBlockCache.put( prev );
                }
            }

            if (dataBlock1 != null)
            {
                assert( dataBlock1.ww.position() == 0 );
                m_dataBlockCache.put( dataBlock1 );
            }
        }
        else
        {
            if (m_speculativeRead)
            {
                if (tailLock > 0)
                {
                    for (;;)
                    {
                        assert( (state & TAIL_LOCK) != 0 );
                        long newState = (state - TAIL_LOCK);
                        if (m_state.compareAndSet(state, newState))
                            break;
                        state = m_state.get();
                    }
                }
                m_speculativeRead = false;
                m_collider.executeInSelectorThread( m_starter );
            }
            else
            {
                /* Should be called first to be sure sendData()
                 * will be blocked before onConnectionClose().
                 */
                m_session.handleReaderStopped();

                long newState;
                for (;;)
                {
                    newState = state;
                    newState |= CLOSED;

                    if (tailLock > 0)
                    {
                        assert( (newState & TAIL_LOCK) != 0 );
                        newState -= TAIL_LOCK;
                    }

                    if (m_state.compareAndSet(state, newState))
                        break;

                    state = m_state.get();
                }

                if (s_logger.isLoggable(Level.FINER))
                {
                    s_logger.finer(
                            m_session.getRemoteAddress().toString() +
                            ": " + stateToString(state) + " -> " + stateToString(newState) + "." );
                }

                if ((newState & LENGTH_MASK) == 0)
                {
                    m_listener.onConnectionClosed();
                    printStats();
                    if (tailLock > 0)
                    {
                        m_dataBlockCache.put( m_tail );
                        m_tail = null;
                    }
                }
            }

            if (prev != null)
                m_dataBlockCache.put( dataBlock0 );

            if (dataBlock1 != null)
                m_dataBlockCache.put( dataBlock1 );
        }

        tls.iov[0] = null;
        tls.iov[1] = null;
    }

    public final void start()
    {
        m_collider.executeInSelectorThread( m_starter );
    }

    public final void stop()
    {
        m_collider.executeInSelectorThread( new Stopper1() );
    }
}
