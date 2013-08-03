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
import java.util.concurrent.atomic.AtomicLong;


public class InputQueue extends Collider.SelectorThreadRunnable implements Runnable
{
    private static class DataBlock
    {
        public DataBlock next;
        public ByteBuffer buf;
        public ByteBuffer rw;
        public ByteBuffer ww;

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

    private class DeferredHandler implements Runnable
    {
        public DataBlock dataBlock;

        public void run()
        {
            assert( dataBlock != null );
            long state = m_state.get();
            InputQueue.this.handleData( dataBlock, state );
        }
    }

    private class Stopper extends Collider.SelectorThreadRunnable implements Runnable
    {
        public void runInSelectorThread()
        {
            int interestOps = m_selectionKey.interestOps();
            if ((interestOps & SelectionKey.OP_READ) == 0)
            {
                assert( false );
            }
            else
            {
                interestOps &= ~SelectionKey.OP_READ;
                m_selectionKey.interestOps( interestOps );
                m_collider.executeInThreadPool( this );
            }
        }

        public void run()
        {
            m_session.onReaderStopped();

            long state = m_state.get();
            for (;;)
            {
                assert( (state & CLOSED) == 0);
                long newState = (state | CLOSED);
                if (m_state.compareAndSet(state, newState))
                    break;
                state = m_state.get();
            }

            if ((state & LENGTH_MASK) == 0)
                m_listener.onConnectionClosed();
        }
    }

    private static final ThreadLocal<DataBlock> s_tlsDataBlock = new ThreadLocal<DataBlock>();

    private static final ThreadLocal<ByteBuffer[]> s_tlsIov = new ThreadLocal<ByteBuffer[]>()
    {
        protected ByteBuffer [] initialValue() { return new ByteBuffer[2]; }
    };

    private static final long LENGTH_MASK = 0x00000000FFFFFFFFL;
    private static final long TAIL_LOCK   = 0x0000000100000000L;
    private static final long CLOSED      = 0x0000000200000000L;

    private Collider m_collider;
    private final boolean m_useDirectBuffers;
    private final int m_blockSize;
    private SessionImpl m_session;
    private SocketChannel m_socketChannel;
    private SelectionKey m_selectionKey;
    private Session.Listener m_listener;

    private AtomicLong m_state;
    private DataBlock m_tail;
    private DeferredHandler m_deferredHandler;

    private DataBlock createDataBlock()
    {
        DataBlock dataBlock = s_tlsDataBlock.get();
        if (dataBlock == null)
            dataBlock = new DataBlock( m_useDirectBuffers, m_blockSize );
        else
            s_tlsDataBlock.set( null );
        return dataBlock;
    }

    private void handleData( DataBlock dataBlock, long state )
    {
        ByteBuffer rw = dataBlock.rw;
        assert( rw.position() == rw.limit() );

        long bytesReady = (state & LENGTH_MASK);
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
            s_tlsDataBlock.set( dataBlock );
            dataBlock = next;
            rw = dataBlock.rw;
            pos = 0;
        }

        boolean tailLock;
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

        if ((state & LENGTH_MASK) > 0)
        {
            if (m_deferredHandler == null)
                m_deferredHandler = new DeferredHandler();
            m_deferredHandler.dataBlock = dataBlock;
            m_collider.executeInThreadPool( m_deferredHandler );
        }
        else
        {
            if (tailLock)
            {
                DataBlock tail = m_tail;
                m_tail = null;
                m_state.addAndGet( -TAIL_LOCK );

                DataBlock next = tail.reset();
                assert( next == null );
                s_tlsDataBlock.set( tail );
            }

            if ((state & CLOSED) != 0)
                m_listener.onConnectionClosed();
        }
    }

    public InputQueue(
            Collider collider,
            int blockSize,
            SessionImpl session,
            SocketChannel socketChannel,
            SelectionKey selectionKey,
            Session.Listener listener )
    {
        m_collider = collider;
        m_useDirectBuffers = collider.getConfig().useDirectBuffers;
        m_blockSize = blockSize;
        m_session = session;
        m_socketChannel = socketChannel;
        m_selectionKey = selectionKey;
        m_listener = listener;
        m_state = new AtomicLong();
        m_tail = null;
    }


    public void runInSelectorThread()
    {
        int interestOps = m_selectionKey.interestOps();
        assert( (interestOps & SelectionKey.OP_READ) == 0 );
        m_selectionKey.interestOps( interestOps | SelectionKey.OP_READ );
    }


    public void run()
    {
        ByteBuffer [] iov = s_tlsIov.get();
        int iovCnt;
        DataBlock prev;
        DataBlock dataBlock0;
        DataBlock dataBlock1;

        boolean tailLock;
        long state = m_state.get();
        for (;;)
        {
            if ((state & LENGTH_MASK) == 0)
            {
                tailLock = false;
                break;
            }

            assert( (state & TAIL_LOCK) == 0 );

            long newState = state;
            newState |= TAIL_LOCK;
            tailLock = true;

            if (m_state.compareAndSet(state, newState))
            {
                state = newState;
                break;
            }

            state = m_state.get();
        }

        int pos0;
        if (tailLock)
        {
            dataBlock0 = m_tail;
            pos0 = dataBlock0.ww.position();

            long space = (m_blockSize - pos0);
            if (space > m_blockSize/2)
            {
                prev = null;
                dataBlock1 = null;
                iov[0] = dataBlock0.ww;
                iovCnt = 1;
            }
            else if (space > 0)
            {
                prev = null;
                dataBlock1 = createDataBlock();
                iov[0] = dataBlock0.ww;
                iov[1] = dataBlock1.ww;
                iovCnt = 2;
            }
            else
            {
                prev = dataBlock0;
                dataBlock0 = createDataBlock();
                dataBlock1 = null;
                pos0 = 0;
                iov[0] = dataBlock0.ww;
                iovCnt = 1;
            }
        }
        else
        {
            prev = null;
            dataBlock0 = createDataBlock();
            dataBlock1 = null;
            pos0 = 0;
            iov[0] = dataBlock0.ww;
            iovCnt = 1;
        }

        long bytesReceived;
        try
        {
            bytesReceived = m_socketChannel.read( iov, 0, iovCnt );
            assert( bytesReceived != 0 );
        }
        catch (Exception ignored) { bytesReceived = 0; }

        if (bytesReceived > 0)
        {
            if (prev != null)
                prev.next = dataBlock0;

            if ((state & LENGTH_MASK) == 0)
            {
                for (;;)
                {
                    if ((state & TAIL_LOCK) == 0)
                        break;
                    state = m_state.get();
                }
            }

            if (bytesReceived > (m_blockSize - pos0))
            {
                assert( dataBlock1 != null );
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

                if (tailLock)
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

            /*
             * if (bytesReceived == space)
             *     m_collider.executeInThreadPool( this );
             * else
             *     m_collider.executInSelectorThread( this );
             * Wrong!
             */
            m_collider.executeInSelectorThread( this );

            if ((state & LENGTH_MASK) == bytesReceived)
            {
                handleData( dataBlock0, state );
                if (prev != null)
                    prev.next = null;
            }

            if (dataBlock1 != null)
            {
                assert( dataBlock1.ww.position() == 0 );
                s_tlsDataBlock.set( dataBlock1 );
            }
        }
        else
        {
            m_session.onReaderStopped();

            for (;;)
            {
                long newState = state;
                newState |= CLOSED;

                if (tailLock)
                {
                    assert( (newState & TAIL_LOCK) != 0 );
                    newState -= TAIL_LOCK;
                }

                if (m_state.compareAndSet(state, newState))
                    break;

                state = m_state.get();
            }

            if ((state & LENGTH_MASK) == 0)
            {
                m_listener.onConnectionClosed();
                if (tailLock)
                    m_tail = null;
            }
        }

        iov[0] = null;
        iov[1] = null;
    }

    public void stop()
    {
        m_collider.executeInSelectorThread( new Stopper() );
    }
}
