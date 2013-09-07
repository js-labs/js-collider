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
import java.util.concurrent.atomic.AtomicLong;


public class OutputQueue
{
    private static class DataBlock
    {
        public DataBlock next;
        public final ByteBuffer buf;
        public final ByteBuffer rw;

        public DataBlock( boolean useDirectBuffers, int blockSize )
        {
            next = null;
            if (useDirectBuffers)
                buf = ByteBuffer.allocateDirect( blockSize );
            else
                buf = ByteBuffer.allocate( blockSize );
            rw = buf.duplicate();
            rw.limit(0);
        }
    }

    private static final int OFFS_WIDTH    = 36;
    private static final int START_WIDTH   = 20;
    private static final int WRITERS_WIDTH = 6;
    private static final long OFFS_MASK    = ((1L << OFFS_WIDTH) - 1);
    private static final long START_MASK   = (((1L << START_WIDTH) -1) << OFFS_WIDTH);
    private static final long WRITERS_MASK = (((1L << WRITERS_WIDTH) - 1) << (START_WIDTH + OFFS_WIDTH));

    private boolean m_useDirectBuffers;
    private int m_blockSize;

    private final AtomicLong m_state;
    private DataBlock m_head;
    private DataBlock m_tail;
    private final ByteBuffer [] m_ww;

    private static long getOffs( long state, int blockSize )
    {
        long offs = (state & OFFS_MASK);
        long ret = (offs % blockSize);
        if (ret > 0)
            return ret;
        if (offs > 0)
            return blockSize;
        return 0;
    }

    private long addDataLocked( long state, ByteBuffer data, int dataSize, int bytesRest )
    {
        DataBlock head = null;
        DataBlock tail = null;
        ByteBuffer ww = null;
        try
        {
            head = new DataBlock( m_useDirectBuffers, m_blockSize );
            tail = head;
            for (;;)
            {
                ww = tail.buf.duplicate();
                if (bytesRest <= m_blockSize)
                {
                    data.limit( data.position() + bytesRest );
                    ww.put( data );
                    bytesRest = 0;
                    break;
                }

                data.limit( data.position() + m_blockSize );
                ww.put( data );
                bytesRest -= m_blockSize;

                DataBlock dataBlock = new DataBlock( m_useDirectBuffers, m_blockSize );
                tail.next = dataBlock;
                tail = dataBlock;
            }
        }
        finally
        {
            if (bytesRest == 0)
            {
                for (int idx=0; idx<WRITERS_WIDTH; idx++)
                    m_ww[idx] = null;

                m_tail.next = head;
                m_tail = tail;
                m_ww[0] = ww;

                long newState = (state & OFFS_MASK);
                newState += dataSize;
                if (newState > OFFS_MASK)
                {
                    newState %= m_blockSize;
                    if (newState == 0)
                        newState = m_blockSize;
                }

                m_state.set( newState );
            }
            else
            {
                /* Looks like something went wrong, most probably out of memory.
                 * The best solution is to unlock the queue.
                 * May be we will lucky and application will survive.
                 */
                m_state.set( state );
            }
        }
        return dataSize;
    }

    public OutputQueue( boolean useDirectBuffers, int blockSize )
    {
        m_useDirectBuffers = useDirectBuffers;
        long maxBlockSize = (START_MASK >> OFFS_WIDTH);
        if (blockSize > maxBlockSize)
            m_blockSize = (int) maxBlockSize;
        else
            m_blockSize = blockSize;

        m_state = new AtomicLong();
        m_head = new DataBlock( m_useDirectBuffers, m_blockSize );
        m_tail = m_head;
        m_ww = new ByteBuffer[WRITERS_WIDTH];

        m_ww[0] = m_tail.buf.duplicate();
    }

    public long addData( ByteBuffer data )
    {
        int dataSize = data.remaining();
        long state = m_state.get();
        for (;;)
        {
            if (state == -1)
            {
                state = m_state.get();
                continue;
            }

            final long offs = getOffs( state, m_blockSize );
            long space = (m_blockSize - offs);

            if (dataSize > space)
            {
                if ((state & WRITERS_MASK) != 0)
                {
                    state = m_state.get();
                    continue;
                }

                if (!m_state.compareAndSet(state, -1))
                {
                    state = m_state.get();
                    continue;
                }

                int bytesRest = dataSize;

                if (space > 0)
                {
                    ByteBuffer ww = m_ww[0];
                    ww.position( (int) offs );
                    data.limit( data.position() + (int)space );
                    ww.put( data );
                    bytesRest -= space;
                }

                return addDataLocked( state, data, dataSize, bytesRest );
            }

            final long writers = (state & WRITERS_MASK);
            if (writers == WRITERS_MASK)
            {
                /* Reached maximum number of writers, let's try later. */
                state = m_state.get();
                continue;
            }

            long newState = (state & OFFS_MASK);
            newState += dataSize;
            if (newState > OFFS_MASK)
            {
                newState %= m_blockSize;
                if (newState == 0)
                    newState = m_blockSize;
            }
            newState |= (state & ~OFFS_MASK);

            long writer = (1L << (START_WIDTH + OFFS_WIDTH));
            int writerIdx = 0;
            for (; writerIdx<WRITERS_WIDTH; writerIdx++, writer<<=1)
            {
                if ((state & writer) == 0)
                    break;
            }

            newState |= writer;
            if (writers == 0)
            {
                assert( (state & START_MASK) == 0 );
                newState |= (offs << OFFS_WIDTH);
            }

            if (!m_state.compareAndSet(state, newState))
            {
                state = m_state.get();
                continue;
            }

            state = newState;

            ByteBuffer ww = m_ww[writerIdx];
            if (ww == null)
            {
                try
                {
                    ww = m_tail.buf.duplicate();
                    m_ww[writerIdx] = ww;
                }
                catch (Throwable ex)
                {
                    /* Most probably OutOfMemoryError happened.
                     * We have to recover queue state, otherwise it will remain
                     * in inconsistent state, and then we will send corrupted data.
                     * Not a good idea to swallow the error, but have no other options.
                     * There is at least one write window initialized,
                     * we could use it, just need to wait for some time.
                     */
                    long failedWriter = writer;
                    loop: for (;;)
                    {
                        writerIdx = 0;
                        writer = (1L << (START_WIDTH + OFFS_WIDTH));
                        for (; writerIdx<WRITERS_WIDTH; writerIdx++, writer<<=1)
                        {
                            if (((state & writer) == 0) && (m_ww[writerIdx] != null))
                            {
                                newState = state;
                                newState -= failedWriter;
                                newState |= writer;
                                if (m_state.compareAndSet(state, newState))
                                {
                                    state = newState;
                                    break loop;
                                }
                            }
                        }
                        state = m_state.get();
                    }
                    ww = m_ww[writerIdx];
                }
            }

            ww.position( (int) offs );
            ww.put( data );

            for (;;)
            {
                newState = state;
                newState -= writer;
                long start = ((state & START_MASK) >> OFFS_WIDTH);
                if ((newState & WRITERS_MASK) == 0)
                {
                    newState &= ~START_MASK;
                    if (m_state.compareAndSet(state, newState))
                    {
                        long end = getOffs( newState, m_blockSize );
                        return (end - start);
                    }
                }
                else if (offs == start)
                {
                    newState &= ~START_MASK;
                    newState |= ((offs + dataSize) << OFFS_WIDTH);
                    if (m_state.compareAndSet(state, newState))
                        return dataSize;
                }
                else
                {
                    if (m_state.compareAndSet(state, newState))
                        return 0;
                }
                state = m_state.get();
            }
        }
    }

    public long getData( ByteBuffer [] iov, long maxBytes )
    {
        DataBlock dataBlock = m_head;
        int pos = dataBlock.rw.position();
        int capacity = dataBlock.rw.capacity();

        if (pos == capacity)
        {
            assert( dataBlock.next != null );
            m_head = dataBlock.next;
            dataBlock.next = null;
            dataBlock = m_head;
            pos = dataBlock.rw.position();
            capacity = dataBlock.rw.capacity();
            assert( pos == 0 );
        }

        long bytesRest = maxBytes;
        long ret = 0;
        int idx = 0;

        for (;;)
        {
            int bb = (capacity - pos);
            if (bb > bytesRest)
                bb = (int) bytesRest;

            dataBlock.rw.limit( pos + bb );
            iov[idx] = dataBlock.rw;

            ret += bb;
            bytesRest -= bb;

            if (++idx == iov.length)
                return ret;

            if (bytesRest == 0)
                break;

            assert( dataBlock.next != null );
            dataBlock = dataBlock.next;
            pos = dataBlock.rw.position();
            capacity = dataBlock.rw.capacity();
        }

        for (; idx<iov.length; idx++)
            iov[idx] = null;

        return ret;
    }

    public void removeData( int pos0, long bytes )
    {
        int pos = pos0;
        long bytesRest = bytes;
        for (;;)
        {
            DataBlock dataBlock = m_head;
            int capacity = dataBlock.rw.capacity();
            int rwb = (capacity - pos);
            if (bytesRest <= rwb)
                break;

            assert( dataBlock.next != null );

            bytesRest -= rwb;
            m_head = dataBlock.next;
            dataBlock.next = null;
            pos = 0;
        }
    }
}
