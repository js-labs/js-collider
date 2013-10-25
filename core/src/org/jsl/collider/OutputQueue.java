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
    private static final int OFFS_WIDTH    = 20;
    private static final int START_WIDTH   = 20;
    private static final int WRITERS_WIDTH = 6;
    private static final long OFFS_MASK    = ((1L << OFFS_WIDTH) - 1);
    private static final long START_MASK   = (((1L << START_WIDTH) -1) << OFFS_WIDTH);
    private static final long WRITERS_MASK = (((1L << WRITERS_WIDTH) - 1) << (START_WIDTH + OFFS_WIDTH));

    private final DataBlockCache m_dataBlockCache;
    private final int m_blockSize;
    private final AtomicLong m_state;
    private DataBlock m_head;
    private DataBlock m_tail;
    private final ByteBuffer [] m_ww;

    private long addDataLocked( long state, ByteBuffer data, int dataSize, int bytesRemaining )
    {
        DataBlock head = null;
        DataBlock tail = null;
        try
        {
            head = m_dataBlockCache.getByDataSize( bytesRemaining );
            tail = head;
            for (;;)
            {
                final ByteBuffer ww = tail.ww;
                assert( (ww.capacity() == m_blockSize) && (ww.remaining() == m_blockSize) );
                if (bytesRemaining <= m_blockSize)
                {
                    data.limit( data.position() + bytesRemaining );
                    ww.put( data );
                    break;
                }

                bytesRemaining -= m_blockSize;
                data.limit( data.position() + m_blockSize );
                ww.put( data );
                tail = tail.next;
            }
        }
        finally
        {
            if (data.remaining() == 0)
            {
                for (int idx=1; idx<WRITERS_WIDTH; idx++)
                    m_ww[idx] = null;

                m_tail.next = head;
                m_tail = tail;
                m_ww[0] = tail.ww;

                assert( bytesRemaining <= OFFS_MASK );
                m_state.set( bytesRemaining );
            }
            else
            {
                /* Looks like something went wrong, most probably out of memory.
                 * The best solution is to unlock the queue.
                 * May be we will lucky and application will survive.
                 */
                m_tail.ww.position( (int)(state & OFFS_MASK) );
                m_state.set( state );

                if (head != null)
                {
                    tail = head;
                    while (tail != null)
                    {
                        tail.reset();
                        tail = tail.next;
                    }
                    m_dataBlockCache.put( head );
                }
            }
        }
        return dataSize;
    }

    public OutputQueue( DataBlockCache dataBlockCache )
    {
        m_dataBlockCache = dataBlockCache;
        m_blockSize = dataBlockCache.getBlockSize();
        m_state = new AtomicLong();
        m_head = dataBlockCache.get(1);
        m_tail = m_head;
        m_ww = new ByteBuffer[WRITERS_WIDTH];
        m_ww[0] = m_tail.ww;
    }

    public final long addData( final ByteBuffer data )
    {
        final int dataSize = data.remaining();
        long state = m_state.get();
        for (;;)
        {
            if (state == -1)
            {
                state = m_state.get();
                continue;
            }

            final long offs = (state & OFFS_MASK);
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
                /* Reached maximum number of writers, let's try a bit later. */
                state = m_state.get();
                continue;
            }

            long newState = (state & OFFS_MASK);
            newState += dataSize;
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
                    ww = m_tail.ww.duplicate();
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
                        return ((newState & OFFS_MASK) - start);
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

    public final void addDataFront( final ByteBuffer data )
    {
        final int dataPosition = data.position();
        final int dataLimit = data.limit();
        int bytesRemaining = (dataLimit - dataPosition);
        assert( bytesRemaining > 0 );

        int pos = m_head.rw.position();
        if (pos > 0)
        {
            /* We can not reuse any write window here,
             * not a big problem to allocate a new one locally.
             */
            ByteBuffer ww = m_head.ww.duplicate();
            ww.limit( pos );
            if (bytesRemaining <= pos)
            {
                ww.position( pos - bytesRemaining );
                ww.put( data );
                return;
            }

            bytesRemaining -= pos;
            data.position( dataLimit-pos );
            ww.put( data );
        }

        DataBlock dataBlockList = m_dataBlockCache.getByDataSize( bytesRemaining );
        DataBlock dataBlock = dataBlockList;
        dataBlockList = dataBlockList.next;
        dataBlock.next = m_head;
        for (;;)
        {
            final ByteBuffer ww = dataBlock.ww;
            assert( (ww.capacity() == m_blockSize) && (ww.remaining() == m_blockSize) );

            if (bytesRemaining <= m_blockSize)
            {
                pos = (m_blockSize - bytesRemaining);
                dataBlock.rw.position( pos );
                ww.position( pos );
                data.position( dataPosition );
                data.limit( dataPosition + bytesRemaining );
                ww.put( data );
                break;
            }

            data.position( bytesRemaining - m_blockSize );
            data.limit( bytesRemaining );
            ww.put( data );

            DataBlock prev = dataBlockList;
            dataBlockList = dataBlockList.next;

            prev.next = dataBlock;
            dataBlock = prev;
        }

        m_head = dataBlock;
        data.position( dataLimit );
        data.limit( dataLimit );
    }

    public final long getData( ByteBuffer [] iov, long maxBytes )
    {
        DataBlock dataBlock = m_head;
        int pos = dataBlock.rw.position();

        if (pos == m_blockSize)
        {
            DataBlock next = dataBlock.next;
            assert( next != null );
            assert( next.rw.position() == 0 );
            dataBlock.reset();
            dataBlock.next = null;
            m_dataBlockCache.put( dataBlock );
            dataBlock = next;
            m_head = next;
            pos = 0;
        }

        long bytesRest = maxBytes;
        long ret = 0;
        int idx = 0;

        for (;;)
        {
            int bb = (m_blockSize - pos);
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
        }

        for (; idx<iov.length; idx++)
            iov[idx] = null;

        return ret;
    }

    public final void removeData( int pos0, long bytes )
    {
        assert( pos0 < m_blockSize );
        long bytesRemaining = (bytes - (m_blockSize - pos0));
        if (bytesRemaining > 0)
        {
            DataBlock head = m_head;
            DataBlock dataBlock = m_head;
            for (;;)
            {
                assert( dataBlock.next != null );
                dataBlock.reset();
                DataBlock prev = dataBlock;
                dataBlock = dataBlock.next;
                if (bytesRemaining <= m_blockSize)
                {
                    assert( bytesRemaining == dataBlock.rw.position() );
                    m_head = dataBlock;
                    prev.next = null;
                    m_dataBlockCache.put( head );
                    break;
                }
                bytesRemaining -= m_blockSize;
            }
        }
    }
}
