/*
 * Copyright (C) 2013 Sergey Zubarev, info@js-labs.org
 *
 * This file is a part of JS-Collider test.
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

package org.jsl.tests;

import org.jsl.collider.DataBlock;
import org.jsl.collider.DataBlockCache;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;


public class BinaryQueue
{
    private static final int OFFS_WIDTH    = 20;
    private static final int START_WIDTH   = 20;
    private static final int WRITERS_WIDTH = 6;
    private static final long OFFS_MASK    = ((1L << OFFS_WIDTH) - 1);
    private static final long START_MASK   = (((1L << START_WIDTH) -1) << OFFS_WIDTH);
    private static final long WRITERS_MASK = (((1L << WRITERS_WIDTH) - 1) << (START_WIDTH + OFFS_WIDTH));

    private static final AtomicLongFieldUpdater<BinaryQueue> s_stateUpdater =
            AtomicLongFieldUpdater.newUpdater( BinaryQueue.class, "m_state" );

    private final DataBlockCache m_dataBlockCache;
    private final int m_blockSize;
    private volatile long m_state;
    private DataBlock m_head;
    private DataBlock m_tail;
    private final ByteBuffer [] m_wr;

    private int putDataLocked( final long state, final ByteBuffer data, final int dataSize, int bytesRemaining )
    {
        DataBlock head = null;
        int ret = -1;
        try
        {
            head = m_dataBlockCache.getByDataSize( bytesRemaining );
            DataBlock tail = head;
            for (;;)
            {
                final ByteBuffer wr = tail.wr;
                assert((wr.position() == 0) &&
                       (wr.capacity() == m_blockSize) &&
                       (wr.remaining() == m_blockSize));
                if (bytesRemaining <= m_blockSize)
                {
                    data.limit(data.position() + bytesRemaining);
                    wr.put(data);
                    break;
                }
                bytesRemaining -= m_blockSize;
                data.limit(data.position() + m_blockSize);
                wr.put(data);
                tail = tail.next;
            }

            for (int idx=1; idx<WRITERS_WIDTH; idx++)
                m_wr[idx] = null;

            m_tail.next = head;
            m_tail = tail;
            m_wr[0] = tail.wr;

            s_stateUpdater.set( this, bytesRemaining );
            ret = dataSize;
        }
        finally
        {
            if (ret < 0)
            {
                /* Looks like something went wrong, most probably out of memory.
                 * The best solution is to unlock the queue.
                 * May be we will lucky and application will survive.
                 */
                m_tail.wr.position( (int)(state & OFFS_MASK) );
                s_stateUpdater.set( this, state );

                if (head != null)
                {
                    DataBlock tail = head;
                    while (tail != null)
                    {
                        tail.reset();
                        tail = tail.next;
                    }
                    m_dataBlockCache.put( head );
                }
            }
        }
        return ret;
    }

    private int putIntLocked( final long state, final int value, final int bytesRemaining )
    {
        DataBlock dataBlock = null;
        int ret = -1;
        try
        {
            dataBlock = m_dataBlockCache.getByDataSize( bytesRemaining );
            final ByteBuffer wr = dataBlock.wr;
            assert((wr.position() == 0) &&
                   (wr.capacity() == m_blockSize) &&
                   (wr.remaining() == m_blockSize));

            if (bytesRemaining == 4)
                wr.putInt(value);
            else if (wr.order() == ByteOrder.LITTLE_ENDIAN)
            {
                int shift = ((4 - bytesRemaining) * 8);
                for (int cc=bytesRemaining;;)
                {
                    wr.put((byte) ((value >> shift) & 0xFF));
                    if (--cc == 0)
                        break;
                    shift += 8;
                }
            }
            else if (wr.order() == ByteOrder.BIG_ENDIAN)
            {
                int shift = ((bytesRemaining - 1) * 8);
                for (int cc=bytesRemaining;;)
                {
                    wr.put((byte) ((value >> shift) & 0xFF));
                    if (--cc == 0)
                        break;
                    shift -= 8;
                }
            }
            else
                assert(false);

            for (int idx=1; idx<WRITERS_WIDTH; idx++)
                m_wr[idx] = null;

            m_tail.next = dataBlock;
            m_tail = dataBlock;
            m_wr[0] = dataBlock.wr;

            s_stateUpdater.set(this, bytesRemaining);
            ret = 4;
        }
        finally
        {
            if (ret < 0)
            {
                /* Looks like something went wrong, most probably out of memory.
                 * The best solution is to unlock the queue.
                 * May be we will lucky and application will survive.
                 */
                m_tail.wr.position((int)(state & OFFS_MASK));
                s_stateUpdater.set(this, state);

                if (dataBlock != null)
                    m_dataBlockCache.put(dataBlock.reset());
            }
        }
        return ret;
    }

    private int removeWriter( final long writer, final long offs, final int dataSize, long state )
    {
        for (;;)
        {
            long newState = state;
            newState -= writer;
            final long start = ((state & START_MASK) >> OFFS_WIDTH);
            if ((newState & WRITERS_MASK) == 0)
            {
                newState &= ~START_MASK;
                if (s_stateUpdater.compareAndSet(this, state, newState))
                {
                    int cc = (int) (newState & OFFS_MASK);
                    return (cc - (int)start);
                }
            }
            else if (offs == start)
            {
                newState &= ~START_MASK;
                newState |= ((offs + dataSize) << OFFS_WIDTH);
                if (s_stateUpdater.compareAndSet(this, state, newState))
                    return dataSize;
            }
            else
            {
                if (s_stateUpdater.compareAndSet(this, state, newState))
                    return 0;
            }
            state = s_stateUpdater.get( this );
        }
    }

    public BinaryQueue( DataBlockCache dataBlockCache )
    {
        m_dataBlockCache = dataBlockCache;
        m_blockSize = dataBlockCache.getBlockSize();
        m_state = 0;
        m_head = dataBlockCache.get(1);
        m_tail = m_head;
        m_wr = new ByteBuffer[WRITERS_WIDTH];
        m_wr[0] = m_tail.wr;
    }

    public final int putData( final ByteBuffer data )
    {
        final int dataSize = data.remaining();
        long state = s_stateUpdater.get( this );
        for (;;)
        {
            if (state == -1)
            {
                state = s_stateUpdater.get( this );
                continue;
            }

            final long offs = (state & OFFS_MASK);
            long space = (m_blockSize - offs);

            if (dataSize > space)
            {
                if ((state & WRITERS_MASK) != 0)
                {
                    state = s_stateUpdater.get( this );
                    continue;
                }

                if (!s_stateUpdater.compareAndSet(this, state, -1))
                {
                    state = s_stateUpdater.get( this );
                    continue;
                }

                int bytesRest = dataSize;

                if (space > 0)
                {
                    final ByteBuffer wr = m_wr[0];
                    wr.position((int) offs);
                    data.limit(data.position() + (int)space);
                    wr.put(data);
                    bytesRest -= space;
                }

                return putDataLocked(state, data, dataSize, bytesRest);
            }

            final long writers = (state & WRITERS_MASK);
            if (writers == WRITERS_MASK)
            {
                /* Reached maximum number of writers, let's try a bit later. */
                state = s_stateUpdater.get( this );
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

            if (!s_stateUpdater.compareAndSet(this, state, newState))
            {
                state = s_stateUpdater.get( this );
                continue;
            }

            state = newState;

            ByteBuffer wr = m_wr[writerIdx];
            if (wr == null)
            {
                try
                {
                    wr = m_tail.wr.duplicate();
                    m_wr[writerIdx] = wr;
                }
                catch (Throwable ex)
                {
                    /* Most probably OutOfMemoryError.
                     * We have to recover queue state, otherwise it will remain
                     * in inconsistent state, and then we will send corrupted data.
                     * Not a good idea to swallow the error, but have no other options.
                     * There is at least one write window initialized,
                     * we could use it, just need to wait for some time.
                     */
                    final long failedWriter = writer;
                    loop: for (;;)
                    {
                        writerIdx = 0;
                        writer = (1L << (START_WIDTH + OFFS_WIDTH));
                        for (; writerIdx<WRITERS_WIDTH; writerIdx++, writer<<=1)
                        {
                            if (((state & writer) == 0) && (m_wr[writerIdx] != null))
                            {
                                newState = state;
                                newState -= failedWriter;
                                newState |= writer;
                                if (s_stateUpdater.compareAndSet(this, state, newState))
                                {
                                    state = newState;
                                    break loop;
                                }
                            }
                        }
                        state = s_stateUpdater.get(this);
                    }
                    wr = m_wr[writerIdx];
                }
            }

            wr.position((int) offs);
            wr.put(data);

            return removeWriter(writer, offs, dataSize, state);
        }
    }

    public final int putInt( final int value )
    {
        long state = s_stateUpdater.get( this );
        for (;;)
        {
            if (state == -1)
            {
                state = s_stateUpdater.get( this );
                continue;
            }

            final long offs = (state & OFFS_MASK);
            final int space = (m_blockSize - (int)offs);

            if (space < 4)
            {
                if ((state & WRITERS_MASK) != 0)
                {
                    state = s_stateUpdater.get( this );
                    continue;
                }

                if (!s_stateUpdater.compareAndSet(this, state, -1))
                {
                    state = s_stateUpdater.get( this );
                    continue;
                }

                if (space > 0)
                {
                    final ByteBuffer wr = m_wr[0];
                    wr.position((int) offs);
                    if (wr.order() == ByteOrder.LITTLE_ENDIAN)
                    {
                        for (int cc=space, shift=0;;)
                        {
                            wr.put((byte) ((value >> shift) & 0xFF));
                            if (--cc == 0)
                                break;
                            shift += 8;
                        }
                    }
                    else if (wr.order() == ByteOrder.BIG_ENDIAN)
                    {
                        for (int cc=space, shift=24;;)
                        {
                            wr.put((byte) ((value >> shift) & 0xFF));
                            if (--cc == 0)
                                break;
                            shift -= 8;
                        }
                    }
                    else
                        assert( false );
                }

                return putIntLocked(state, value, 4-space);
            }

            final long writers = (state & WRITERS_MASK);
            if (writers == WRITERS_MASK)
            {
                /* Reached maximum number of writers, let's try a bit later. */
                state = s_stateUpdater.get( this );
                continue;
            }

            long newState = (state + 4);
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

            if (!s_stateUpdater.compareAndSet(this, state, newState))
            {
                state = s_stateUpdater.get(this);
                continue;
            }

            state = newState;

            ByteBuffer wr = m_wr[writerIdx];
            if (wr == null)
            {
                try
                {
                    wr = m_tail.wr.duplicate();
                    m_wr[writerIdx] = wr;
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
                    final long failedWriter = writer;
                    loop: for (;;)
                    {
                        writerIdx = 0;
                        writer = (1L << (START_WIDTH + OFFS_WIDTH));
                        for (; writerIdx<WRITERS_WIDTH; writerIdx++, writer<<=1)
                        {
                            if (((state & writer) == 0) && (m_wr[writerIdx] != null))
                            {
                                newState = state;
                                newState -= failedWriter;
                                newState |= writer;
                                if (s_stateUpdater.compareAndSet(this, state, newState))
                                {
                                    state = newState;
                                    break loop;
                                }
                            }
                        }
                        state = s_stateUpdater.get(this);
                    }
                    wr = m_wr[writerIdx];
                }
            }

            wr.position((int) offs);
            wr.putInt(value);

            return removeWriter(writer, offs, 4, state);
        }
    }

    public final long getData( ByteBuffer [] iov, long maxBytes )
    {
        DataBlock dataBlock = m_head;
        int pos = dataBlock.rd.position();

        if (pos == m_blockSize)
        {
            final DataBlock next = dataBlock.next;
            assert( next != null );
            assert( next.rd.position() == 0 );
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

            dataBlock.rd.limit(pos + bb);
            iov[idx] = dataBlock.rd;

            ret += bb;
            bytesRest -= bb;

            if (++idx == iov.length)
                return ret;

            if (bytesRest == 0)
                break;

            assert( dataBlock.next != null );
            dataBlock = dataBlock.next;
            pos = dataBlock.rd.position();
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
                    assert(bytesRemaining == dataBlock.rd.position());
                    m_head = dataBlock;
                    prev.next = null;
                    m_dataBlockCache.put( head );
                    break;
                }
                assert((dataBlock.rd.position() == dataBlock.rd.capacity()) &&
                       (dataBlock.rd.limit() == dataBlock.rd.capacity()));
                bytesRemaining -= m_blockSize;
            }
        }
    }
}
