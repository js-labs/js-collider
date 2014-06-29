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

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLongArray;

public class BinaryQueue
{
    private static final int BLOCK_SIZE_SIZE = (Integer.SIZE / 8);
    private static final int WRITE_STATE = 7;
    private static final int BYTES_READY = 15;

    private static final int OFFS_WIDTH    = 20;
    private static final int START_WIDTH   = 20;
    private static final int WRITERS_WIDTH = 6;
    private static final long OFFS_MASK    = ((1L << OFFS_WIDTH) - 1);
    private static final long START_MASK   = (((1L << START_WIDTH) -1) << OFFS_WIDTH);
    private static final long WRITERS_MASK = (((1L << WRITERS_WIDTH) - 1) << (START_WIDTH + OFFS_WIDTH));

    private final DataBlockCache m_dataBlockCache;
    private final int m_dataBlockSize;
    private final AtomicLongArray m_state;
    private final ByteBuffer [] m_ww;
    private DataBlock m_head;
    private DataBlock m_tail;

    private long m_bytesRemaining;
    private long m_bytesProcessed;
    private int m_rdOffs;

    private int put_i( ByteBuffer data )
    {
        final int dataSize = (BLOCK_SIZE_SIZE + data.remaining());
        assert( dataSize <= m_dataBlockSize );

        long state = m_state.get( WRITE_STATE );
        for (;;)
        {
            if (state == -1)
            {
                state = m_state.get( WRITE_STATE );
                continue;
            }

            final long offs = (state & OFFS_MASK);
            long space = (m_dataBlockSize - offs);

            if (dataSize > space)
            {
                if ((state & WRITERS_MASK) != 0)
                {
                    state = m_state.get( WRITE_STATE );
                    continue;
                }

                if (!m_state.compareAndSet(WRITE_STATE, state, -1))
                {
                    state = m_state.get( WRITE_STATE );
                    continue;
                }

                if (space >= BLOCK_SIZE_SIZE)
                    m_tail.ww.putInt( (int) offs, 0 );

                m_tail.next = m_dataBlockCache.get(1);
                m_tail = m_tail.next;
                m_ww[0] = m_tail.ww;
                for (int idx=1; idx<WRITERS_WIDTH; idx++)
                    m_ww[idx] = null;

                m_tail.ww.putInt( dataSize - BLOCK_SIZE_SIZE );
                m_tail.ww.put( data );

                m_state.set( WRITE_STATE, dataSize );
                return dataSize;
            }

            final long writers = (state & WRITERS_MASK);
            if (writers == WRITERS_MASK)
            {
                /* Reached maximum number of writers, let's try again a bit later. */
                state = m_state.get( WRITE_STATE );
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

            if (!m_state.compareAndSet(WRITE_STATE, state, newState))
            {
                state = m_state.get( WRITE_STATE );
                continue;
            }

            state = newState;

            ByteBuffer ww = m_ww[writerIdx];
            if (ww == null)
            {
                ww = m_tail.ww.duplicate();
                m_ww[writerIdx] = ww;
            }

            ww.position( (int) offs );
            ww.putInt( dataSize - BLOCK_SIZE_SIZE );
            ww.put( data );

            return removeWriter( state, writer, offs, dataSize );
        }
    }

    private int removeWriter( long state, long writer, long offs, int dataSize )
    {
        for (;;)
        {
            long newState = state;
            newState -= writer;
            final long start = ((state & START_MASK) >> OFFS_WIDTH);
            if ((newState & WRITERS_MASK) == 0)
            {
                newState &= ~START_MASK;
                if (m_state.compareAndSet(WRITE_STATE, state, newState))
                {
                    long cc = (newState & OFFS_MASK);
                    return (int) (cc - start);
                }
            }
            else if (offs == start)
            {
                newState &= ~START_MASK;
                newState |= ((offs + dataSize) << OFFS_WIDTH);
                if (m_state.compareAndSet(WRITE_STATE, state, newState))
                    return dataSize;
            }
            else if (m_state.compareAndSet(WRITE_STATE, state, newState))
                return 0;

            state = m_state.get( WRITE_STATE );
        }
    }

    private ByteBuffer getRW()
    {
        int blockSize;
        if (((m_rdOffs + BLOCK_SIZE_SIZE) > m_dataBlockSize) ||
            ((blockSize = ((ByteBuffer)m_head.rw.limit(m_rdOffs+BLOCK_SIZE_SIZE).position(m_rdOffs)).getInt()) == 0))
        {
            final DataBlock dataBlock = m_head;
            m_head = dataBlock.getNextAndReset();
            m_dataBlockCache.put( dataBlock );
            m_rdOffs = 0;
            assert( m_head.rw.position() == 0 );
            blockSize = m_head.rw.getInt();
        }
        m_head.rw.limit( m_rdOffs + BLOCK_SIZE_SIZE + blockSize );
        return m_head.rw;
    }

    public BinaryQueue( DataBlockCache dataBlockCache )
    {
        m_dataBlockCache = dataBlockCache;
        m_dataBlockSize =  dataBlockCache.getBlockSize();
        m_state = new AtomicLongArray( 8*3 );
        m_ww = new ByteBuffer[WRITERS_WIDTH];
        m_head = dataBlockCache.get( 1 );
        m_tail = m_head;
        m_ww[0] = m_tail.ww;
    }

    public ByteBuffer putDataNoCopy( ByteBuffer data )
    {
        final int dataSize = data.remaining();
        long state = m_state.get( BYTES_READY );
        for (;;)
        {
            if (state == 0)
            {
                long newState = (state + dataSize);
                if (m_state.compareAndSet(BYTES_READY, 0, newState))
                {
                    m_bytesRemaining = -newState;
                    return data;
                }
                state = m_state.get( BYTES_READY );
            }
            else
            {
                final int bytesReady = put_i( data );
                if (bytesReady > 0)
                {
                    for (;;)
                    {
                        long newState = (state + bytesReady);
                        if (m_state.compareAndSet(BYTES_READY, state, newState))
                        {
                            if (state == 0)
                            {
                                m_bytesRemaining = newState;
                                return getRW();
                            }
                            return null;
                        }
                        state = m_state.get( BYTES_READY );
                    }
                }
                else
                    return null;
            }
        }
    }

    public ByteBuffer putData( ByteBuffer data )
    {
        final int bytesReady = put_i( data );
        if (bytesReady > 0)
        {
            long state = m_state.get( BYTES_READY );
            for (;;)
            {
                final long newState = (state + bytesReady);
                if (m_state.compareAndSet(BYTES_READY, state, newState))
                {
                    if (state == 0)
                    {
                        m_bytesRemaining = newState;
                        return getRW();
                    }
                    return null;
                }
                state = m_state.get( BYTES_READY );
            }
        }
        else
            return null;
    }

    public ByteBuffer getNext()
    {
        if (m_bytesRemaining > 0)
        {
            final long blockSize = (BLOCK_SIZE_SIZE + m_head.rw.getInt(m_rdOffs));
            assert( blockSize <= m_bytesRemaining );
            m_rdOffs += blockSize;
            m_bytesProcessed += blockSize;
            m_bytesRemaining -= blockSize;

            if (m_bytesRemaining == 0)
            {
                long bytesProcessed = m_bytesProcessed;
                m_bytesProcessed = 0;
                long state = m_state.get( BYTES_READY );
                for (;;)
                {
                    assert( bytesProcessed <= state );
                    final long newState = (state - bytesProcessed);
                    if (m_state.compareAndSet(BYTES_READY, state, newState))
                    {
                        if (newState == 0)
                            return null;
                        m_bytesRemaining = newState;
                        break;
                    }
                    state = m_state.get( BYTES_READY );
                }
            }
        }
        else
        {
            assert (m_bytesRemaining < 0);
            assert (m_bytesProcessed == 0);

            final long blockSize = -m_bytesRemaining;
            m_bytesRemaining = 0;

            long state = m_state.get( BYTES_READY );
            for (;;)
            {
                final long newState = (state - blockSize);
                if (m_state.compareAndSet(BYTES_READY, state, newState))
                {
                    if (newState == 0)
                        return null;
                    m_bytesRemaining = newState;
                    break;
                }
                state = m_state.get( BYTES_READY );
            }
        }

        return getRW();
    }
}
