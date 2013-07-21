/*
 * JS-Collider framework.
 * Copyright (C) 2013 Sergey Zubarev
 * info@js-labs.org
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
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
        private ByteBuffer m_buf;
        public ByteBuffer rw;
        public ByteBuffer ww;

        public DataBlock( int size )
        {
            next = null;
            m_buf = ByteBuffer.allocateDirect( size );
            rw = m_buf.duplicate();
            ww = m_buf.duplicate();
            rw.limit(0);
        }
    }

    private final int OFFS_WIDTH = 36;
    private final int START_WIDTH = 20;
    private final int WRITERS_WIDTH = 6; /* even 64 concurrent threads looks unlikely */
    private final long OFFS_MASK    = ((1L << OFFS_WIDTH) - 1);
    private final long START_MASK   = (((1L << START_WIDTH) -1) << OFFS_WIDTH);
    private final long WRITERS_MASK = (((1L << WRITERS_WIDTH) - 1) << (START_WIDTH + OFFS_WIDTH));
    private final long WRITER       = (1L << (START_WIDTH+OFFS_WIDTH));

    private int m_blockSize;
    private AtomicLong m_state;
    private DataBlock m_head;
    private DataBlock m_tail;

    public OutputQueue( int blockSize )
    {
        m_blockSize = blockSize;
        m_state = new AtomicLong();
        m_head = new DataBlock( blockSize );
        m_tail = m_head;
    }

    public long addData( ByteBuffer data )
    {
        int dataRemaining = data.remaining();
        long state = m_state.get();
        for (;;)
        {
            if (state == -1)
            {
                state = m_state.get();
                continue;
            }

            final long offs = ((state & OFFS_MASK) % m_blockSize);
            long space = (m_blockSize - offs);
            assert( m_tail.ww.remaining() == space );

            if (dataRemaining > space)
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

                data.limit( data.position() + (int)space );
                m_tail.ww.put( data );

                int bytesRest = (dataRemaining - (int)space);
                while (bytesRest > m_blockSize)
                {
                    DataBlock dataBlock = new DataBlock( m_blockSize );
                    data.limit( data.position() + m_blockSize );
                    dataBlock.ww.put( data );
                    m_tail.next = dataBlock;
                    m_tail = dataBlock;
                    bytesRest -= m_blockSize;
                }

                DataBlock dataBlock = new DataBlock( m_blockSize );
                data.limit( data.position() + bytesRest );
                dataBlock.ww.put( data );
                m_tail.next = dataBlock;
                m_tail = dataBlock;

                assert( data.remaining() == 0 );

                long newState = (state & OFFS_MASK);
                newState += dataRemaining;

                if (newState > OFFS_MASK)
                    newState %= m_blockSize;

                boolean res = m_state.compareAndSet( -1, newState );
                assert( res );

                return dataRemaining;
            }

            long writers = (state & WRITERS_MASK);
            if (writers == WRITERS_MASK)
            {
                /* Reached maximum number of writers, let's try later. */
                state = m_state.get();
                continue;
            }

            long newState = (state & OFFS_MASK);
            newState += dataRemaining;
            if (newState > OFFS_MASK)
                newState %= m_blockSize;
            newState |= (state & ~OFFS_MASK);
            newState += WRITER;

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

            m_tail.ww.put( data );

            state = newState;
            for (;;)
            {
                assert( (state & WRITERS_MASK) != 0 );

                newState = (state - WRITER);
                if ((newState & WRITERS_MASK) == 0)
                {
                    long start = ((newState >> OFFS_WIDTH) & START_MASK);
                    newState &= ~(START_MASK << OFFS_WIDTH);
                    if (m_state.compareAndSet(state, newState))
                    {
                        long end = ((newState & OFFS_MASK) % m_blockSize);
                        return (end - start);
                    }
                }
                else if (offs == ((newState >> OFFS_WIDTH) & START_MASK))
                {
                    newState &= ~(OFFS_MASK << OFFS_WIDTH);
                    newState |= ((offs + dataRemaining) << OFFS_WIDTH);
                    if (m_state.compareAndSet(state, newState))
                        return dataRemaining;
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
}
