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
        }
    }

    private final int OFFS_WIDTH = 28;
    private final int WRITERS_WIDTH = 6;

    private final long OFFS_MASK   = ((1 << OFFS_WIDTH) - 1);
    private final long WRITER_MASK = (((1 << WRITERS_WIDTH) - 1) << (OFFS_WIDTH * 2));
    private final long WRITER      = (1 << OFFS_WIDTH*2);
    private final long LOCK        = (1 << (WRITERS_WIDTH + OFFS_WIDTH*2));

    private int m_blockSize;
    private AtomicLong m_state;
    private DataBlock m_head;
    private DataBlock m_tail;

    public OutputQueue( int blockSize )
    {
        m_blockSize = blockSize;
        m_state = new AtomicLong();
    }

    public long addData( ByteBuffer data )
    {
        int dataSize = data.remaining();
        long state = m_state.get();
        for (;;)
        {
            if ((state & LOCK) != 0)
            {
                state = m_state.get();
                continue;
            }

            final long offs = (state & OFFS_MASK);
            long space = (m_blockSize - offs);
            if (dataSize > space)
            {
                if ((state & WRITER_MASK) != 0)
                {
                    /* Some threads still writing, can't lock */
                    state = m_state.get();
                    continue;
                }

                long newState = (state | LOCK);
                if (!m_state.compareAndSet(state, newState))
                {
                    state = m_state.get();
                    continue;
                }

                /*
                ACE_OS::memcpy( m_tail->get_wr_ptr(offs), data, space );

                data = ((const char*)data) + space;
                long bytesRest = static_cast<long>( size );
                bytesRest -= space;

                Spare * spare = this->cache_get( bytesRest );
                m_tail->set_next( spare );
                while (1)
                {
                    if (bytesRest > SPARE_CAPACITY)
                    {
                        ACE_OS::memcpy( spare->get_wr_ptr(0), data, SPARE_CAPACITY );
                        data = ((const char*)data) + SPARE_CAPACITY;
                        bytesRest -= SPARE_CAPACITY;
                        spare = spare->get_next();
                    }
                    else
                    {
                        ACE_OS::memcpy( spare->get_wr_ptr(0), data, bytesRest );
                        break;
                    }
                }

                m_tail = spare;
                cpphelper::atomic_swap( &m_offsAndFlags, bytesRest );
                return static_cast<long>( size );
                */
            }
            break;
        }
        return 0;
    }
}
