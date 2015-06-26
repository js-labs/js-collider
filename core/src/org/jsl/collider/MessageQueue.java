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

/**
 *                          +----------------+
 *   source (thread) 1 ---> |                |
 *   source (thread) 2 ---> | Message Queue  |---> message processor
 *   source (thread) 3 ---> |                |
 *                          +----------------+
 *
 * Queue implements binary message serialization allowing user thread
 * to process just added message if queue was empty. Data will not be
 * copied in to the queue in this case. If queue was not empty
 * at put() call - message data is copied into the queue and supposed
 * to be handled by the thread which processes the data at this time.
 *
 * Typical usage pattern looks like:
 *
 *     MessageQueue queue;
 *     ByteBuffer msg;
 *
 *     msg = queue.put( msg );
 *     while (msg != null)
 *     {
 *         processMessage( msg );
 *         msg = queue.getNext();
 *     }
 *
 */

package org.jsl.collider;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLongArray;

public class MessageQueue
{
    private static final int MSG_SIZE_SIZE = (Integer.SIZE / Byte.SIZE);
    private static final int WRITE_STATE = 7;
    private static final int BYTES_READY = 15;

    private static final int OFFS_WIDTH    = 20;
    private static final int START_WIDTH   = OFFS_WIDTH;
    private static final int WRITERS_WIDTH = 6;
    private static final long OFFS_MASK    = ((1L << OFFS_WIDTH) - 1);
    private static final long START_MASK   = (OFFS_MASK << OFFS_WIDTH);
    private static final long WRITERS_MASK = (((1L << WRITERS_WIDTH) - 1) << (START_WIDTH + OFFS_WIDTH));

    private final DataBlockCache m_dataBlockCache;
    private final int m_dataBlockSize;
    private final AtomicLongArray m_state;
    private final ByteBuffer [] m_ww;
    private DataBlock m_head;
    private DataBlock m_tail;

    private int m_blockSize;
    private long m_bytesReady;
    private long m_bytesProcessed;
    private int m_rdOffs;

    private int put_i( ByteBuffer msg )
    {
        assert( msg.remaining() > 0 );
        final int blockSize = (MSG_SIZE_SIZE + msg.remaining());
        assert( blockSize <= m_dataBlockSize );

        for (;;)
        {
            long state = m_state.get( WRITE_STATE );
            if (state == -1)
                continue;

            final long offs = (state & OFFS_MASK);
            long space = (m_dataBlockSize - offs);

            if (blockSize > space)
            {
                if ((state & WRITERS_MASK) != 0)
                    continue;

                if (!m_state.compareAndSet(WRITE_STATE, state, -1))
                    continue;

                if (space >= MSG_SIZE_SIZE)
                    m_tail.ww.putInt( (int) offs, 0 );

                m_tail.next = m_dataBlockCache.get(1);
                m_tail = m_tail.next;

                m_ww[0] = m_tail.ww;
                for (int idx=1; idx<WRITERS_WIDTH; idx++)
                    m_ww[idx] = null;

                m_tail.ww.putInt( blockSize - MSG_SIZE_SIZE );
                m_tail.ww.put( msg );

                m_state.set( WRITE_STATE, blockSize );
                return blockSize;
            }

            final long writers = (state & WRITERS_MASK);
            if (writers == WRITERS_MASK)
            {
                /* Reached maximum number of writers, let's try again a bit later. */
                continue;
            }

            long newState = (state & OFFS_MASK);
            newState += blockSize;
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
                continue;

            state = newState;

            ByteBuffer ww = m_ww[writerIdx];
            if (ww == null)
            {
                ww = m_tail.ww.duplicate();
                m_ww[writerIdx] = ww;
            }

            ww.position( (int) offs );
            ww.putInt( blockSize - MSG_SIZE_SIZE );
            ww.put( msg );

            for (;;)
            {
                newState = state;
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
                    newState |= ((offs + blockSize) << OFFS_WIDTH);
                    if (m_state.compareAndSet(WRITE_STATE, state, newState))
                        return blockSize;
                }
                else if (m_state.compareAndSet(WRITE_STATE, state, newState))
                    return 0;

                state = m_state.get( WRITE_STATE );
            }
        }
    }

    private ByteBuffer getRW()
    {
        int msgSize;
        if (((m_rdOffs + MSG_SIZE_SIZE) > m_dataBlockSize) ||
            ((msgSize = ((ByteBuffer)m_head.rw.limit(m_rdOffs+MSG_SIZE_SIZE)).getInt(m_rdOffs)) == 0))
        {
            final DataBlock dataBlock = m_head;
            m_head = dataBlock.next;
            dataBlock.reset();
            m_dataBlockCache.put( dataBlock );
            m_rdOffs = 0;
            assert( m_head.rw.position() == 0 );
            msgSize = m_head.rw.getInt(0);
        }
        m_blockSize = (MSG_SIZE_SIZE + msgSize);
        m_head.rw.limit( m_rdOffs + m_blockSize );
        m_head.rw.position( m_rdOffs + MSG_SIZE_SIZE );
        return m_head.rw;
    }

    public MessageQueue( DataBlockCache dataBlockCache )
    {
        m_dataBlockCache = dataBlockCache;
        m_dataBlockSize = (int) ((dataBlockCache.getBlockSize() <= OFFS_MASK)
                                        ? dataBlockCache.getBlockSize() : OFFS_MASK);
        m_state = new AtomicLongArray( 8*3 );
        m_ww = new ByteBuffer[WRITERS_WIDTH];
        m_head = dataBlockCache.get( 1 );
        m_tail = m_head;
        m_ww[0] = m_tail.ww;
    }

    /**
     * @return ByteBuffer instance to be processed (queue was empty),
     * or <null> if queue was not empty.
     */
    public final ByteBuffer putAndGet( ByteBuffer msg )
    {
        final int msgSize = msg.remaining();
        assert( msgSize > 0 );

        long state = m_state.get( BYTES_READY );
        if (state == 0)
        {
            if (m_state.compareAndSet(BYTES_READY, state, msgSize))
            {
                /* data supposed to be handled by caller */
                m_blockSize = 0;
                m_bytesReady = msgSize;
                m_bytesProcessed = 0;
                return msg;
            }
            state = m_state.get( BYTES_READY );
        }

        final int bytes = put_i( msg );
        if (bytes > 0)
        {
            for (;;)
            {
                final long newState = (state + bytes);
                if (m_state.compareAndSet(BYTES_READY, state, newState))
                {
                    if ((state <= 0) && (newState > 0))
                    {
                        /* data supposed to be handled by caller */
                        m_bytesReady = newState;
                        m_bytesProcessed = 0;
                        return getRW();
                    }
                    return null;
                }
                state = m_state.get( BYTES_READY );
            }
        }
        return null;
    }

    /**
     * @return next data block to be processed,
     * or <null> if queue become empty.
     */
    public final ByteBuffer getNext()
    {
        if (m_blockSize > 0)
        {
            /* Here we should handle quite tricky situation properly:
             *        T1                     T2
             *   add 100 bytes
             *                         add 200 bytes
             *                         bytesReady += 200
             *   bytesReady += 100                (=200)
             *              (=300)
             * So, queue had a 100 bytes, but BYTES_READY increased by 200 first,
             * and m_bytesRemaining can be < 0.
             */
            m_rdOffs += m_blockSize;
            m_bytesProcessed += m_blockSize;

            if (m_bytesProcessed < m_bytesReady)
            {
                /* There are some more messages */
                return getRW();
            }
            else
            {
                for (;;)
                {
                    final long state = m_state.get( BYTES_READY );
                    final long newState = (state - m_bytesProcessed);
                    if (m_state.compareAndSet(BYTES_READY, state, newState))
                    {
                        if (newState <= 0)
                            return null;
                        m_bytesReady = newState;
                        m_bytesProcessed = 0;
                        return getRW();
                    }
                }
            }
        }
        else
        {
            assert( m_bytesReady > 0 );
            assert( m_bytesProcessed == 0 );

            for (;;)
            {
                final long state = m_state.get( BYTES_READY );
                assert( state >= m_bytesReady );

                final long newState = (state - m_bytesReady);
                if (m_state.compareAndSet(BYTES_READY, state, newState))
                {
                    if (newState > 0)
                    {
                        m_bytesReady = newState;
                        return getRW();
                    }
                    return null;
                }
            }
        }
    }
}
