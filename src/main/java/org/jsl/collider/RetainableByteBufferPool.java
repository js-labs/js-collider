/*
 * Copyright (C) 2015 Sergey Zubarev, info@js-labs.org
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
import java.nio.ByteOrder;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.logging.Level;
import java.util.logging.Logger;

public class RetainableByteBufferPool
{
    private static class Chunk
    {
        private final static AtomicIntegerFieldUpdater<Chunk> s_retainCountUpdater =
                AtomicIntegerFieldUpdater.newUpdater(Chunk.class, "m_retainCount");

        private final static AtomicReferenceFieldUpdater<Chunk, Chunk> s_nextUpdater =
                AtomicReferenceFieldUpdater.newUpdater(Chunk.class, Chunk.class, "m_next");

        private final ChunkCache m_cache;
        private final ByteBuffer m_byteBuffer;
        private volatile int m_retainCount;
        private volatile Chunk m_next;

        Chunk(ChunkCache cache, ByteBuffer byteBuffer)
        {
            m_cache = cache;
            m_byteBuffer = byteBuffer;
            m_retainCount = (byteBuffer.capacity() + 1);
        }

        public ByteBuffer getByteBuffer()
        {
            return m_byteBuffer;
        }

        public void release(int bytes)
        {
            for (;;)
            {
                final int retainCount = m_retainCount;
                assert(retainCount >= bytes);
                if (s_retainCountUpdater.compareAndSet(this, retainCount, retainCount-bytes))
                {
                    if (retainCount == bytes)
                    {
                        s_retainCountUpdater.lazySet(this, m_byteBuffer.capacity()+1);
                        if (m_cache != null)
                            m_cache.put(this);
                    }
                    break;
                }
            }
        }

        public static Chunk getNext(Chunk chunk)
        {
            return s_nextUpdater.get(chunk);
        }

        public static void lazySetNext(Chunk chunk, Chunk next)
        {
            s_nextUpdater.lazySet(chunk, next);
        }
    }

    // On chunk switch pool will be locked,
    // so we can use SCMP based implementation for the cache.

    private static class ChunkCache
    {
        private final static AtomicIntegerFieldUpdater<ChunkCache> s_sizeUpdater =
                AtomicIntegerFieldUpdater.newUpdater(ChunkCache.class, "m_size");

        private final static AtomicReferenceFieldUpdater<ChunkCache, Chunk> s_tailUpdater =
                AtomicReferenceFieldUpdater.newUpdater(ChunkCache.class, Chunk.class, "m_tail");

        private final int m_bufferCapacity;
        private final boolean m_useDirectBuffer;
        private final ByteOrder m_byteOrder;
        private final int m_maxSize;
        private final int m_initialSize;
        private Chunk m_head;
        private volatile Chunk m_tail;
        private volatile int m_size;

        ChunkCache(int bufferCapacity, boolean useDirectBuffer, ByteOrder byteOrder, int maxSize, int initialSize)
        {
            m_bufferCapacity = bufferCapacity;
            m_useDirectBuffer = useDirectBuffer;
            m_byteOrder = byteOrder;
            m_maxSize = maxSize;
            m_initialSize = ((initialSize < maxSize) ? initialSize : maxSize);

            if (m_initialSize > 0)
            {
                Chunk head = alloc(m_useDirectBuffer, m_bufferCapacity, m_byteOrder, this);
                Chunk tail = head;
                int cc = (m_initialSize - 1);
                for (; cc>0; cc--)
                {
                    final Chunk chunk = alloc(m_useDirectBuffer, m_bufferCapacity, m_byteOrder, this);
                    Chunk.lazySetNext(tail, chunk);
                    tail = chunk;
                }
                m_head = head;
                s_tailUpdater.lazySet(this, tail);
                s_sizeUpdater.lazySet(this, m_initialSize);
            }
        }

        Chunk alloc(int size)
        {
            return alloc(m_useDirectBuffer, size, m_byteOrder, null);
        }

        static Chunk alloc(boolean useDirectBuffer, int size, ByteOrder byteOrder, ChunkCache cache)
        {
            final ByteBuffer byteBuffer =
                    useDirectBuffer
                            ? ByteBuffer.allocateDirect(size)
                            : ByteBuffer.allocate(size);
            byteBuffer.order(byteOrder);
            return new Chunk(cache, byteBuffer);
        }

        Chunk get()
        {
            final Chunk chunk = m_head;
            if (chunk == null)
                return alloc(m_useDirectBuffer, m_bufferCapacity, m_byteOrder, this);

            Chunk next = Chunk.getNext(chunk);
            if (next == null)
            {
                m_head = null;
                if (!s_tailUpdater.compareAndSet(this, chunk, null))
                {
                    while ((next = Chunk.getNext(chunk)) == null);
                    Chunk.lazySetNext(chunk, null);
                    m_head = next;
                }
            }
            else
            {
                Chunk.lazySetNext(chunk, null);
                m_head = next;
            }

            s_sizeUpdater.decrementAndGet(this);
            return chunk;
        }

        public void put(Chunk chunk)
        {
            for (;;)
            {
                final int size = s_sizeUpdater.get(this);
                if (size == m_maxSize)
                    return;
                if (s_sizeUpdater.compareAndSet(this, size, size+1))
                    break;
            }

            final Chunk tail = s_tailUpdater.getAndSet(this, chunk);
            if (tail == null)
                m_head = chunk;
            else
                Chunk.s_nextUpdater.set(tail, chunk);
        }

        public void release(Logger logger)
        {
            final int size = s_sizeUpdater.get(this);
            if (size < m_initialSize)
            {
                if (logger.isLoggable(Level.WARNING))
                {
                    logger.log(Level.WARNING, "RetainableByteBufferPool[" + m_bufferCapacity + "]: " +
                            " size on release (" + size + ") is less than initial size (" + m_initialSize + ")");
                }
            }
            s_sizeUpdater.lazySet(this, 0);

            Chunk chunk = m_head;
            while (chunk != null)
            {
                final Chunk next = Chunk.getNext(chunk);
                Chunk.lazySetNext(chunk, null);
                chunk = next;
            }
        }
    }

    private static class BufferImpl extends RetainableByteBuffer
    {
        private final Chunk m_chunk;
        private final int m_offs;
        private final int m_capacity;
        private final int m_reservedSize;

        BufferImpl(Chunk chunk, int offs, int capacity, int reservedSize)
        {
            super(chunk.getByteBuffer().duplicate());
            m_buf.position(offs);
            m_buf.limit(offs + capacity);
            m_chunk = chunk;
            m_offs = offs;
            m_capacity = capacity;
            m_reservedSize = reservedSize;
        }

        protected void finalRelease()
        {
            m_chunk.release( m_reservedSize );
        }

        public String toString()
        {
            String ret = super.toString();
            ret += " [" + m_chunk.toString() + " reserved=" + m_reservedSize + "]";
            return ret;
        }

        /* NIO Buffer */

        public int capacity()
        {
            return m_capacity;
        }

        public RetainableByteBuffer clear()
        {
            m_buf.clear();
            m_buf.position(m_offs);
            m_buf.limit(m_offs + m_capacity);
            return this;
        }

        public RetainableByteBuffer flip()
        {
            final int position = m_buf.position();
            m_buf.position(m_offs);
            m_buf.limit(position);
            return this;
        }

        public int limit()
        {
            return (m_buf.limit() - m_offs);
        }

        public RetainableByteBuffer limit(int newLimit)
        {
            if ((newLimit > m_capacity) || (newLimit < 0))
                throw new IllegalArgumentException();
            m_buf.limit(m_offs + newLimit);
            return this;
        }

        public int position()
        {
            return (m_buf.position() - m_offs);
        }

        public RetainableByteBuffer position(int position)
        {
            m_buf.position(m_offs + position);
            return this;
        }

        public RetainableByteBuffer rewind()
        {
            m_buf.position(m_offs);
            return this;
        }

        /* NIO ByteBuffer */

        public byte get(int index)
        {
            return m_buf.get(m_offs + index);
        }

        public RetainableByteBuffer put(int index, byte value)
        {
            m_buf.put(index, value);
            return this;
        }

        public int getInt(int index)
        {
            return m_buf.getInt(m_offs + index);
        }

        public RetainableByteBuffer putInt( int index, int value )
        {
            m_buf.putInt( m_offs + index, value );
            return this;
        }

        public short getShort( int index )
        {
            return m_buf.getShort(m_offs + index);
        }

        public RetainableByteBuffer putShort( int index, short value )
        {
            m_buf.putShort( m_offs + index, value );
            return this;
        }

        public float getFloat( int index )
        {
            return m_buf.getFloat(m_offs + index);
        }

        public RetainableByteBuffer putFloat(int index, float value)
        {
            m_buf.putFloat(m_offs + index, value);
            return this;
        }

        public double getDouble( int index )
        {
            return m_buf.getDouble( m_offs + index );
        }

        public RetainableByteBuffer putDouble( int index, double value )
        {
            m_buf.putDouble( m_offs + index, value );
            return this;
        }
    }

    private final static AtomicIntegerFieldUpdater<RetainableByteBufferPool> s_stateUpdater =
            AtomicIntegerFieldUpdater.newUpdater( RetainableByteBufferPool.class, "m_state" );

    private final int m_chunkSize;
    private final ChunkCache m_cache;
    private volatile int m_state;
    private Chunk m_chunk;

    private BufferImpl allocNewLocked(int state, int space, int size, int reservedSize)
    {
        m_chunk.release(space + 1);
        m_chunk = m_cache.get();
        final Chunk chunk = m_chunk;

        int newState = (state + space);
        assert((newState % m_chunkSize) == 0);
        newState += reservedSize;
        if (newState < 0)
            newState = reservedSize;
        m_state = newState;

        return new BufferImpl(chunk, 0, size, reservedSize);
    }

    public RetainableByteBufferPool(int chunkSize, boolean useDirectBuffers, ByteOrder byteOrder, int maxCacheSize, int initialCacheSize)
    {
        m_chunkSize = chunkSize;
        m_cache = new ChunkCache(chunkSize, useDirectBuffers, byteOrder, maxCacheSize, initialCacheSize);
        m_chunk = m_cache.get();
    }

    public final RetainableByteBuffer alloc(int size, int minSize)
    {
        for (;;)
        {
            final int state = s_stateUpdater.get(this);
            if (state == -1)
                continue;

            final int offs = (state % m_chunkSize);
            final int space = (m_chunkSize - offs);

            /* Would be better to align all slices at least by 4 bytes. */
            final int reservedSize = ((size + 3) & -4);

            if (reservedSize < space)
            {
                assert( (offs + reservedSize) <= m_chunkSize );

                int newState = (state + reservedSize);
                if (newState <= 0)
                    newState = (offs + reservedSize);

                final Chunk chunk = m_chunk;
                if (!s_stateUpdater.compareAndSet(this, state, newState))
                    continue;

                return new BufferImpl(chunk, offs, size, reservedSize);
            }
            else if (reservedSize == space)
            {
                if (!s_stateUpdater.compareAndSet(this, state, -1))
                    continue;

                m_chunk.release(1);
                final Chunk chunk = m_chunk;
                m_chunk = m_cache.get();

                int newState = (state + space);
                if (newState <= 0)
                    newState = (offs + space);

                m_state = newState;

                return new BufferImpl(chunk, offs, size, reservedSize);
            }
            else if (minSize <= space)
            {
                /* (size > space), but (minSize <= space),
                 * caller is ready to use smaller buffer.
                 */
                if (!s_stateUpdater.compareAndSet(this, state, -1))
                    continue;

                m_chunk.release(1);
                final Chunk chunk = m_chunk;
                m_chunk = m_cache.get();

                int newState = (state + space);
                if (newState <= 0)
                    newState = (offs + space);

                m_state = newState;

                return new BufferImpl(chunk, offs, space, space);
            }
            else if (size < m_chunkSize)
            {
                /* size > space */
                if (!s_stateUpdater.compareAndSet(this, state, -1))
                    continue;
                return allocNewLocked(state, space, size, reservedSize);
            }
            else if (size == m_chunkSize)
            {
                /* space < size, let's just take a new chunk. */
                final Chunk chunk = m_cache.get();
                return new BufferImpl(chunk, 0, size, size);
            }
            else if (minSize <= m_chunkSize)
            {
                /* minSize > space */
                if (!s_stateUpdater.compareAndSet(this, state, -1))
                    continue;

                final int rs = ((minSize + 3) & -4);
                assert( rs <= m_chunkSize );

                return allocNewLocked(state, space, minSize, rs);
            }
            else
            {
                /* size > m_chunkSize */
                final Chunk chunk = m_cache.alloc(size);
                final BufferImpl ret = new BufferImpl(chunk, 0, size, size);
                chunk.release(1);
                return ret;
            }
        }
    }

    public RetainableByteBuffer alloc(int size)
    {
        return alloc(size, size);
    }

    public void release(Logger logger)
    {
        final int state = s_stateUpdater.get(this);
        assert(state != -1);
        final int offs = (state % m_chunkSize);
        final int space = (m_chunkSize - offs);
        m_chunk.release(space + 1);
        m_chunk = null;

        m_cache.release(logger);
    }
}
