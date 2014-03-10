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
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.logging.Logger;

public class PooledByteBuffer extends RetainableByteBuffer
{
    private final Chunk m_chunk;
    private final int m_reservedSize;

    private static class Chunk
    {
        private final static AtomicIntegerFieldUpdater<Chunk> s_rcUpdater =
                AtomicIntegerFieldUpdater.newUpdater( Chunk.class, "m_rc" );

        private final ChunkCache m_cache;
        private final ByteBuffer m_buf;
        private volatile int m_rc;

        public Chunk( ChunkCache cache, ByteBuffer buf )
        {
            m_cache = cache;
            m_buf = buf;
            m_rc = (buf.capacity() + 1);
        }

        public final ByteBuffer getByteBuffer()
        {
            return m_buf;
        }

        public final void release( int bytes )
        {
            for (;;)
            {
                final int rc = m_rc;
                assert( rc >= bytes );
                if (s_rcUpdater.compareAndSet(this, rc, rc-bytes))
                {
                    if (rc == bytes)
                    {
                        m_rc = (m_buf.capacity() + 1);
                        if (m_cache != null)
                            m_cache.put( this );
                    }
                    break;
                }
            }
        }
    }

    private static class ChunkCache extends ObjectCache<Chunk>
    {
        private final boolean m_useDirectBuffers;
        private final int m_bufferCapacity;
        private final int m_initialSize;

        public ChunkCache( boolean useDirectBuffers, int bufferCapacity, int maxCacheSize, int initialSize )
        {
            super( "ByteBufferPool[" + bufferCapacity + "]", new Chunk[maxCacheSize] );

            m_useDirectBuffers = useDirectBuffers;
            m_bufferCapacity = bufferCapacity;
            m_initialSize = initialSize;

            for (int cc=initialSize; cc>0; cc--)
                put( allocateObject() );
        }

        protected Chunk allocateObject()
        {
            ByteBuffer buf;
            if (m_useDirectBuffers)
                buf = ByteBuffer.allocateDirect( m_bufferCapacity );
            else
                buf = ByteBuffer.allocate( m_bufferCapacity );
            return new Chunk( this, buf );
        }

        public final void clear( Logger logger )
        {
            clear( logger, m_initialSize );
        }

        public final String clear()
        {
            return clear( m_initialSize );
        }
    }

    public static class Pool
    {
        private final static AtomicIntegerFieldUpdater<Pool> s_stateUpdater =
                AtomicIntegerFieldUpdater.newUpdater( Pool.class, "m_state" );

        private final boolean m_useDirectBuffers;
        private final ChunkCache m_cache;
        private final int m_chunkSize;
        private volatile int m_state;
        private volatile Chunk m_chunk;

        private PooledByteBuffer allocNewLocked( int state, int space, int size )
        {
            m_chunk.release( space + 1 );
            m_chunk = m_cache.get();
            final Chunk chunk = m_chunk;

            int newState = (state + space + size);
            int reservedSize = size;
            if (newState <= 0)
                newState = size;

            if ((size % 4) > 0)
            {
                final int cc = (4 - (size % 4));
                if ((size + cc) <= m_chunkSize)
                {
                    newState += cc;
                    reservedSize += cc;
                }
            }
            m_state = newState;

            return new PooledByteBuffer( chunk, 0, size, reservedSize );
        }

        public Pool()
        {
            this( 64*1024 );
        }

        public Pool( int chunkSize )
        {
            m_useDirectBuffers = true;
            m_cache = new ChunkCache( m_useDirectBuffers, chunkSize, 128, 2 );
            m_chunkSize = chunkSize;
            m_chunk = m_cache.get();
        }

        public final PooledByteBuffer alloc( int size, int minSize )
        {
            for (;;)
            {
                int state = m_state;
                if (state == -1)
                    continue;

                final int offs = (state % m_chunkSize);
                int space = (m_chunkSize - offs);

                if (size < space)
                {
                    int newState = (state + size);
                    int reservedSize = size;
                    if (newState <= 0)
                        newState = (offs + size);

                    if ((size % 4) > 0)
                    {
                        final int cc = (4 - (size % 4));
                        if ((offs + size + cc) <= m_chunkSize)
                        {
                            newState += cc;
                            reservedSize += cc;
                        }
                    }

                    final Chunk chunk = m_chunk;
                    if (!s_stateUpdater.compareAndSet(this, state, newState))
                        continue;

                    return new PooledByteBuffer( chunk, offs, size, reservedSize );
                }
                else if ((size == space) || (minSize <= space))
                {
                    if (!s_stateUpdater.compareAndSet(this, state, -1))
                        continue;

                    m_chunk.release( 1 );
                    final Chunk chunk = m_chunk;
                    m_chunk = m_cache.get();

                    int newState = (state + space);
                    if (newState <= 0)
                        newState = (offs + space);

                    m_state = newState;

                    return new PooledByteBuffer( chunk, offs, space, space );
                }
                else if (size < m_chunkSize)
                {
                    /* size > space */
                    if (!s_stateUpdater.compareAndSet(this, state, -1))
                        continue;
                    return allocNewLocked( state, space, size );
                }
                else if (size == m_chunkSize)
                {
                    /* space < size, let's just take a new chunk. */
                    final Chunk chunk = m_cache.get();
                    return new PooledByteBuffer( chunk, 0, size, size );
                }
                else if (minSize <= m_chunkSize)
                {
                    /* minSize > space */
                    if (!s_stateUpdater.compareAndSet(this, state, -1))
                        continue;
                    return allocNewLocked( state, space, minSize );
                }
                else
                {
                    /* size > m_chunkSize */
                    final ByteBuffer buf =
                            m_useDirectBuffers ? ByteBuffer.allocateDirect(size) : ByteBuffer.allocate( size );
                    final Chunk chunk = new Chunk( null, buf );
                    PooledByteBuffer ret = new PooledByteBuffer( chunk, 0, size, size );
                    chunk.release(1);
                    return ret;
                }
            }
        }

        public final PooledByteBuffer alloc( int size )
        {
            return alloc( size, size );
        }

        public final void clear( Logger logger )
        {
            m_cache.clear( logger );
        }

        public final String clear()
        {
            return m_cache.clear();
        }
    }

    private PooledByteBuffer( Chunk chunk, int offs, int size, int reservedSize )
    {
        super( chunk.getByteBuffer().duplicate(), offs, size );
        m_chunk = chunk;
        m_reservedSize = reservedSize;
    }

    protected void finalRelease()
    {
        m_chunk.release( m_reservedSize );
    }

    public String toString()
    {
        String ret = super.toString();
        ret += " reserved=" + m_reservedSize;
        return ret;
    }
}
