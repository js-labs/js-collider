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

    private static class Chunk
    {
        private final static AtomicIntegerFieldUpdater<Chunk> s_retainCountUpdater =
                AtomicIntegerFieldUpdater.newUpdater( Chunk.class, "m_retainCount" );

        private final ChunkCache m_cache;
        private final ByteBuffer m_buf;
        private volatile int m_retainCount;

        public Chunk( ChunkCache cache, ByteBuffer buf )
        {
            m_cache = cache;
            m_buf = buf;
            m_retainCount = 1;
        }

        public final ByteBuffer getByteBuffer()
        {
            return m_buf;
        }

        public final void retain()
        {
            for (;;)
            {
                final int retainCount = m_retainCount;
                assert( retainCount > 0 );
                if (s_retainCountUpdater.compareAndSet(this, retainCount, retainCount+1))
                    break;
            }
        }

        public final void release()
        {
            for (;;)
            {
                final int retainCount = m_retainCount;
                assert( retainCount > 0 );
                if (s_retainCountUpdater.compareAndSet(this, retainCount, retainCount-1))
                {
                    if (retainCount == 1)
                    {
                        m_retainCount = retainCount;
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

        private static final int OFFS_MASK    = 0x00FFFFFF;
        private static final int WRITERS_MASK = 0x7F000000;
        private static final int WRITER       = 0x01000000;

        private final boolean m_useDirectBuffers;
        private final ChunkCache m_cache;
        private final int m_chunkSize;
        private volatile int m_state;
        private Chunk m_chunk;

        public Pool()
        {
            this( 128*1024 );
        }

        public Pool( int chunkSize )
        {
            chunkSize &= OFFS_MASK;
            m_useDirectBuffers = true;
            m_cache = new ChunkCache( m_useDirectBuffers, chunkSize, 128, 2 );
            m_chunkSize = chunkSize;
            m_chunk = m_cache.get();
        }

        public final PooledByteBuffer alloc( int size )
        {
            for (;;)
            {
                int state = m_state;
                if (state == -1)
                    continue;

                final int offs = (state & OFFS_MASK);
                int space = (m_chunkSize - offs);

                if (size <= space)
                {
                    if ((state & WRITERS_MASK) == WRITERS_MASK)
                        continue;

                    int newState = (state + WRITER + size);
                    if ((size % 4) > 0)
                    {
                        final int cc = (4 - (size % 4));
                        if (((newState & OFFS_MASK) + cc) <= m_chunkSize)
                            newState += cc;
                    }

                    if (!s_stateUpdater.compareAndSet(this, state, newState))
                        continue;

                    final Chunk chunk = m_chunk;
                    chunk.retain();

                    state = newState;
                    for (;;)
                    {
                        assert( (state & WRITERS_MASK) > 0 );
                        newState = (state - WRITER);
                        if (s_stateUpdater.compareAndSet(this, state, newState))
                            break;
                        state = m_state;
                    }

                    return new PooledByteBuffer( chunk, offs, size );
                }
                else if (size <= m_chunkSize)
                {
                    if ((state & WRITERS_MASK) != 0)
                        continue;

                    if (!s_stateUpdater.compareAndSet(this, state, -1))
                        continue;

                    m_chunk.release();
                    m_chunk = m_cache.get();
                    final Chunk chunk = m_chunk;
                    chunk.retain();

                    int newState = size;
                    if ((size % 4) > 0)
                    {
                        final int cc = (4 - (size % 4));
                        if (((newState & OFFS_MASK) + cc) <= m_chunkSize)
                            newState += cc;
                    }
                    m_state = newState;

                    return new PooledByteBuffer( chunk, 0, size );
                }
                else
                {
                    final ByteBuffer buf =
                            m_useDirectBuffers ? ByteBuffer.allocateDirect(size) : ByteBuffer.allocate(size);
                    final Chunk chunk = new Chunk( null, buf );
                    return new PooledByteBuffer( chunk, 0, size );
                }
            }
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

    private PooledByteBuffer( Chunk chunk, int offs, int size )
    {
        super( chunk.getByteBuffer().duplicate(), offs, size );
        m_chunk = chunk;
    }

    public void retain()
    {
        m_chunk.retain();
    }

    public void release()
    {
        m_chunk.release();
    }
}
