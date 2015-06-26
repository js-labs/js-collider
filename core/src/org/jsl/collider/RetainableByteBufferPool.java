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
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.logging.Logger;

public class RetainableByteBufferPool
{
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

    private static class BufferImpl extends RetainableByteBuffer
    {
        private final Chunk m_chunk;
        private final int m_offs;
        private final int m_capacity;
        private final int m_reservedSize;

        public BufferImpl( Chunk chunk, int offs, int capacity, int reservedSize )
        {
            super( chunk.getByteBuffer().duplicate() );
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

        public RetainableByteBuffer clear()
        {
            m_buf.clear();
            m_buf.position( m_offs );
            m_buf.limit( m_offs + m_capacity );
            return this;
        }

        public RetainableByteBuffer flip()
        {
            final int position = m_buf.position();
            m_buf.position( m_offs );
            m_buf.limit( position );
            return this;
        }

        public int capacity()
        {
            return m_capacity;
        }

        public int limit()
        {
            return (m_buf.limit() - m_offs);
        }

        public RetainableByteBuffer limit( int newLimit )
        {
            if ((newLimit > m_capacity) || (newLimit < 0))
                throw new IllegalArgumentException();
            m_buf.limit( m_offs + newLimit );
            return this;
        }

        public int position()
        {
            return (m_buf.position() - m_offs);
        }

        public RetainableByteBuffer position( int position )
        {
            m_buf.position( m_offs + position );
            return this;
        }

        public byte get( int index )
        {
            return m_buf.get( m_offs + index );
        }

        public RetainableByteBuffer put( int index, byte value )
        {
            m_buf.put( index, value );
            return this;
        }

        public int getInt( int index )
        {
            return m_buf.getInt( m_offs + index );
        }

        public RetainableByteBuffer putInt( int index, int value )
        {
            m_buf.putInt( m_offs + index, value );
            return this;
        }

        public short getShort( int index )
        {
            return m_buf.getShort( m_offs + index );
        }

        public RetainableByteBuffer putShort( int index, short value )
        {
            m_buf.putShort( m_offs + index, value );
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

    private final boolean m_useDirectBuffers;
    private final ChunkCache m_cache;
    private final int m_chunkSize;
    private volatile int m_state;
    private Chunk m_chunk;

    private BufferImpl allocNewLocked( int state, int space, int size, int reservedSize )
    {
        m_chunk.release( space + 1 );
        m_chunk = m_cache.get();
        final Chunk chunk = m_chunk;

        int newState = (state + space);
        assert( (newState % m_chunkSize) == 0 );
        newState += reservedSize;
        if (newState < 0)
            newState = reservedSize;
        m_state = newState;

        return new BufferImpl( chunk, 0, size, reservedSize );
    }

    /**
     * Default constructor.
     * Creates a pool with chunk size = 64Kb.
     */
    public RetainableByteBufferPool()
    {
        this( 64*1024 );
    }

    public RetainableByteBufferPool( int chunkSize )
    {
        this( chunkSize, true );
    }

    public RetainableByteBufferPool( int chunkSize, boolean useDirectBuffers )
    {
        m_useDirectBuffers = useDirectBuffers;
        m_cache = new ChunkCache( m_useDirectBuffers, chunkSize, 128, 2 );
        m_chunkSize = chunkSize;
        m_chunk = m_cache.get();
    }

    public final RetainableByteBuffer alloc( int size, int minSize )
    {
        for (;;)
        {
            final int state = m_state;
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

                return new BufferImpl( chunk, offs, size, reservedSize );
            }
            else if (reservedSize == space)
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

                return new BufferImpl( chunk, offs, size, reservedSize );
            }
            else if (minSize <= space)
            {
                /* (size > space), but (minSize <= space),
                 * caller is ready to use smaller buffer.
                 */
                if (!s_stateUpdater.compareAndSet(this, state, -1))
                    continue;

                m_chunk.release( 1 );
                final Chunk chunk = m_chunk;
                m_chunk = m_cache.get();

                int newState = (state + space);
                if (newState <= 0)
                    newState = (offs + space);

                m_state = newState;

                return new BufferImpl( chunk, offs, space, space );
            }
            else if (size < m_chunkSize)
            {
                /* size > space */
                if (!s_stateUpdater.compareAndSet(this, state, -1))
                    continue;
                return allocNewLocked( state, space, size, reservedSize );
            }
            else if (size == m_chunkSize)
            {
                /* space < size, let's just take a new chunk. */
                final Chunk chunk = m_cache.get();
                return new BufferImpl( chunk, 0, size, size );
            }
            else if (minSize <= m_chunkSize)
            {
                /* minSize > space */
                if (!s_stateUpdater.compareAndSet(this, state, -1))
                    continue;

                final int rs = ((minSize + 3) & -4);
                assert( rs <= m_chunkSize );

                return allocNewLocked( state, space, minSize, rs );
            }
            else
            {
                /* size > m_chunkSize */
                final ByteBuffer buf =
                        m_useDirectBuffers ? ByteBuffer.allocateDirect( size )
                                           : ByteBuffer.allocate( size );
                final Chunk chunk = new Chunk( null, buf );
                final BufferImpl ret = new BufferImpl( chunk, 0, size, size );
                chunk.release(1);
                return ret;
            }
        }
    }

    public final RetainableByteBuffer alloc( int size )
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
