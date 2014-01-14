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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

public class CachedByteBuffer
{
    public static class Cache extends ObjectCache<CachedByteBuffer>
    {
        private final boolean m_useDirectBuffers;
        private final int m_bufferCapacity;
        private final int m_initialSize;

        public Cache( boolean useDirectBuffers, int bufferCapacity )
        {
            this( useDirectBuffers, bufferCapacity, 1000, 100 );
        }

        public Cache( boolean useDirectBuffers, int bufferCapacity, int maxCacheSize, int initialSize )
        {
            super( new CachedByteBuffer[maxCacheSize] );

            m_useDirectBuffers = useDirectBuffers;
            m_bufferCapacity = bufferCapacity;
            m_initialSize = initialSize;

            for (int cc=initialSize; cc>0; cc--)
                put( allocateObject() );
        }

        protected CachedByteBuffer allocateObject()
        {
            ByteBuffer buf;
            if (m_useDirectBuffers)
                buf = ByteBuffer.allocateDirect( m_bufferCapacity );
            else
                buf = ByteBuffer.allocate( m_bufferCapacity );
            return new CachedByteBuffer( this, buf );
        }

        public final int getBufferCapacity()
        {
            return m_bufferCapacity;
        }

        public final void clear( Logger logger )
        {
            if (logger.isLoggable(Level.WARNING))
                logger.warning( clear() );
        }

        public final String clear()
        {
            String ret = "CachedByteBuffer[" + m_bufferCapacity + "]: ";
            ret += clear( m_initialSize );
            return ret;
        }
    }

    private final Cache m_cache;
    private final AtomicInteger m_retainCount;
    private final ByteBuffer m_buf;

    private CachedByteBuffer( Cache cache, ByteBuffer buf )
    {
        m_cache = cache;
        m_retainCount = new AtomicInteger(1);
        m_buf = buf;
    }

    public final void retain()
    {
        for (;;)
        {
            final int retainCount = m_retainCount.get();
            assert( retainCount > 0 );
            if (m_retainCount.compareAndSet(retainCount, retainCount+1))
                return;
        }
    }

    public final void release()
    {
        for (;;)
        {
            final int retainCount = m_retainCount.get();
            assert( retainCount > 0 );
            if (m_retainCount.compareAndSet(retainCount, retainCount-1))
            {
                if (retainCount == 1)
                {
                    m_retainCount.lazySet( retainCount );
                    m_buf.clear();
                    m_cache.put( this );
                }
                return;
            }
        }
    }

    public final ByteBuffer getByteBuffer()
    {
        return m_buf;
    }

    public final void put( ByteBuffer src )
    {
        m_buf.put( src );
    }

    public final void position( int pos )
    {
        m_buf.position( pos );
    }

    public final void flip()
    {
        m_buf.flip();
    }

    public final void clear()
    {
        m_buf.clear();
    }
}
