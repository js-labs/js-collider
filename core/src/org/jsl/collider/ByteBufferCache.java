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

public class ByteBufferCache
{
    private static class Cache extends ObjectCache<BufferImpl>
    {
        private final boolean m_useDirectBuffer;
        private final int m_bufferCapacity;

        protected BufferImpl allocateObject()
        {
            final ByteBuffer buf = (m_useDirectBuffer ? ByteBuffer.allocateDirect(m_bufferCapacity)
                                                      : ByteBuffer.allocate(m_bufferCapacity));
            return new BufferImpl( buf, this );
        }

        public Cache( boolean useDirectBuffers, int bufferCapacity, int maxCacheSize, int initialSize )
        {
            super( "ByteBufferCache[" + bufferCapacity + "]", new BufferImpl[maxCacheSize] );
            m_useDirectBuffer = useDirectBuffers;
            m_bufferCapacity = bufferCapacity;
            for (int cc=initialSize; cc>0; cc--)
                put( allocateObject() );
        }
    }

    private static class BufferImpl extends RetainableByteBuffer
    {
        private final Cache m_cache;

        public BufferImpl( ByteBuffer buf, Cache cache )
        {
            super( buf, 0, buf.capacity() );
            m_cache = cache;
        }

        protected void finalRelease()
        {
            m_cache.put( this );
        }
    }

    private final Cache m_cache;

    public ByteBufferCache( boolean useDirectBuffers, int bufferCapacity )
    {
        m_cache = new Cache( useDirectBuffers, bufferCapacity, 64, 2 );
    }

    public final RetainableByteBuffer get()
    {
        return m_cache.get();
    }
}
