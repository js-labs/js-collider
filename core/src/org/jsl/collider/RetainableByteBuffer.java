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

public abstract class RetainableByteBuffer
{
    private final ByteBuffer m_buf;
    private final int m_offs;
    private final int m_size;

    public RetainableByteBuffer( ByteBuffer buf, int offs, int size )
    {
        m_buf = buf;
        m_offs = offs;
        m_size = size;
        m_buf.position( offs );
        m_buf.limit( offs + size );
    }

    public abstract void retain();
    public abstract void release();

    public final ByteBuffer getByteBuffer( SessionImpl.RetainableByteBufferFriend friend )
    {
        return m_buf;
    }

    public final int capacity()
    {
        return m_size;
    }

    public final RetainableByteBuffer clear()
    {
        m_buf.position( m_offs );
        m_buf.limit( m_offs + m_size );
        return this;
    }

    public final RetainableByteBuffer flip()
    {
        final int position = m_buf.position();
        m_buf.position( m_offs );
        m_buf.limit( position );
        return this;
    }

    public final int limit()
    {
        return (m_buf.limit() - m_offs);
    }

    public final RetainableByteBuffer limit( int limit )
    {
        if ((limit < 0) || (limit > m_size))
            throw new IllegalArgumentException();
        m_buf.limit( m_offs + limit );
        return this;
    }

    public final int position()
    {
        return (m_buf.position() - m_offs);
    }

    public final RetainableByteBuffer position( int position )
    {
        if ((position < 0) || (position > limit()))
            throw new IllegalArgumentException();
        m_buf.position( m_offs + position );
        return this;
    }

    public final int remaining()
    {
        return m_buf.remaining();
    }

    public final RetainableByteBuffer put( ByteBuffer src )
    {
        m_buf.put( src );
        return this;
    }

    public final RetainableByteBuffer putInt( int index, int value )
    {
        m_buf.putInt( index+m_offs, value );
        return this;
    }
}
