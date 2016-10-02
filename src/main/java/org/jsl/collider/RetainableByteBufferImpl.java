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

class RetainableByteBufferImpl extends RetainableByteBuffer
{
    public RetainableByteBufferImpl( ByteBuffer byteBuffer )
    {
        super( byteBuffer );
    }

    /*
     * NIO Buffer
     */

    public int capacity()
    {
        return m_buf.capacity();
    }

    public RetainableByteBuffer clear()
    {
        m_buf.clear();
        return this;
    }

    public RetainableByteBuffer flip()
    {
        m_buf.flip();
        return this;
    }

    public int limit()
    {
        return m_buf.limit();
    }

    public RetainableByteBuffer limit( int limit )
    {
        m_buf.limit( limit );
        return this;
    }

    public int position()
    {
        return m_buf.position();
    }

    public RetainableByteBuffer position( int position )
    {
        m_buf.position( position );
        return this;
    }

    public RetainableByteBuffer rewind()
    {
        m_buf.rewind();
        return this;
    }

    /*
     * NIO ByteBuffer
     */

    public byte get( int index )
    {
        return m_buf.get( index );
    }

    public RetainableByteBuffer put( int index, byte value )
    {
        m_buf.put( index, value );
        return this;
    }

    public int getInt( int index )
    {
        return m_buf.getInt( index );
    }

    public RetainableByteBuffer putInt( int index, int value )
    {
        m_buf.putInt( index, value );
        return this;
    }

    public short getShort( int index )
    {
        return m_buf.getShort( index );
    }

    public RetainableByteBuffer putShort( int index , short value )
    {
        m_buf.putShort( index, value );
        return this;
    }

    public float getFloat( int index )
    {
        return m_buf.getFloat( index );
    }

    public RetainableByteBuffer putFloat( int index, float value )
    {
        m_buf.putFloat( index, value );
        return this;
    }

    public double getDouble( int index )
    {
        return m_buf.getDouble( index );
    }

    public RetainableByteBuffer putDouble( int index, double value )
    {
        m_buf.putDouble( index, value );
        return this;
    }
}
