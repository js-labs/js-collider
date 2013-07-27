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


public abstract class PacketHandler implements Session.Listener
{
    private final int m_headerSize;
    private ByteBuffer m_buf;

    private static boolean copyData( ByteBuffer dst, ByteBuffer src, int bytes )
    {
        int pos = src.position();
        int limit = src.limit();
        int available = (limit - pos);
        if (available < bytes)
        {
            dst.put( src );
            return false;
        }
        else
        {
            src.limit( pos + bytes );
            dst.put( src );
            src.limit( limit );
            return true;
        }
    }

    private static ByteBuffer getBuffer( int capacity )
    {
        if (capacity < 1024)
            capacity = 1024;
        return ByteBuffer.allocate( capacity );
    }

    public PacketHandler( int headerSize )
    {
        m_headerSize = headerSize;
    }

    public void onDataReceived( ByteBuffer data )
    {
        if ((m_buf != null) && (m_buf.position() > 0))
        {
            int bpos = m_buf.position();
            if (bpos < m_headerSize)
            {
                int cc = (m_headerSize - bpos);
                if (!copyData(m_buf, data, cc))
                    return;
                bpos = m_headerSize;
            }

            /* Now m_buf contains the whole header */

            m_buf.flip();
            int packetLen = onValidateHeader( m_buf );
            if (packetLen < 0)
                return;

            if (m_buf.capacity() < packetLen)
            {
                ByteBuffer buf = ByteBuffer.allocate( packetLen );
                m_buf.position( 0 );
                buf.put( m_buf );
                m_buf = buf;
            }
            else
            {
                m_buf.position( bpos );
                m_buf.limit( m_buf.capacity() );
            }

            int cc = (packetLen - bpos);
            if (!copyData(m_buf, data, cc))
                return;

            m_buf.flip();
            onPacketReceived( m_buf );
            m_buf.clear();
        }

        int pos = data.position();
        int limit = data.limit();
        int bytesRest = (limit - pos);

        while (bytesRest > 0)
        {
            if (bytesRest < m_headerSize)
            {
                if (m_buf == null)
                    m_buf = getBuffer( m_headerSize );
                m_buf.put( data );
                break;
            }

            int packetLen = onValidateHeader( data );
            if (packetLen < 0)
                break;
            data.position( pos );

            if (bytesRest < packetLen)
            {
                if ((m_buf == null) || (m_buf.capacity() < packetLen))
                    m_buf = getBuffer( packetLen );
                m_buf.put( data );
                break;
            }

            pos += packetLen;
            data.limit( pos );

            onPacketReceived( data );

            data.position( pos );
            data.limit( limit );

            bytesRest -= packetLen;
        }
    }

    protected abstract int onValidateHeader( ByteBuffer header );
    protected abstract void onPacketReceived( ByteBuffer packet );
}
