/*
 * JS-Collider framework.
 * Copyright (C) 2013 Sergey Zubarev
 * info@js-labs.org
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.jsl.collider;

import java.nio.ByteBuffer;


public abstract class PacketHandler implements Session.Listener
{
    private Session m_session;
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
        src.limit( pos + bytes );
        dst.put( src );
        src.limit( limit );
        return true;
    }

    private static ByteBuffer getBuffer( int capacity )
    {
        if (capacity < 1024)
            capacity = 1024;
        return ByteBuffer.allocate( capacity );
    }

    public PacketHandler( Session session, int headerSize )
    {
        m_session = session;
        m_headerSize = headerSize;
    }

    public void handleData( ByteBuffer data )
    {
        if ((m_buf != null) && (m_buf.position() > 0))
        {
            int pos = m_buf.position();
            if (pos < m_headerSize)
            {
                int cc = (m_headerSize - pos);
                if (!copyData(m_buf, data, cc))
                    return;
            }

            /* Now m_buf contains the whole header */

            m_buf.flip();
            int packetLen = validateHeader( m_buf );
            if (packetLen < 0)
                return;

            if (m_buf.capacity() < packetLen)
            {
                ByteBuffer buf = ByteBuffer.allocate( packetLen );
                m_buf.limit( pos );
                m_buf.position( 0 );
                buf.put( m_buf );
                m_buf = buf;
            }
            else
            {
                m_buf.limit( m_buf.capacity() );
                m_buf.position( pos );
            }

            int cc = (packetLen - pos);
            if (!copyData(m_buf, data, cc))
                return;

            /* Now m_buf contains the whole packet */

            m_buf.flip();
            handlePacket( m_buf );
            m_buf.clear();
        }

        int limit = data.limit();
        for (;;)
        {
            int remaining = data.remaining();
            if (remaining < m_headerSize)
            {
                if (m_buf == null)
                    m_buf = getBuffer( m_headerSize );
                m_buf.put( data );
                break;
            }

            int pos = data.position();
            int packetLen = validateHeader( data );
            if (packetLen < 0)
            {
                m_session.closeConnection();
                break;
            }
            data.position( pos );

            if (remaining < packetLen)
            {
                if ((m_buf == null) || (m_buf.capacity() < packetLen))
                    m_buf = getBuffer( packetLen );
                m_buf.put( data );
                break;
            }

            data.limit( pos + packetLen );
            handlePacket( data );

            data.limit( limit );
            data.position( pos + packetLen );
        }
    }

    public abstract int validateHeader( ByteBuffer header );
    public abstract void handlePacket( ByteBuffer packet );
}
