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

/*
 * Helper class providing byte stream defragmentation functionality.
 * Useful for binary protocols with messages having a fixed length header
 * containing the length of the whole message.
 *
 * Constructor argument is a length of the message header.
 *
 * Derived class is supposed to implement method <tt>validateHeader</tt>
 * which is called when whole header is available for decoding and
 * should return the length of the message. ByteBuffer containing the
 * header can has a position greater than 0.
 * Method <tt>getNext</tt> returns the next available whole message,
 * or null if no message is available.
 *
 * Here is a usage example with Session.Listener
 * for simple message header containing only 4-bytes length field:
 *
 * <pre>{@code
 *   class ServerListener implements Session.Listener
 *   {
 *       private final StreamDefragger streamDefragger = new StreamDefragger(4)
 *       {
 *           protected int validateHeader( ByteBuffer header )
 *           {
 *               return header.getInt();
 *           }
 *       };
 *
 *       public void onDataReceived( RetainableByteBuffer data )
 *       {
 *           RetainableByteBuffer msg = streamDefragger.getNext( data );
 *           while (msg != null)
 *           {
 *               // process message
 *               msg = streamDefragger.getNext();
 *           }
 *       }
 *
 *       public void onConnectionClose()
 *       {
 *           streamDefragger.close();
 *       }
 *   }
 * }</pre>
 */

public abstract class StreamDefragger
{
    public final static RetainableByteBuffer INVALID_HEADER = new BufferImpl(null);

    private final int m_headerSize;
    private RetainableByteBuffer m_buf;
    private RetainableByteBuffer m_data;
    private int m_packetLen;
    private int m_pos;
    private int m_limit;

    private static class BufferImpl extends RetainableByteBufferImpl
    {
        public BufferImpl( ByteBuffer byteBuffer )
        {
            super( byteBuffer );
        }

        protected void finalRelease()
        {
            /* Do nothing */
        }

        public static BufferImpl create( int capacity, boolean isDirect )
        {
            if (capacity < 1024)
                capacity = 1024;
            final ByteBuffer byteBuffer =
                    isDirect ? ByteBuffer.allocateDirect( capacity )
                             : ByteBuffer.allocate( capacity );
            return new BufferImpl( byteBuffer );
        }
    }

    private static boolean copyData(
            RetainableByteBuffer rdst, RetainableByteBuffer rsrc, int bytes )
    {
        final ByteBuffer dst = rdst.getNioByteBuffer();
        final ByteBuffer src = rsrc.getNioByteBuffer();
        final int pos = src.position();
        final int limit = src.limit();
        final int available = (limit - pos);
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

    public StreamDefragger( int headerSize )
    {
        m_headerSize = headerSize;
    }

    public final RetainableByteBuffer getNext( RetainableByteBuffer data )
    {
        assert( m_data == null );

        if ((m_buf != null) && (m_buf.position() > 0))
        {
            int pos = m_buf.position();
            if (pos < m_headerSize)
            {
                final int cc = (m_headerSize - pos);
                if (!copyData(m_buf, data, cc))
                    return null;

                m_buf.flip();
                m_packetLen = validateHeader( m_buf.getNioByteBuffer() );
                if (m_packetLen <= 0)
                    return INVALID_HEADER;

                if (m_buf.capacity() < m_packetLen)
                {
                    final BufferImpl buf = BufferImpl.create( m_packetLen, data.getNioByteBuffer().isDirect() );
                    m_buf.position( 0 );
                    m_buf.limit( m_headerSize );
                    buf.put( m_buf );
                    m_buf = buf;
                }
                else
                    m_buf.limit( m_buf.capacity() );

                pos = m_headerSize;
                m_buf.position( pos );
            }

            final int cc = (m_packetLen - pos);
            if (!copyData(m_buf, data, cc))
                return null;
            m_buf.flip();

            m_data = data;
            m_pos = data.position();
            m_limit = data.limit();

            return m_buf;
        }

        m_data = data;
        m_pos = data.position();
        m_limit = data.limit();
        return getNext();
    }

    public final RetainableByteBuffer getNext()
    {
        m_data.position( m_pos );
        m_data.limit( m_limit );

        final int bytesRemaining = (m_limit - m_pos);
        if (bytesRemaining == 0)
        {
            if ((m_buf != null) && !m_buf.releaseReuse())
                m_buf = null;
            m_data = null;
            return null;
        }

        if (bytesRemaining < m_headerSize)
        {
            /* m_buf will always have at least m_headerSize capacity */
            if ((m_buf == null) || !m_buf.releaseReuse())
                m_buf = BufferImpl.create( m_headerSize, m_data.getNioByteBuffer().isDirect() );
            m_buf.put( m_data );
            m_data = null;
            return null;
        }

        m_packetLen = validateHeader( m_data.getNioByteBuffer() );
        m_data.position( m_pos );

        if (m_packetLen <= 0)
            return INVALID_HEADER;

        if (bytesRemaining < m_packetLen)
        {
            if (m_buf == null)
                m_buf = BufferImpl.create( m_packetLen, m_data.getNioByteBuffer().isDirect() );
            else if (m_buf.capacity() < m_packetLen)
            {
                m_buf.release();
                m_buf = BufferImpl.create( m_packetLen, m_data.getNioByteBuffer().isDirect() );
            }
            else if (!m_buf.releaseReuse())
                m_buf = BufferImpl.create( m_packetLen, m_data.getNioByteBuffer().isDirect() );

            m_buf.put( m_data );
            m_data = null;
            return null;
        }

        m_pos += m_packetLen;
        m_data.limit( m_pos );
        return m_data;
    }

    public void close()
    {
        if (m_buf != null)
        {
            m_buf.release();
            m_buf = null;
        }
    }

    abstract protected int validateHeader( ByteBuffer header );
}
