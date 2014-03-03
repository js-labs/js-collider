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

import java.io.File;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;

public class ShMemClient extends ShMem
{
    private final int m_blockSize;
    private final ChannelIn m_in;
    private final ChannelOut m_out;
    private final ByteBuffer m_c2sBB;
    private final ByteBuffer m_s2cBB;

    public ShMemClient( String fileHint, int blockSize, File directory ) throws IOException
    {
        /* It will be better if block size is a multiplier of 4096. */
        if ((blockSize & 0x0FFF) > 0)
        {
            blockSize &= ~0x0FFF;
            blockSize +=  0x1000;
        }
        m_blockSize = blockSize;

        final String prefix = "jsc-" + fileHint + "-";
        final File fileC2S = File.createTempFile( prefix, ".c2s", directory );
        m_out = new ChannelOut( fileC2S, blockSize, true );

        final File fileS2C = File.createTempFile( prefix, ".s2c", directory );
        m_in = new ChannelIn( fileS2C, blockSize, true );

        final CharsetEncoder encoder = Charset.defaultCharset().newEncoder();
        m_c2sBB = encoder.encode( CharBuffer.wrap(fileC2S.getAbsolutePath()) );
        m_s2cBB = encoder.encode( CharBuffer.wrap(fileS2C.getAbsolutePath()) );
    }

    public ShMemClient( String fileHint, int blockSize ) throws IOException
    {
        this( fileHint, blockSize, null );
    }

    public ShMemClient( String fileHint ) throws IOException
    {
        this( fileHint, 64*1024 );
    }

    public final int getDescriptorLength()
    {
        /* Shared memory session descriptor structure:
         * short : descriptor version
         * int   : shared memory block size
         * short : length of the (client->server) file path
         *       : (client->server) file absolute path
         * short : length of the (server->client) file name
         *       : (server->client) file absolute path
         */
        return (2 +
                4 +
                2 + m_c2sBB.remaining() +
                2 + m_s2cBB.remaining());
    }

    public final void getDescriptor( ByteBuffer buf ) throws BufferOverflowException
    {
        assert( buf.remaining() >= getDescriptorLength() );
        buf.putShort( (short) 1 );
        buf.putInt( m_blockSize );
        buf.putShort( (short) m_c2sBB.remaining() );
        buf.put( m_c2sBB );
        buf.putShort( (short) m_s2cBB.remaining() );
        buf.put( m_s2cBB );
    }

    public ChannelIn getIn()
    {
        return m_in;
    }

    public ChannelOut getOut()
    {
        return m_out;
    }

    public void close()
    {
        m_in.close();
        m_out.close();
    }
}
