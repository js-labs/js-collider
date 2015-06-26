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
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;

public class ShMemServer extends ShMem
{
    private final ChannelIn m_in;
    private final ChannelOut m_out;

    public ShMemServer( RetainableByteBuffer buf ) throws Exception
    {
        final short descriptorVersion = buf.getShort();
        final int bufLimit = buf.limit();
        if (descriptorVersion != 1)
            throw new Exception( "ShMem descriptor version " + descriptorVersion + " not supported." );

        final CharsetDecoder decoder = Charset.defaultCharset().newDecoder();
        final int blockSize = buf.getInt();

        final int length = buf.getShort();
        buf.limit( buf.position() + length );
        final File fileC2S = new File( decoder.decode(buf.getNioByteBuffer()).toString() );
        m_in = new ChannelIn( fileC2S, blockSize, false );

        buf.limit( bufLimit );
        buf.getShort();
        final File fileS2C = new File( decoder.decode(buf.getNioByteBuffer()).toString() );
        m_out = new ChannelOut( fileS2C, blockSize, false );
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
