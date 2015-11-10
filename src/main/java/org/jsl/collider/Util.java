/*
 * Copyright (C) 2013 Sergey Zubarev, info@js-labs.org
 *
 * This file is a part of JS-Collider framework.
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
import java.util.Arrays;

public class Util
{
    private static final char [] HD =
    {
        '0', '1', '2', '3', '4', '5', '6', '7',
        '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'
    };

    public static String formatDelay( long startTime, long endTime )
    {
        final long delay = ((endTime - startTime) / 1000);
        if (delay > 0)
            return String.format( "%d.%06d", delay/1000000, delay%1000000 );
        return "0.0";
    }

    public static String hexDump( ByteBuffer bb )
    {
        int pos = bb.position();
        int limit = bb.limit();
        if (pos == limit)
            return "<empty>";

        if (limit > 0xFFFF)
            limit = 0xFFFF;

        final StringBuilder sb = new StringBuilder();
        /*         "0000: 00 01 02 03 04 05 06 07-08 09 0A 0B 0C 0D 0E 0F | ................" */
        sb.append( "       0  1  2  3  4  5  6  7  8  9  A  B  C  D  E  F [" );
        sb.append( pos );
        sb.append( ", " );
        sb.append( limit );
        sb.append( "]\n" );

        final char [] buf = new char[73];
        Arrays.fill( buf, ' ' );
        buf[4] = ':';
        buf[29] = '-';
        buf[54] = '|';
        buf[72] = '\n';

        loop: while (pos < limit)
        {
            final int pp = (pos - bb.position());
            buf[0] = HD[(pp >> 12) & 0xF];
            buf[1] = HD[(pp >> 8) & 0xF];
            buf[2] = HD[(pp >> 4) & 0xF];
            buf[3] = HD[pp & 0xF];

            int hp = 6;
            int cp = 56;
            for (int idx=0;;)
            {
                final int v = bb.get( pos );
                buf[hp++] = HD[(v >> 4) & 0xF];
                buf[hp] = HD[v & 0xF];
                hp+= 2;

                buf[cp++] = ((v >= 32) && (v < 128)) ? (char)v : '.';

                pos++;

                if (++idx == 16)
                    break;

                if (pos == limit)
                {
                    for (; idx<16; idx++)
                    {
                        buf[hp++] = ' ';
                        buf[hp] = ' ';
                        hp += 2;
                        buf[cp++] = ' ';
                    }
                    sb.append( buf );
                    break loop;
                }
            }

            sb.append( buf );
        }

        return sb.toString();
    }

    public static String hexDump( RetainableByteBuffer bb )
    {
        return hexDump( bb.getNioByteBuffer() );
    }
}
