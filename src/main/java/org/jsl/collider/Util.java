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

public class Util
{
    private static final char [] HD = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};

    public static String formatDelay(long startTime, long endTime)
    {
        final long delay = ((endTime - startTime) / 1000);
        if (delay > 0)
            return String.format("%d.%06d", delay/1000000, delay%1000000);
        return "0.0";
    }

    public static void hexDump(ByteBuffer byteBuffer, StringBuilder sb, int maxLines)
    {
        int pos = byteBuffer.position();
        int limit = byteBuffer.limit();
        if (pos == limit)
        {
            sb.append("<empty>");
            return;
        }

        int c = ((limit - pos) / 16);
        if (c > maxLines)
            limit = (pos + (maxLines * 16));

        /*        "0000: 00 01 02 03 04 05 06 07 08 09 0A 0B 0C 0D 0E 0F | ................" */
        sb.append("       0  1  2  3  4  5  6  7  8  9  A  B  C  D  E  F [");
        sb.append(pos);
        sb.append(", ");
        sb.append(limit);
        sb.append("]\n");

        while (pos < limit)
        {
            int p = (pos - byteBuffer.position());
            sb.append(HD[(p >> 12) & 0x0F]);
            sb.append(HD[(p >> 8) & 0x0F]);
            sb.append(HD[(p >> 4) & 0x0F]);
            sb.append(HD[p & 0x0F]);
            sb.append(": ");

            int n = Math.min((limit - pos), 16);
            p = pos;
            for (c=n; c>0; c--, p++)
            {
                final int v = (((int)byteBuffer.get(p)) & 0xFF);
                sb.append(HD[(v >> 4)]);
                sb.append(HD[v & 0x0F]);
                sb.append(' ');
            }

            for (c=(16-n); c>0; c--)
                sb.append("   ");

            sb.append("| ");

            p = pos;
            for (c=n; c>0; c--, p++)
            {
                final int v = byteBuffer.get(p);
                final char vc = ((v >= 32) ? (char)v : '.');
                sb.append(vc);
            }

            sb.append('\n');
            pos += n;
        }
    }

    public static String hexDump(ByteBuffer byteBuffer, int maxLines)
    {
        final StringBuilder sb = new StringBuilder();
        hexDump(byteBuffer, sb, maxLines);
        return sb.toString();
    }

    public static String hexDump(ByteBuffer byteBuffer)
    {
        return hexDump(byteBuffer, 10);
    }

    public static String hexDump(RetainableByteBuffer bb)
    {
        return hexDump(bb.getNioByteBuffer());
    }
}
