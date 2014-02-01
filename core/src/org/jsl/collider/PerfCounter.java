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

import java.util.concurrent.atomic.AtomicLongArray;

public class PerfCounter
{
    /* 0 - min
     * 1 - avg
     * 2 - max
     * 3 - sum
     */
    private final String m_name;
    private final AtomicLongArray m_data;

    public PerfCounter( String name )
    {
        m_name = name;
        m_data = new AtomicLongArray(4);
        m_data.set( 0, Long.MAX_VALUE );
    }

    public final void trace( long startTime )
    {
        final long delay = (System.nanoTime() - startTime);

        for (;;)
        {
            long min = m_data.get(0);
            if (delay >= min)
                break;
            if (m_data.compareAndSet(0, min, delay))
                break;
        }

        for (;;)
        {
            final long avg = m_data.get(1);
            long cnt = (avg >> 32);
            long newAvg = cnt * (avg & 0xFFFFFFFFL) + delay;
            cnt++;
            newAvg /= cnt;
            newAvg &= 0xFFFFFFFFL;
            newAvg |= (cnt << 32);
            if (m_data.compareAndSet(1, avg, newAvg))
                break;
        }

        for (;;)
        {
            long max = m_data.get(2);
            if (delay <= max)
                break;
            if (m_data.compareAndSet(2, max, delay))
                break;
        }

        for (;;)
        {
            long total = m_data.get(3);
            if (m_data.compareAndSet(3, total, total+delay))
                break;
        }
    }

    public final String getStats()
    {
        String str = m_name;
        str += ": min=" + formatUsec( m_data.get(0)/1000 );
        str += " avg=" + formatUsec( (m_data.get(1) & 0xFFFFFFFFL) / 1000 );
        str += " max=" + formatUsec( m_data.get(2)/1000 );
        str += " sum=" + formatUsec( m_data.get(3)/1000 );
        str += " cnt=" + (m_data.get(1) >> 32);
        return str;
    }

    private String formatUsec( long usec )
    {
        if (usec == 0)
            return "0.0";
        long sec = (usec / 1000000);
        usec %= 1000000;
        return String.format( "%d.%06d", sec, usec );
    }
}
