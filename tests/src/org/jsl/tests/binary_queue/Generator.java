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

package org.jsl.tests.binary_queue;

import org.jsl.tests.Util;
import java.nio.ByteBuffer;

public class Generator extends Thread
{
    private Main m_main;
    private int m_msgs;
    private ByteBuffer m_msg;

    public Generator( Main main, int messages, int messageSize, int magic )
    {
        m_main = main;
        m_msgs = messages;
        if (messageSize > 4)
        {
            if (messageSize < 8)
                messageSize = 8;
            m_msg = ByteBuffer.allocate( messageSize );
            m_msg.putInt( messageSize );
            m_msg.putInt( magic );
            for (int idx=0; idx<messageSize-8; idx++)
                m_msg.put( (byte) idx );
            m_msg.flip();
        }
    }

    public void run()
    {
        System.out.println( Thread.currentThread().getName() + ": started." );

        long startTime = System.nanoTime();
        if (m_msg == null)
        {
            for (int idx=0; idx<m_msgs; idx++)
                m_main.putInt( 4 );
        }
        else
        {
            for (int idx=0; idx<m_msgs; idx++)
            {
                m_main.putData( m_msg );
                m_msg.rewind();
            }
        }
        long endTime = System.nanoTime();

        System.out.println( Thread.currentThread().getName() +
                            ": sent " + m_msgs + " messages (" +
                            ((long)m_msgs) * ((m_msg == null) ? 4 : m_msg.capacity()) +
                            " bytes) at " + Util.formatDelay(startTime, endTime) + "." );
    }
}
