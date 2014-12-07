/*
 * JS-Collider framework tests.
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

package org.jsl.tests.message_queue;

import org.jsl.collider.MessageQueue;
import org.jsl.collider.DataBlockCache;
import org.jsl.collider.Util;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

public class Main
{
    private static final int OPS = 1000000;
    private final AtomicInteger m_msgs;

    private class Worker extends Thread
    {
        private final int m_id;
        private final MessageQueue m_queue;
        private final int m_messageSize;

        public Worker( int id, MessageQueue queue, int messageSize )
        {
            m_id = id;
            m_queue = queue;
            m_messageSize = messageSize;
        }

        public void run()
        {
            System.out.println( m_id + ": started" );

            final ByteBuffer byteBuffer = ByteBuffer.allocateDirect( (1 + m_messageSize) * (Integer.SIZE/Byte.SIZE) );
            byteBuffer.putInt( m_messageSize );

            for (int idx=0, value=0x11111111; idx<m_messageSize; idx++, value+=0x11111111)
                byteBuffer.putInt( value );

            int messagesProcessed = 0;
            int sync = 0;
            for (int idx=0; idx<OPS; idx++)
            {
                byteBuffer.position( 0 );
                byteBuffer.limit( byteBuffer.capacity() );

                final ByteBuffer msg = m_queue.putAndGet( byteBuffer );
                if (msg != null)
                {
                    messagesProcessed += processMessage( msg, m_queue );
                    if (msg == byteBuffer)
                        sync++;
                }
            }

            System.out.println( m_id + ": done: " + OPS + ", " + messagesProcessed + " (sync=" + sync + ")" );
            m_msgs.addAndGet( messagesProcessed );
        }
    }

    private int processMessage( ByteBuffer msg, MessageQueue queue )
    {
        int ret = 0;
        try
        {
            for (;;)
            {
                int valid = 0x11111111;
                final int cnt = msg.getInt();
                for (int j = 0; j < cnt; j++)
                {
                    int v = msg.getInt();
                    if (v != valid)
                    {
                        throw new RuntimeException(
                                "Unexpected value " + String.format("0x%x", v) +
                                ", should be " + String.format("0x%x", valid) );
                    }
                    valid += 0x11111111;
                }
                if (msg.remaining() > 0)
                    throw new RuntimeException( "Unexpected message size, should be empty." );
                ret++;

                msg = queue.getNext();
                if (msg == null)
                    break;
            }
        }
        catch (Exception ex)
        {
            final int pos = msg.position();
            msg.limit( msg.capacity() ).position( 0 );
            System.out.println( ex.toString() );
            System.out.println( "pos=0x" + String.format("%x", pos) + "\n" + Util.hexDump(msg) );
        }
        return ret;
    }

    private Main()
    {
        m_msgs = new AtomicInteger(0);
    }

    private void run()
    {
        final DataBlockCache dataBlockCache = new DataBlockCache( true, 4*1024, 4, 64 );
        final MessageQueue queue =  new MessageQueue( dataBlockCache );
        final Thread [] thread = new Thread[3];

        for (int idx=0; idx<thread.length; idx++)
        {
            thread[idx] = new Worker( idx, queue, idx * idx );
            thread[idx].start();
        }

        try
        {
            for (Thread t : thread)
                t.join();
            System.out.println( "Total messages: " + m_msgs.get() );
        }
        catch (InterruptedException ex)
        {
            ex.printStackTrace();
        }

        if (m_msgs.get() != (OPS*thread.length))
        {
            throw new RuntimeException(
                    "Processed " + m_msgs.get() + " messages instead of " +
                    (OPS * thread.length) + "." );
        }
    }

    public static void main( String [] args )
    {
        new Main().run();
    }
}
