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

package org.jsl.tests.binary_queue;

import org.jsl.collider.BinaryQueue;
import org.jsl.collider.DataBlockCache;

import java.nio.ByteBuffer;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class Main
{
    private static final int OPS = 1000000;

    private class Producer1 extends Thread
    {
        private final int m_id;
        private final BinaryQueue m_queue;

        public Producer1( int id, BinaryQueue queue )
        {
            m_id = id;
            m_queue = queue;
        }

        public void run()
        {
            System.out.println( m_id + ": started" );

            int ops = 0;
            final ByteBuffer byteBuffer = ByteBuffer.allocateDirect( 4*8 );
            byteBuffer.putInt( 0x11111111 );
            byteBuffer.putInt( 0x22222222 );
            byteBuffer.putInt( 0x33333333 );
            byteBuffer.putInt( 0x44444444 );

            for (int idx=0; idx<OPS; idx++)
            {
                byteBuffer.position(0);
                final ByteBuffer bb = m_queue.putData( byteBuffer );
                if (bb != null)
                {
                    m_lock.lock();
                    try
                    {
                        m_bb = bb;
                        m_cond.signal();
                    }
                    finally
                    {
                        m_lock.unlock();
                    }
                    ops++;
                }
            }

            System.out.println( m_id + ": done (" + ops + ")" );
        }
    }

    private class Producer2 extends Thread
    {
        private final int m_id;
        private final BinaryQueue m_queue;

        public Producer2( int id, BinaryQueue queue )
        {
            m_id = id;
            m_queue = queue;
        }

        public void run()
        {
            System.out.println( m_id + ": started" );

            int ops = 0;
            final ByteBuffer byteBuffer = ByteBuffer.allocateDirect( 4*8 );
            byteBuffer.putInt( 0x11111111 );
            byteBuffer.putInt( 0x22222222 );
            byteBuffer.putInt( 0x33333333 );
            byteBuffer.putInt( 0x44444444 );

            for (int idx=0; idx<OPS; idx++)
            {
                byteBuffer.position(0);
                ByteBuffer bb = m_queue.putDataNoCopy( byteBuffer );
                if (bb != null)
                {
                    do
                    {
                        int valid = 0x11111111;
                        for (int j=0; j<4; j++)
                        {
                            int v = bb.getInt();
                            if (v != valid)
                                throw new RuntimeException( "Unexpected value " + v + ", should be " + valid );
                            valid += 0x11111111;
                        }
                        ops++;
                        m_ops++;
                        bb = m_queue.getNext();
                    }
                    while (bb != null);
                }
            }

            m_lock.lock();
            try
            {
                m_cond.signal();
            }
            finally
            {
                m_lock.unlock();
            }

            System.out.println( m_id + ": done (" + ops + ")" );
        }
    }

    private final ReentrantLock m_lock;
    private final Condition m_cond;
    private ByteBuffer m_bb;
    private int m_ops;

    private Main()
    {
        m_lock = new ReentrantLock();
        m_cond = m_lock.newCondition();
    }

    private void run()
    {
        final DataBlockCache dataBlockCache = new DataBlockCache( true, 4*1024, 4, 64 );
        final BinaryQueue queue =  new BinaryQueue( dataBlockCache );
        final Thread [] threads = new Thread[4];

        for (int idx=0; idx<threads.length; idx++)
        {
            if ((idx & 1) == 0)
                threads[idx] = new Producer1( idx, queue );
            else
                threads[idx] = new Producer2( idx, queue );
            threads[idx].start();
        }

        try
        {
            ByteBuffer bb = null;
            int waits = 0;

            loop: for (;;)
            {
                if (bb == null)
                {
                    waits++;
                    m_lock.lock();
                    try
                    {
                        for (;;)
                        {
                            if (m_bb != null)
                            {
                                bb = m_bb;
                                m_bb = null;
                                break;
                            }

                            if (m_ops == threads.length * OPS)
                                break loop;

                            m_cond.await();
                        }
                    }
                    finally
                    {
                        m_lock.unlock();
                    }
                }

                int valid = 0x11111111;
                for (int idx = 0; idx < 4; idx++)
                {
                    int v = bb.getInt();
                    if (v != valid)
                        throw new RuntimeException( "Unexpected value " + v + ", should be " + valid );
                    valid += 0x11111111;
                }

                if (++m_ops == threads.length * OPS)
                    break;

                bb = queue.getNext();
            }

            for (Thread t : threads)
                t.join();

            System.out.println( m_ops + " events processed (" + waits + " waits)." );
        }
        catch (InterruptedException ex)
        {
            ex.printStackTrace();
        }
    }

    public static void main( String [] args )
    {
        new Main().run();
    }
}
