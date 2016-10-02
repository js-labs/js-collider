/*
 * JS-Collider framework.
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

package org.jsl.tests.thread_pool;

import org.jsl.collider.ThreadPool;
import java.util.concurrent.CountDownLatch;

public class Main
{
    private final Runnable [] m_task;
    private final CountDownLatch m_done;
    private final ThreadPool m_threadPool;

    private class Runnable extends ThreadPool.Runnable
    {
        private Thread m_worker;
        private int m_events;

        public Runnable( int events )
        {
            m_events = events;
        }

        public void runInThreadPool()
        {
            assert( m_worker == null );
            m_worker = Thread.currentThread();
            if (--m_events == 0)
            {
                m_worker = null;
                m_done.countDown();
            }
            else
            {
                m_worker = null;
                m_threadPool.execute( this );
            }
        }
    }

    private Main( int tasks, int events )
    {
        m_task = new Runnable[tasks];
        for (int idx=0; idx<tasks; idx++)
            m_task[idx] = new Runnable( events );
        m_done = new CountDownLatch( tasks );
        m_threadPool = new ThreadPool( "TTP", tasks+1 );
    }

    private void run()
    {
        m_threadPool.start();
        for (int idx=0; idx<m_task.length; idx++)
            m_threadPool.execute( m_task[idx] );

        try
        {
            m_done.await();
            m_threadPool.stopAndWait();
        }
        catch (final InterruptedException ex)
        {
            ex.printStackTrace();
        }

        System.out.println( "Test done." );
    }

    public static void main( String [] args )
    {
        final int tasks = 4;
        final int events = 100000;
        new Main( tasks, events ).run();
    }
}
