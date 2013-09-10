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

package org.jsl.tests.sched_latency;

import org.jsl.collider.ThreadPool;
import java.util.concurrent.Semaphore;

public class SL_ThreadPool
{
    private final long [] m_res;
    private final Semaphore m_semDone;
    private ThreadPool [] m_threadPool;
    private long m_time;
    private int m_idx;

    private class TestRunnable extends ThreadPool.Runnable
    {
        public void runInThreadPool()
        {
            if (m_idx < m_res.length)
            {
                long time = System.nanoTime();
                m_res[m_idx] = (time - m_time);
                m_idx++;
                m_time = time;
                m_threadPool[m_idx%2].execute( this );
            }
            else
                m_semDone.release();
        }
    }

    public SL_ThreadPool( long [] res )
    {
        m_res = res;
        m_semDone = new Semaphore(0);
    }

    public void start()
    {
        TestRunnable runnable = new TestRunnable();
        m_threadPool = new ThreadPool[2];
        m_threadPool[0] = new ThreadPool( "TP1", 1 );
        m_threadPool[1] = new ThreadPool( "TP2", 1 );
        m_threadPool[0].start();
        m_threadPool[1].start();

        m_time = System.nanoTime();
        m_threadPool[0].execute( runnable );

        try
        {
            m_semDone.acquire(1);
            m_threadPool[0].stopAndWait();
            m_threadPool[1].stopAndWait();
        }
        catch (InterruptedException ex)
        {
            System.out.println(ex);
        }
    }
}
