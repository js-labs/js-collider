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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;


public class SL_Executor
{
    private final long [] m_res;
    private final Semaphore m_semDone;
    private final ExecutorService [] m_executor;
    private long m_time;
    private int m_idx;

    private class TestRunnable implements Runnable
    {
        public void run()
        {
            if (m_idx < m_res.length)
            {
                long time = System.nanoTime();
                m_res[m_idx] = (time - m_time);
                m_idx++;
                m_time = time;
                m_executor[m_idx%2].execute( this );
            }
            else
                m_semDone.release();
        }
    }

    public SL_Executor( long [] res )
    {
        m_res = res;
        m_semDone = new Semaphore(0);
        m_executor = new ExecutorService[2];
        m_executor[0] = Executors.newFixedThreadPool(1);
        m_executor[1] = Executors.newFixedThreadPool(1);
    }

    public void start()
    {
        TestRunnable runnable = new TestRunnable();

        m_time = System.nanoTime();
        m_executor[0].execute( runnable );

        try { m_semDone.acquire(1); }
        catch (InterruptedException ex) { System.out.println(ex); }

        for (int idx=0; idx<2; idx++)
        {
            m_executor[idx].shutdown();
            try
            {
                if (!m_executor[idx].awaitTermination(2, TimeUnit.SECONDS))
                    m_executor[idx].shutdownNow();
                m_executor[idx].awaitTermination( 2, TimeUnit.SECONDS );
            }
            catch (InterruptedException ex)
            {
                ex.printStackTrace();
            }
        }
    }
}
