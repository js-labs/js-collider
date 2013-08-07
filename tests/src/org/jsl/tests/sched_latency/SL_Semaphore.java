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

import java.util.concurrent.Semaphore;

public class SL_Semaphore
{
    private final long [] m_res;
    private final Semaphore m_semDone;
    private long m_time;
    private int m_idx;

    private class TestThread implements Runnable
    {
        private final Semaphore m_sem1;
        private final Semaphore m_sem2;

        public TestThread( Semaphore sem1, Semaphore sem2 )
        {
            m_sem1 = sem1;
            m_sem2 = sem2;
        }

        public void run()
        {
            for (;;)
            {
                try { m_sem1.acquire(); }
                catch (InterruptedException ex) { System.out.println(ex); }

                if (m_idx == m_res.length)
                    break;

                long time = System.nanoTime();
                m_res[m_idx] = (time - m_time);
                m_idx++;
                m_time = time;

                m_sem2.release();
            }

            m_sem2.release();
            m_semDone.release();
        }
    }

    public SL_Semaphore( long [] res )
    {
        m_res = res;
        m_semDone = new Semaphore(0);
    }

    public void start()
    {
        Semaphore sem1 = new Semaphore(0);
        Semaphore sem2 = new Semaphore(0);

        m_time = System.nanoTime();
        new Thread(new TestThread(sem1, sem2)).start();
        new Thread(new TestThread(sem2, sem1)).start();

        m_time = System.nanoTime();
        sem1.release();

        try { m_semDone.acquire(2); }
        catch (InterruptedException ex) { System.out.println(ex); }
    }
}

