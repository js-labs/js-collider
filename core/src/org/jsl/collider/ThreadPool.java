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

package org.jsl.collider;

import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.AbstractQueuedSynchronizer;
import java.util.logging.Level;
import java.util.logging.Logger;


public class ThreadPool
{
    public static abstract class Runnable
    {
        public volatile Runnable nextThreadPoolRunnable;
        public abstract void runInThreadPool();
    }

    private static class Sync extends AbstractQueuedSynchronizer
    {
        private int m_maxState;

        public Sync( int maxState )
        {
            m_maxState = maxState;
        }

        protected final int tryAcquireShared( int acquires )
        {
            for (;;)
            {
                int state = getState();
                int newState = (state - 1);
                if ((newState < 0) || compareAndSetState(state, newState))
                    return newState;
            }
        }

        protected final boolean tryReleaseShared( int releases )
        {
            for (;;)
            {
                int state = getState();
                if (state == m_maxState)
                    return true;
                int newState = (state + releases);
                if (compareAndSetState(state, newState))
                    return true;
            }
        }
    }

    private static class Tls
    {
        private int m_idx;

        public Tls()
        {
            m_idx = 0;
        }

        public final int getNext()
        {
            if (++m_idx < 0)
                m_idx = 0;
            return m_idx;
        }
    }

    private class Worker extends Thread
    {
        public Worker( String name )
        {
            super( name );
        }

        public void run()
        {
            if (s_logger.isLoggable(Level.FINE))
                s_logger.fine( Thread.currentThread().getName() + ": started." );

            int rqIdx = 0;
            while (m_run)
            {
                m_sync.acquireShared(1);
                int cc = m_contentionFactor;
                for (;;)
                {
                    Runnable runnable = getNext( rqIdx );
                    if (runnable == null)
                    {
                        if (--cc == 0)
                            break;
                    }
                    else
                    {
                        runnable.runInThreadPool();
                        cc = m_contentionFactor;
                    }
                    rqIdx++;
                    rqIdx %= m_contentionFactor;
                }
            }

            if (s_logger.isLoggable(Level.FINE))
                s_logger.fine( Thread.currentThread().getName() + ": finished." );
        }
    }

    private Runnable getNext( int rqIdx )
    {
        rqIdx *= (FS_PADDING * 2);
        Runnable head = m_rq.get( rqIdx );
        for (;;)
        {
            if (head == null)
                return null;

            Runnable next = head.nextThreadPoolRunnable;
            if (m_rq.compareAndSet(rqIdx, head, next))
            {
                if (next == null)
                {
                    rqIdx += FS_PADDING;
                    if (!m_rq.compareAndSet(rqIdx, head, null))
                    {
                        while (head.nextThreadPoolRunnable == null);
                        rqIdx -= FS_PADDING;
                        m_rq.set( rqIdx, head.nextThreadPoolRunnable );
                    }
                }
                head.nextThreadPoolRunnable = null;
                return head;
            }
            head = m_rq.get( rqIdx );
        }
    }

    private static final Logger s_logger = Logger.getLogger( ThreadPool.class.getName() );

    private static final int FS_PADDING = 8;
    private final int m_contentionFactor;
    private final Sync m_sync;
    private final Thread [] m_thread;
    private final AtomicReferenceArray<Runnable> m_rq;
    private final ThreadLocal<Tls> m_tls;
    private volatile boolean m_run;

    public ThreadPool( String name, int threads, int contentionFactor )
    {
        assert( contentionFactor >= 1 );
        if (contentionFactor < 1)
            contentionFactor = 1;

        m_contentionFactor = contentionFactor;
        m_sync = new Sync( threads );

        m_thread = new Thread[threads];
        for (int idx=0; idx<threads; idx++)
        {
            String workerName = (name + "-" + idx);
            m_thread[idx] = new Worker( workerName );
        }

        m_rq = new AtomicReferenceArray<Runnable>( contentionFactor * FS_PADDING * 2 );
        m_tls = new ThreadLocal<Tls>() { protected Tls initialValue() { return new Tls(); } };
        m_run = true;
    }

    public ThreadPool( String name, int threads )
    {
        this( name, threads, threads*2 );
    }

    public final void start()
    {
        for (Thread thread : m_thread)
            thread.start();
    }

    public final void stopAndWait() throws InterruptedException
    {
        assert( m_thread != null );

        m_run = false;
        m_sync.releaseShared( m_thread.length );

        for (int idx=0; idx<m_thread.length; idx++)
        {
            m_thread[idx].join();
            m_thread[idx] = null;
        }
    }

    public final void execute( Runnable runnable )
    {
        assert( runnable.nextThreadPoolRunnable == null );

        int idx = (m_tls.get().getNext() % m_contentionFactor);
        idx *= (FS_PADDING * 2);
        idx += FS_PADDING;

        Runnable tail = m_rq.getAndSet( idx, runnable );
        if (tail == null)
        {
            idx -= FS_PADDING;
            m_rq.set( idx, runnable );
        }
        else
            tail.nextThreadPoolRunnable = runnable;

        m_sync.releaseShared(1);
    }
}
