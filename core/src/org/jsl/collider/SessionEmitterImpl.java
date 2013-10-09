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

import java.io.IOException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.HashSet;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;


public abstract class SessionEmitterImpl
{
    private static final Logger s_logger = Logger.getLogger( SessionEmitterImpl.class.getName() );

    protected final ColliderImpl m_collider;
    protected final InputQueue.DataBlockCache m_inputQueueDataBlockCache;
    protected final OutputQueue.DataBlockCache m_outputQueueDataBlockCache;

    private final SessionEmitter m_sessionEmitter;
    private final ReentrantLock m_lock;
    private final Condition m_cond;
    private final HashSet<Thread> m_callbackThreads;
    private int m_pendingOps;

    private class SessionStarter1 extends ColliderImpl.SelectorThreadRunnable
    {
        private final Selector m_selector;
        private final SocketChannel m_socketChannel;

        public SessionStarter1( Selector selector, SocketChannel socketChannel )
        {
            m_selector = selector;
            m_socketChannel = socketChannel;
        }

        public void runInSelectorThread()
        {
            try
            {
                SelectionKey selectionKey = m_socketChannel.register( m_selector, 0, null );
                m_collider.executeInThreadPool( new SessionStarter2(m_socketChannel, selectionKey) );
            }
            catch (IOException ex)
            {
                m_collider.executeInThreadPool( new ExceptionNotifier(ex, m_socketChannel) );
            }
        }
    }

    private class SessionStarter2 extends ThreadPool.Runnable
    {
        private final SocketChannel m_socketChannel;
        private final SelectionKey m_selectionKey;

        public SessionStarter2( SocketChannel socketChannel, SelectionKey selectionKey )
        {
            m_socketChannel = socketChannel;
            m_selectionKey = selectionKey;
        }

        public void runInThreadPool()
        {
            Thread currentThread = Thread.currentThread();
            addThread( currentThread );
            try
            {
                SessionImpl sessionImpl = new SessionImpl(
                        m_collider,
                        m_socketChannel,
                        m_selectionKey,
                        m_outputQueueDataBlockCache );

                Session.Listener sessionListener = m_sessionEmitter.createSessionListener( sessionImpl );
                sessionImpl.initialize(
                        m_sessionEmitter.inputQueueMaxSize,
                        m_inputQueueDataBlockCache,
                        sessionListener );
            }
            finally
            {
                removeThreadAndReleaseMonitor( currentThread );
            }
        }
    }

    protected class ExceptionNotifier extends ThreadPool.Runnable
    {
        private final IOException m_exception;
        private final SelectableChannel m_channel;

        public ExceptionNotifier( IOException exception, SelectableChannel channel )
        {
            m_exception = exception;
            m_channel = channel;
        }

        public void runInThreadPool()
        {
            if (s_logger.isLoggable(Level.WARNING))
                s_logger.warning( m_exception.toString() );

            Thread currentThread = Thread.currentThread();
            addThread( currentThread );

            try
            {
                m_sessionEmitter.onException( m_exception );
            }
            finally
            {
                int pendingOps = removeThreadAndReleaseMonitor( currentThread );
                assert( pendingOps == 1 );
            }

            try
            {
                m_channel.close();
            }
            catch (IOException ex)
            {
                if (s_logger.isLoggable(Level.WARNING))
                    s_logger.warning( ex.toString() );
            }
        }
    }

    protected SessionEmitterImpl(
            ColliderImpl collider,
            InputQueue.DataBlockCache inputQueueDataBlockCache,
            OutputQueue.DataBlockCache outputQueueDataBlockCache,
            SessionEmitter sessionEmitter )
    {
        m_collider = collider;
        m_inputQueueDataBlockCache = inputQueueDataBlockCache;
        m_outputQueueDataBlockCache = outputQueueDataBlockCache;

        if (sessionEmitter.inputQueueMaxSize == 0)
            sessionEmitter.inputQueueMaxSize = collider.getConfig().inputQueueMaxSize;
        m_sessionEmitter = sessionEmitter;

        m_lock = new ReentrantLock();
        m_cond = m_lock.newCondition();
        m_callbackThreads = new HashSet<Thread>();
        m_pendingOps = 1;
    }

    protected final void addThread( Thread thread )
    {
        m_lock.lock();
        try
        {
            m_callbackThreads.add( thread );
        }
        finally
        {
            m_lock.unlock();
        }
    }

    protected final void removeThread( Thread thread )
    {
        m_lock.lock();
        try
        {
            m_callbackThreads.remove( thread );
        }
        finally
        {
            m_lock.unlock();
        }
    }

    protected final int removeThreadAndReleaseMonitor( Thread thread )
    {
        boolean done = false;
        int pendingOps;

        m_lock.lock();
        try
        {
            pendingOps = m_pendingOps;
            if (m_callbackThreads.remove(thread))
            {
                if (--m_pendingOps == 0)
                {
                    m_cond.signalAll();
                    done = true;
                }
            }
        }
        finally
        {
            m_lock.unlock();
        }

        if (done)
            m_collider.removeEmitterNoWait( m_sessionEmitter );

        return pendingOps;
    }

    protected final int releaseMonitor()
    {
        boolean done = false;
        int pendingOps;

        m_lock.lock();
        try
        {
            pendingOps = m_pendingOps;
            if (--m_pendingOps == 0)
            {
                m_cond.signalAll();
                done = true;
            }
        }
        finally
        {
            m_lock.unlock();
        }

        if (done)
            m_collider.removeEmitterNoWait( m_sessionEmitter );

        return pendingOps;
    }

    protected final void waitMonitor() throws InterruptedException
    {
        Thread currentThread = Thread.currentThread();
        m_lock.lock();
        try
        {
            if (m_callbackThreads.remove(currentThread))
            {
                /* Called from the listener callback. */
                m_pendingOps--;
            }

            while (m_pendingOps > 0)
                m_cond.await();
        }
        finally
        {
            m_lock.unlock();
        }
    }

    protected final void startSession( Selector selector, SocketChannel socketChannel )
    {
        m_sessionEmitter.configureSocketChannel( m_collider, socketChannel );
        SessionStarter1 starter = new SessionStarter1( selector, socketChannel );

        m_lock.lock();
        try
        {
            m_pendingOps++;
        }
        finally
        {
            m_lock.unlock();
        }

        m_collider.executeInSelectorThread( starter );
    }

    public abstract void stopAndWait() throws InterruptedException;
}
