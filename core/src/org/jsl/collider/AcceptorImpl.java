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
import java.nio.channels.*;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;


public class AcceptorImpl extends ThreadPool.Runnable
        implements ColliderImpl.ChannelHandler, SessionEmitterImpl
{
    private class SessionStarter1 extends ColliderImpl.SelectorThreadRunnable
    {
        private final SessionImpl m_sessionImpl;

        public SessionStarter1( SessionImpl sessionImpl )
        {
            m_sessionImpl = sessionImpl;
        }

        public void runInSelectorThread()
        {
            SocketChannel socketChannel = m_sessionImpl.register( m_selector );
            m_collider.executeInThreadPool( new SessionStarter2(m_sessionImpl, socketChannel) );
        }
    }

    private class SessionStarter2 extends ThreadPool.Runnable
    {
        private final SessionImpl m_sessionImpl;
        private final SocketChannel m_socketChannel;

        public SessionStarter2( SessionImpl sessionImpl, SocketChannel socketChannel )
        {
            m_sessionImpl = sessionImpl;
            m_socketChannel = socketChannel;
        }

        public void runInThreadPool()
        {
            if (m_socketChannel == null)
            {
                if (s_logger.isLoggable(Level.FINE))
                    s_logger.fine( m_sessionImpl.getRemoteAddress().toString() );

                Thread currentThread = Thread.currentThread();

                m_lock.lock();
                try
                {
                    m_callbackThreads.add(currentThread);
                }
                finally
                {
                    m_lock.unlock();
                }

                Session.Listener sessionListener = m_acceptor.createSessionListener( m_sessionImpl );

                m_lock.lock();
                try
                {
                    if (m_callbackThreads.remove(currentThread))
                    {
                        int pendingOps = m_pendingOps.decrementAndGet();
                        assert( pendingOps >= 0 );
                        if (pendingOps == 0)
                        {
                            m_running = false;
                            m_cond.signalAll();
                        }
                    }
                }
                finally
                {
                    m_lock.unlock();
                }

                m_sessionImpl.initialize( m_inputQueueDataBlockCache, sessionListener );
            }
            else
            {
                pendingOpsDec();
                try
                {
                    m_socketChannel.close();
                }
                catch (IOException ex)
                {
                    if (s_logger.isLoggable(Level.WARNING))
                        s_logger.warning( ex.toString() );
                }
            }
        }
    }

    private class ErrorNotifier extends ThreadPool.Runnable
    {
        private final IOException m_exception;

        public ErrorNotifier( IOException exception )
        {
            m_exception = exception;
        }

        public void runInThreadPool()
        {
            Thread currentThread = Thread.currentThread();

            m_lock.lock();
            try
            {
                m_callbackThreads.add( currentThread );
            }
            finally
            {
                m_lock.unlock();
            }

            m_acceptor.onAcceptorStartingFailure( m_exception.toString() );

            m_lock.lock();
            try
            {
                if (m_callbackThreads.remove(currentThread))
                {
                    m_pendingOps.set( 0 );
                    m_running = false;
                    m_cond.signalAll(); /* May be some thread waiting. */
                }
            }
            finally
            {
                m_lock.unlock();
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
            m_channel = null;
        }
    }

    private class ChannelCloser extends ColliderImpl.SelectorThreadRunnable
    {
        public void runInSelectorThread()
        {
            closeSelectionKeyAndChannel();
        }
    }

    private class Starter1 extends ColliderImpl.SelectorThreadRunnable
    {
        public void runInSelectorThread()
        {
            try
            {
                m_selectionKey = m_channel.register( m_selector, 0, AcceptorImpl.this );
                m_collider.executeInThreadPool( new Starter2() );
            }
            catch (IOException ex)
            {
                m_collider.executeInThreadPool( new ErrorNotifier(ex) );
            }
        }
    }

    private class Starter2 extends ThreadPool.Runnable
    {
        public void runInThreadPool()
        {
            Thread currentThread = Thread.currentThread();

            m_lock.lock();
            try
            {
                m_callbackThreads.add( currentThread );
            }
            finally
            {
                m_lock.unlock();
            }

            m_acceptor.onAcceptorStarted( m_channel.socket().getLocalPort() );

            ColliderImpl.SelectorThreadRunnable nextRunnable = m_selectorRegistrator;
            m_lock.lock();
            try
            {
                if (m_callbackThreads.remove(currentThread))
                {
                    if (!m_run)
                    {
                        m_pendingOps.set(0);
                        m_running = false;
                        m_cond.signalAll();
                        nextRunnable = new ChannelCloser();
                    }
                }
            }
            finally
            {
                m_lock.unlock();
            }
            m_collider.executeInSelectorThread( nextRunnable );
        }
    }

    private class Stopper extends ColliderImpl.SelectorThreadRunnable
    {
        public void runInSelectorThread()
        {
            if (m_selectionKey != null)
            {
                int interestOps = m_selectionKey.interestOps();
                if ((interestOps & SelectionKey.OP_ACCEPT) != 0)
                {
                    pendingOpsDec();
                    closeSelectionKeyAndChannel();
                }
            }
        }
    }

    private class SelectorRegistrator extends ColliderImpl.SelectorThreadRunnable
    {
        public void runInSelectorThread()
        {
            if (m_selectionKey != null)
            {
                assert( (m_selectionKey.interestOps() & SelectionKey.OP_ACCEPT) == 0 );
                if (m_run)
                {
                    m_selectionKey.interestOps( SelectionKey.OP_ACCEPT );
                }
                else
                {
                    pendingOpsDec();
                    closeSelectionKeyAndChannel();
                }
            }
        }
    }

    void closeSelectionKeyAndChannel()
    {
        m_selectionKey.cancel();
        m_selectionKey = null;
        try
        {
            m_channel.close();
        }
        catch (IOException ex)
        {
            if (s_logger.isLoggable(Level.WARNING))
                s_logger.warning( ex.toString() );
        }
        m_channel = null;
    }

    void pendingOpsDec()
    {
        int pendingOps = m_pendingOps.decrementAndGet();
        if (pendingOps == 0)
        {
            m_lock.lock();
            try
            {
                assert( m_running );
                m_running = false;
                m_cond.signalAll();
            }
            finally
            {
                m_lock.unlock();
            }
        }
    }

    private static final Logger s_logger = Logger.getLogger( AcceptorImpl.class.getName() );

    private final ColliderImpl m_collider;
    private final Selector m_selector;
    private final InputQueue.DataBlockCache m_inputQueueDataBlockCache;
    private final OutputQueue.DataBlockCache m_outputQueueDataBlockCache;
    private final Acceptor m_acceptor;

    private ServerSocketChannel m_channel;
    private SelectionKey m_selectionKey;

    private final SelectorRegistrator m_selectorRegistrator;
    private final AtomicInteger m_pendingOps;
    private final ReentrantLock m_lock;
    private final Condition m_cond;
    private final HashSet<Thread> m_callbackThreads;
    private volatile boolean m_run;
    private boolean m_running;

    public AcceptorImpl(
            ColliderImpl colliderImpl,
            Selector selector,
            InputQueue.DataBlockCache inputQueueDataBlockCache,
            OutputQueue.DataBlockCache outputQueueDataBlockCache,
            Acceptor acceptor,
            ServerSocketChannel channel )
    {
        m_collider = colliderImpl;
        m_selector = selector;
        m_inputQueueDataBlockCache = inputQueueDataBlockCache;
        m_outputQueueDataBlockCache = outputQueueDataBlockCache;
        m_acceptor = acceptor;

        m_channel = channel;

        m_selectorRegistrator = new SelectorRegistrator();
        m_pendingOps = new AtomicInteger(1);
        m_lock = new ReentrantLock();
        m_cond = m_lock.newCondition();
        m_callbackThreads = new HashSet<Thread>();
        m_run = true;
        m_running = true;
    }

    public void start()
    {
        if (s_logger.isLoggable(Level.FINE))
            s_logger.fine( m_channel.socket().getLocalSocketAddress().toString() );
        m_collider.executeInSelectorThread( new Starter1() );
    }

    public void stop() throws InterruptedException
    {
        /* Stop can actually be called even before start(),
         * but it is not a problem, should work properly.
         */
        Thread currentThread = Thread.currentThread();
        m_lock.lock();
        try
        {
            if (m_run)
            {
                m_run = false;
                m_collider.executeInSelectorThread( new Stopper() );
            }

            if (m_callbackThreads.remove(currentThread))
            {
                /* Called from the listener callback. */
                int pendingOps = m_pendingOps.decrementAndGet();
                if (pendingOps == 0)
                    m_running = false;
            }

            while (m_running)
                m_cond.await();
        }
        finally
        {
            m_lock.unlock();
        }
    }

    public void handleReadyOps( ThreadPool threadPool )
    {
        m_selectionKey.interestOps( 0 );
        threadPool.execute( this );
    }

    public void runInThreadPool()
    {
        while (m_run)
        {
            SocketChannel socketChannel;

            try
            {
                socketChannel = m_channel.accept();
                if (socketChannel == null)
                    break;
            }
            catch (IOException ex)
            {
                if (s_logger.isLoggable(Level.WARNING))
                    s_logger.warning( ex.toString() );
                break;
            }

            try { socketChannel.configureBlocking( false ); }
            catch (IOException ex)
            {
                /* Having unblocking mode is critical, can't work with this socket. */
                try { socketChannel.close(); }
                catch (IOException ignored) {}
                if (s_logger.isLoggable(Level.WARNING))
                    s_logger.warning( ex.toString() );
                continue;
            }

            m_pendingOps.incrementAndGet();
            m_acceptor.configureSocketChannel(m_collider, socketChannel );

            SessionImpl sessionImpl = new SessionImpl(m_collider, m_acceptor, socketChannel, m_outputQueueDataBlockCache );
            m_collider.executeInSelectorThread( new SessionStarter1(sessionImpl) );
        }

        m_collider.executeInSelectorThread( m_selectorRegistrator );
    }
}
