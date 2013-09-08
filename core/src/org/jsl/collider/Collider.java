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
import java.net.StandardSocketOptions;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.util.Set;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;


public class Collider
{
    public static class Config
    {
        public int threadPoolThreads;
        public boolean useDirectBuffers;
        public int shutdownTimeout;

        public int socketSendBufSize;
        public int socketRecvBufSize;
        public int inputQueueBlockSize;
        public int inputQueueCacheInitialSize;
        public int inputQueueCacheMaxSize;
        public int outputQueueBlockSize;
        public int outputQueueCacheInitialSize;
        public int outputQueueCacheMaxSize;

        public Config()
        {
            threadPoolThreads = 0; /* by default = number of cores */
            useDirectBuffers  = true;
            shutdownTimeout   = 60;

            /* Use system default settings by default */
            socketSendBufSize = 0;
            socketRecvBufSize = 0;

            inputQueueBlockSize         = (32 * 1024);
            inputQueueCacheInitialSize  = 0;
            inputQueueCacheMaxSize      = 0; /* by default = (threadPoolThreads*3) */
            outputQueueBlockSize        = (16 * 1024);
            outputQueueCacheInitialSize = 0;
            outputQueueCacheMaxSize     = 0; /* by default = (threadPoolThreads*3) */
        }
    }

    public interface ChannelHandler
    {
        public void handleReadyOps( Executor executor );
    }

    public static abstract class SelectorThreadRunnable
    {
        public volatile SelectorThreadRunnable nextSelectorThreadRunnable;
        abstract public void runInSelectorThread();
    }

    private class Stopper1 extends ThreadPool.Runnable
    {
        public void runInThreadPool()
        {
            SessionEmitter [] emitters = null;
            m_lock.lock();
            try
            {
                int size = m_emitters.size();
                if (size > 0)
                {
                    emitters = new SessionEmitter[size];
                    Iterator<SessionEmitter> it = m_emitters.keySet().iterator();
                    for (int idx=0; idx<size; idx++)
                        emitters[idx] = it.next();
                }
            }
            finally
            {
                m_lock.unlock();
            }

            if (emitters != null)
            {
                boolean interrupted = false;
                try
                {
                    for (SessionEmitter emitter : emitters)
                    {
                        try
                        {
                            removeEmitter( emitter );
                        }
                        catch (InterruptedException ex)
                        {
                            if (s_logger.isLoggable(Level.WARNING))
                                s_logger.warning( ex.toString() );
                            interrupted = true;
                        }
                    }
                }
                finally
                {
                    if (interrupted)
                        Thread.currentThread().interrupt();
                }
            }

            executeInSelectorThread( new Stopper2() );
        }
    }

    private class Stopper2 extends SelectorThreadRunnable
    {
        public void runInSelectorThread()
        {
            Set<SelectionKey> keys = m_selector.keys();
            for (SelectionKey key : keys)
            {
                Object attachment = key.attachment();
                if (attachment instanceof SessionImpl)
                    ((SessionImpl)attachment).closeConnection();
                /*
                 * else if (attachment instanceof AcceptorImpl)
                 * {
                 *     Can happen, canceled SelectionKey is not removed from the
                 *     Selector right at the SelectionKey.cancel() call,
                 *     but will present in the keys set till the next
                 *     Selector.select() call.
                 * }
                 */
            }
            m_state = ST_STOPPING;
        }
    }

    private void removeEmitter( SessionEmitter emitter ) throws InterruptedException
    {
        SessionEmitterImpl emitterImpl;
        m_lock.lock();
        try
        {
            emitterImpl = m_emitters.get( emitter );
            if (emitterImpl == null)
                return;
            m_emitters.remove( emitter );
        }
        finally
        {
            m_lock.unlock();
        }
        emitterImpl.stop();
    }

    private final static int ST_RUNNING = 1;
    private final static int ST_STOPPING = 2;

    private static final Logger s_logger = Logger.getLogger( Collider.class.getName() );

    private final Config m_config;
    private final Selector m_selector;
    private final ExecutorService m_executor;
    private final ThreadPool m_threadPool;
    private int m_state;

    private final ReentrantLock m_lock;
    private final Map<SessionEmitter, SessionEmitterImpl> m_emitters;
    private final Map<Integer, InputQueue.DataBlockCache> m_inputQueueDataBlockCache;
    private final Map<Integer, OutputQueue.DataBlockCache> m_outputQueueDataBlockCache;
    private boolean m_stop;

    private volatile SelectorThreadRunnable m_strHead;
    private AtomicReference<SelectorThreadRunnable> m_strTail;

    public Collider() throws IOException
    {
        this( new Config() );
    }

    public Collider( Config config ) throws IOException
    {
        m_config = config;
        m_selector = Selector.open();

        int threadPoolThreads = config.threadPoolThreads;
        if (threadPoolThreads == 0)
            threadPoolThreads = Runtime.getRuntime().availableProcessors();
        if (threadPoolThreads < 4)
            threadPoolThreads = 4;
        m_executor = Executors.newFixedThreadPool( 2 );
        m_threadPool = new ThreadPool( "CTP", threadPoolThreads );

        if (config.inputQueueCacheMaxSize == 0)
            config.inputQueueCacheMaxSize = (threadPoolThreads * 3);

        m_state = ST_RUNNING;

        m_lock = new ReentrantLock();
        m_emitters = new HashMap<SessionEmitter, SessionEmitterImpl>();
        m_inputQueueDataBlockCache = new HashMap<Integer, InputQueue.DataBlockCache>();
        m_outputQueueDataBlockCache = new HashMap<Integer, OutputQueue.DataBlockCache>();
        m_stop = false;

        m_strHead = null;
        m_strTail = new AtomicReference<SelectorThreadRunnable>();
    }

    public Config getConfig()
    {
        return m_config;
    }

    public void run()
    {
        if (s_logger.isLoggable(Level.FINE))
            s_logger.fine( "Starting collider." );

        m_threadPool.start();
        for (;;)
        {
            try
            {
                //long startTime = System.nanoTime();
                m_selector.select();
                //long endTime = System.nanoTime();
                //System.out.println( "selector.select() " + Util.formatDelay(startTime, endTime) );
            }
            catch (IOException ex)
            {
                if (s_logger.isLoggable(Level.WARNING))
                    s_logger.warning( ex.toString() );
            }

            Set<SelectionKey> selectedKeys = m_selector.selectedKeys();
            for (SelectionKey key : selectedKeys)
            {
                ChannelHandler channelHandler = (ChannelHandler) key.attachment();
                channelHandler.handleReadyOps( m_executor );
            }
            selectedKeys.clear();

            while (m_strHead != null)
            {
                m_strHead.runInSelectorThread();
                SelectorThreadRunnable head = m_strHead;
                SelectorThreadRunnable next = m_strHead.nextSelectorThreadRunnable;
                if (next == null)
                {
                    m_strHead = null;
                    if (m_strTail.compareAndSet(head, null))
                        break;
                    while (head.nextSelectorThreadRunnable == null);
                    next = head.nextSelectorThreadRunnable;
                }
                m_strHead = next;
                head.nextSelectorThreadRunnable = null;
            }

            if (m_state == ST_STOPPING)
            {
                if (m_selector.keys().size() == 0)
                    break;
                m_selector.wakeup();
            }
        }

        m_executor.shutdown();
        try
        {
            if (!m_executor.awaitTermination(m_config.shutdownTimeout, TimeUnit.SECONDS))
                m_executor.shutdownNow();
            m_executor.awaitTermination( m_config.shutdownTimeout, TimeUnit.SECONDS );
            m_threadPool.stopAndWait();
        }
        catch (InterruptedException ex)
        {
            if (s_logger.isLoggable(Level.WARNING))
                s_logger.warning( ex.toString() );
        }

        if (s_logger.isLoggable(Level.FINE))
            s_logger.fine( "Collider stopped." );
    }

    public void stop()
    {
        if (s_logger.isLoggable(Level.FINE))
            s_logger.fine( "Stopping collider." );

        m_lock.lock();
        try
        {
            if (m_stop)
                return;
            m_stop = true;
        }
        finally
        {
            m_lock.unlock();
        }

        executeInThreadPool( new Stopper1() );
    }

    public void executeInSelectorThread( SelectorThreadRunnable runnable )
    {
        assert( runnable.nextSelectorThreadRunnable == null );
        SelectorThreadRunnable tail = m_strTail.getAndSet( runnable );
        if (tail == null)
        {
            m_strHead = runnable;
            m_selector.wakeup();
        }
        else
            tail.nextSelectorThreadRunnable = runnable;
    }

    public void executeInThreadPool( Runnable runnable )
    {
        m_executor.execute( runnable );
    }

    public void executeInThreadPool( ThreadPool.Runnable runnable )
    {
        m_threadPool.execute( runnable );
    }

    public void addAcceptor( Acceptor acceptor )
    {
        int inputQueueBlockSize = acceptor.inputQueueBlockSize;
        if (inputQueueBlockSize == 0)
        {
            inputQueueBlockSize = m_config.inputQueueBlockSize;
            assert( inputQueueBlockSize > 0 );
        }

        int outputQueueBlockSize = acceptor.outputQueueBlockSize;
        if (outputQueueBlockSize == 0)
        {
            outputQueueBlockSize = m_config.outputQueueBlockSize;
            assert( outputQueueBlockSize > 0 );
        }

        InputQueue.DataBlockCache inputQueueDataBlockCache;
        OutputQueue.DataBlockCache outputQueueDataBlockCache;

        m_lock.lock();
        try
        {
            inputQueueDataBlockCache = m_inputQueueDataBlockCache.get( inputQueueBlockSize );
            if (inputQueueDataBlockCache == null)
            {
                inputQueueDataBlockCache = new InputQueue.DataBlockCache(
                        m_config.useDirectBuffers,
                        inputQueueBlockSize,
                        m_config.inputQueueCacheInitialSize,
                        m_config.inputQueueCacheMaxSize );
                m_inputQueueDataBlockCache.put( inputQueueBlockSize, inputQueueDataBlockCache );
            }

            outputQueueDataBlockCache = m_outputQueueDataBlockCache.get( outputQueueBlockSize );
            if (outputQueueDataBlockCache == null)
            {
                outputQueueDataBlockCache = new OutputQueue.DataBlockCache(
                        m_config.useDirectBuffers,
                        outputQueueBlockSize,
                        m_config.outputQueueCacheInitialSize,
                        m_config.outputQueueCacheMaxSize );
                m_outputQueueDataBlockCache.put( outputQueueBlockSize, outputQueueDataBlockCache );
            }
        }
        finally
        {
            m_lock.unlock();
        }

        ServerSocketChannel channel;
        try
        {
            channel = ServerSocketChannel.open();
            channel.configureBlocking( false );
            channel.setOption( StandardSocketOptions.SO_REUSEADDR, acceptor.reuseAddr );
            channel.socket().bind( acceptor.getAddr() );
        }
        catch (IOException ex)
        {
            acceptor.onAcceptorStartingFailure( ex.toString() );
            return;
        }

        AcceptorImpl acceptorImpl = new AcceptorImpl(
                this,
                m_selector,
                inputQueueDataBlockCache,
                outputQueueDataBlockCache,
                acceptor,
                channel );

        String errorMessage = null;

        m_lock.lock();
        try
        {
            if (m_stop)
                errorMessage = "Collider stopped.";
            else if (m_emitters.containsKey(acceptor))
                errorMessage = "Acceptor already registered.";
            else
                m_emitters.put( acceptor, acceptorImpl );
        }
        finally
        {
            m_lock.unlock();
        }

        if (errorMessage == null)
            acceptorImpl.start();
        else
        {
            acceptor.onAcceptorStartingFailure( errorMessage );
            try
            {
                channel.close();
            }
            catch (IOException ex)
            {
                if (s_logger.isLoggable(Level.WARNING))
                    s_logger.warning( ex.toString() );
            }
        }
    }

    public void removeAcceptor( Acceptor acceptor ) throws InterruptedException
    {
        removeEmitter( acceptor );
    }
}
