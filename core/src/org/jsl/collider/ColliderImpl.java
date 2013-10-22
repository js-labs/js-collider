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
import java.net.ServerSocket;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Set;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;


public class ColliderImpl extends Collider
{
    public interface ChannelHandler
    {
        public void handleReadyOps( ThreadPool threadPool );
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

    private static class DummyRunnable extends SelectorThreadRunnable
    {
        public void runInSelectorThread()
        {
        }
    }

    private class SelectorAlarm extends ThreadPool.Runnable
    {
        public SelectorAlarm next;
        public SelectorThreadRunnable str;

        public void runInThreadPool()
        {
            if (m_strHead == str)
                m_selector.wakeup();

            str = null;
            SelectorAlarm tail = m_alarmCache.getAndSet( 1, this );
            if (tail == null)
                m_alarmCache.set( 0, this );
            else
                tail.next = this;
        }
    }

    private InputQueue.DataBlockCache getInputQueueDataBlockCache( final SessionEmitter sessionEmitter )
    {
        final Config config = getConfig();

        int inputQueueBlockSize = sessionEmitter.inputQueueBlockSize;
        if (inputQueueBlockSize == 0)
        {
            inputQueueBlockSize = config.inputQueueBlockSize;
            assert( inputQueueBlockSize > 0 );
        }

        InputQueue.DataBlockCache cache = null;

        m_lock.lock();
        try
        {
            cache = m_inputQueueDataBlockCache.get( inputQueueBlockSize );
            if (cache == null)
            {
                cache = new InputQueue.DataBlockCache(
                    config.useDirectBuffers,
                    inputQueueBlockSize,
                    config.inputQueueCacheInitialSize,
                    config.inputQueueCacheMaxSize );
                m_inputQueueDataBlockCache.put( inputQueueBlockSize, cache );
            }
        }
        finally
        {
            m_lock.unlock();
        }

        return cache;
    }

    private DataBlockCache getOutputQueueDataBlockCache( final SessionEmitter sessionEmitter )
    {
        final Config config = getConfig();

        int outputQueueBlockSize = sessionEmitter.outputQueueBlockSize;
        if (outputQueueBlockSize == 0)
        {
            outputQueueBlockSize = config.outputQueueBlockSize;
            assert( outputQueueBlockSize > 0 );
        }

        DataBlockCache cache = null;

        m_lock.lock();
        try
        {
            cache = m_dataBlockCache.get( outputQueueBlockSize );
            if (cache == null)
            {
                cache = new DataBlockCache(
                                config.useDirectBuffers,
                                outputQueueBlockSize,
                                config.outputQueueCacheInitialSize,
                                config.outputQueueCacheMaxSize );
                m_dataBlockCache.put( outputQueueBlockSize, cache );
            }
        }
        finally
        {
            m_lock.unlock();
        }

        return cache;
    }

    private void removeEmitter( SessionEmitter sessionEmitter ) throws InterruptedException
    {
        SessionEmitterImpl emitterImpl;
        m_lock.lock();
        try
        {
            emitterImpl = m_emitters.get( sessionEmitter );
            if (emitterImpl == null)
                return;
            /*
             * m_emitters.remove( sessionEmitter );
             * It is better to keep the emitter in the container
             * allowing collider stop properly later.
             */
        }
        finally
        {
            m_lock.unlock();
        }
        emitterImpl.stopAndWait();
    }

    public void removeEmitterNoWait( SessionEmitter sessionEmitter )
    {
        /* Supposed to be called by emitter itself. */
        m_lock.lock();
        try
        {
            m_emitters.remove( sessionEmitter );
        }
        finally
        {
            m_lock.unlock();
        }
    }

    private final static int ST_RUNNING = 1;
    private final static int ST_STOPPING = 2;

    private static final Logger s_logger = Logger.getLogger( "org.jsl.collider.Collider" );

    private final Selector m_selector;
    private final ThreadPool m_threadPool;
    private final DummyRunnable m_dummyRunnable;
    private int m_state;

    private final ReentrantLock m_lock;
    private final Map<SessionEmitter, SessionEmitterImpl> m_emitters;
    private final Map<Integer, InputQueue.DataBlockCache> m_inputQueueDataBlockCache;
    private final Map<Integer, DataBlockCache> m_dataBlockCache;
    private boolean m_stop;

    private volatile SelectorThreadRunnable m_strHead;
    private final AtomicReference<SelectorThreadRunnable> m_strTail;
    private final AtomicReferenceArray<SelectorAlarm> m_alarmCache;

    public ColliderImpl( Config config ) throws IOException
    {
        super( config );

        m_selector = Selector.open();

        int threadPoolThreads = config.threadPoolThreads;
        if (threadPoolThreads == 0)
            threadPoolThreads = Runtime.getRuntime().availableProcessors();
        if (threadPoolThreads < 4)
            threadPoolThreads = 4;
        m_threadPool = new ThreadPool( "CTP", threadPoolThreads );

        if (config.inputQueueCacheMaxSize == 0)
            config.inputQueueCacheMaxSize = (threadPoolThreads * 3);

        if (config.outputQueueCacheMaxSize == 0)
            config.outputQueueCacheMaxSize = (threadPoolThreads * 3);

        m_dummyRunnable = new DummyRunnable();
        m_state = ST_RUNNING;

        m_lock = new ReentrantLock();
        m_emitters = new HashMap<SessionEmitter, SessionEmitterImpl>();
        m_inputQueueDataBlockCache = new HashMap<Integer, InputQueue.DataBlockCache>();
        m_dataBlockCache = new HashMap<Integer, DataBlockCache>();
        m_stop = false;

        m_strHead = null;
        m_strTail = new AtomicReference<SelectorThreadRunnable>();

        m_alarmCache = new AtomicReferenceArray<SelectorAlarm>(2);
        SelectorAlarm alarm = new SelectorAlarm();
        m_alarmCache.set( 0, alarm );
        for (int idx=threadPoolThreads; idx>0; idx--)
        {
            alarm.next = new SelectorAlarm();
            alarm = alarm.next;
        }
        m_alarmCache.set( 1, alarm );
    }

    public void run()
    {
        if (s_logger.isLoggable(Level.FINE))
            s_logger.fine( "starting." );

        m_threadPool.start();

        for (;;)
        {
            try
            {
                m_selector.select();
            }
            catch (IOException ex)
            {
                if (s_logger.isLoggable(Level.WARNING))
                    s_logger.warning( ex.toString() );
            }

            SelectorThreadRunnable strHead;

            assert( m_dummyRunnable.nextSelectorThreadRunnable == null );
            if (m_strTail.compareAndSet(null, m_dummyRunnable))
            {
                assert( m_strHead == null );
                strHead = m_dummyRunnable;
            }
            else
            {
                while (m_strHead == null);
                strHead = m_strHead;
                m_strHead = null;
            }

            Set<SelectionKey> selectedKeys = m_selector.selectedKeys();
            for (SelectionKey key : selectedKeys)
            {
                ChannelHandler channelHandler = (ChannelHandler) key.attachment();
                channelHandler.handleReadyOps( m_threadPool );
            }
            selectedKeys.clear();

            while (strHead != null)
            {
                strHead.runInSelectorThread();
                SelectorThreadRunnable next = strHead.nextSelectorThreadRunnable;
                if (next == null)
                {
                    if (m_strTail.compareAndSet(strHead, null))
                        break;
                    while (strHead.nextSelectorThreadRunnable == null);
                    next = strHead.nextSelectorThreadRunnable;
                }
                strHead.nextSelectorThreadRunnable = null;
                strHead = next;
            }

            if (m_state == ST_STOPPING)
            {
                if (m_selector.keys().size() == 0)
                    break;
                m_selector.wakeup();
            }
        }

        try
        {
            m_threadPool.stopAndWait();
        }
        catch (InterruptedException ex)
        {
            if (s_logger.isLoggable(Level.WARNING))
                s_logger.warning( ex.toString() );
        }

        for (Map.Entry<Integer, InputQueue.DataBlockCache> me : m_inputQueueDataBlockCache.entrySet())
        {
            me.getValue().clear( s_logger, getConfig().inputQueueCacheInitialSize );
            m_inputQueueDataBlockCache.remove( me.getKey() );
        }

        System.out.println( InputQueue.s_pc.getStats() );

        if (s_logger.isLoggable(Level.FINE))
            s_logger.fine( "stopped." );
    }

    public void stop()
    {
        if (s_logger.isLoggable(Level.FINE))
            s_logger.fine( "" );

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

    public final void executeInSelectorThread( SelectorThreadRunnable runnable )
    {
        assert( runnable.nextSelectorThreadRunnable == null );
        SelectorThreadRunnable tail = m_strTail.getAndSet( runnable );
        if (tail == null)
        {
            m_strHead = runnable;

            for (;;)
            {
                SelectorAlarm alarm = m_alarmCache.get(0);
                if (alarm == null)
                {
                    m_selector.wakeup();
                    if (s_logger.isLoggable(Level.WARNING))
                        s_logger.warning( "Selector alarm cache is empty." );
                    break;
                }

                SelectorAlarm next = alarm.next;
                if (m_alarmCache.compareAndSet(0, alarm, next))
                {
                    if (next == null)
                    {
                        if (!m_alarmCache.compareAndSet(1, alarm, null))
                        {
                            while (alarm.next == null);
                            m_alarmCache.set( 0, alarm.next );
                        }
                    }

                    alarm.next = null;
                    alarm.str = runnable;
                    m_threadPool.execute( alarm );
                    break;
                }
            }
        }
        else
            tail.nextSelectorThreadRunnable = runnable;
    }

    public final void executeInThreadPool( ThreadPool.Runnable runnable )
    {
        m_threadPool.execute( runnable );
    }

    public void addAcceptor( Acceptor acceptor )
    {
        ServerSocketChannel channel;
        try
        {
            channel = ServerSocketChannel.open();
            channel.configureBlocking( false );

            final ServerSocket socket = channel.socket();
            socket.setReuseAddress( acceptor.reuseAddr );
            socket.bind( acceptor.getAddr() );
        }
        catch (IOException ex)
        {
            acceptor.onException(ex);
            return;
        }

        InputQueue.DataBlockCache inputQueueDataBlockCache = getInputQueueDataBlockCache( acceptor );
        DataBlockCache outputQueueDataBlockCache = getOutputQueueDataBlockCache( acceptor );

        AcceptorImpl acceptorImpl = new AcceptorImpl(
                this,
                inputQueueDataBlockCache,
                outputQueueDataBlockCache,
                acceptor,
                m_selector,
                channel );

        IOException ex = null;

        m_lock.lock();
        try
        {
            if (m_stop)
                ex = new IOException( "Collider stopped." );
            else if (m_emitters.containsKey(acceptor))
                ex = new IOException( "Acceptor already registered." );
            else
                m_emitters.put( acceptor, acceptorImpl );
        }
        finally
        {
            m_lock.unlock();
        }

        if (ex == null)
            acceptorImpl.start();
        else
        {
            acceptor.onException(ex);
            try
            {
                channel.close();
            }
            catch (IOException ex1)
            {
                /* Should never happen */
                if (s_logger.isLoggable(Level.WARNING))
                    s_logger.warning( ex1.toString() );
            }
        }
    }

    public void removeAcceptor( Acceptor acceptor ) throws InterruptedException
    {
        removeEmitter( acceptor );
    }

    public void addConnector( Connector connector )
    {
        SocketChannel socketChannel;
        boolean connected;

        try
        {
            socketChannel = SocketChannel.open();
            socketChannel.configureBlocking( false );
            connected = socketChannel.connect( connector.getAddr() );
        }
        catch (IOException ex)
        {
            if (s_logger.isLoggable(Level.WARNING))
                s_logger.warning( ex.toString() );

            connector.onException( ex );
            return;
        }

        InputQueue.DataBlockCache inputQueueDataBlockCache = getInputQueueDataBlockCache( connector );
        DataBlockCache outputQueueDataBlockCache = getOutputQueueDataBlockCache( connector );

        ConnectorImpl connectorImpl = new ConnectorImpl(
                this,
                inputQueueDataBlockCache,
                outputQueueDataBlockCache,
                connector,
                m_selector );

        IOException ex = null;

        m_lock.lock();
        try
        {
            if (m_stop)
                ex = new IOException( "Collider stopped." );
            else if (m_emitters.containsKey(connector))
                ex = new IOException( "Connector already registered." );
            else
                m_emitters.put( connector, connectorImpl );
        }
        finally
        {
            m_lock.unlock();
        }

        if (ex == null)
            connectorImpl.start( socketChannel, connected );
        else
            connector.onException( ex );
    }

    public void removeConnector( Connector connector ) throws InterruptedException
    {
        removeEmitter( connector );
    }
}
