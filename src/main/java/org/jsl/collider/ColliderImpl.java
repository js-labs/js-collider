/*
 * Copyright (C) 2013 Sergey Zubarev, info@js-labs.org
 *
 * This file is a part of JS-Collider framework.
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
import java.net.NetworkInterface;
import java.net.InetSocketAddress;
import java.net.DatagramSocket;
import java.net.StandardSocketOptions;
import java.net.StandardProtocolFamily;
import java.nio.ByteOrder;
import java.nio.channels.Selector;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.DatagramChannel;
import java.nio.channels.MembershipKey;
import java.util.Set;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

class ColliderImpl extends Collider
{
    public interface ChannelHandler
    {
        int handleReadyOps( ThreadPool threadPool );
    }

    public static abstract class SelectorThreadRunnable
    {
        public volatile SelectorThreadRunnable nextSelectorThreadRunnable;
        abstract public int runInSelectorThread();
    }

    private class Stopper1 extends ThreadPool.Runnable
    {
        public void runInThreadPool()
        {
            SessionEmitter [] emitters = null;
            DatagramListener [] datagramListeners = null;

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

                size = m_datagramListeners.size();
                if (size > 0)
                {
                    datagramListeners = new DatagramListener[size];
                    Iterator<DatagramListener> it = m_datagramListeners.keySet().iterator();
                    for (int idx=0; idx<size; idx++)
                        datagramListeners[idx] = it.next();
                }
            }
            finally
            {
                m_lock.unlock();
            }

            boolean interrupted = false;
            try
            {
                if (emitters != null)
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

                if (datagramListeners != null)
                {
                    for (DatagramListener datagramListener : datagramListeners)
                    {
                        try
                        {
                            removeDatagramListener( datagramListener );
                        }
                        catch (InterruptedException ex)
                        {
                            if (s_logger.isLoggable(Level.WARNING))
                                s_logger.warning( ex.toString() );
                            interrupted = true;
                        }
                    }
                }
            }
            finally
            {
                if (interrupted)
                    Thread.currentThread().interrupt();
            }

            /* No new session can appear now, can close all current. */
            executeInSelectorThread( new Stopper2() );
        }
    }

    private class Stopper2 extends SelectorThreadRunnable
    {
        public int runInSelectorThread()
        {
            final Set<SelectionKey> keys = m_selector.keys();
            for (SelectionKey key : keys)
            {
                final Object attachment = key.attachment();
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
            m_run = false;
            return 0;
        }
    }

    private static class DummyRunnable extends SelectorThreadRunnable
    {
        public int runInSelectorThread()
        {
            return 0;
        }
    }

    private class SelectorAlarm extends ThreadPool.Runnable
    {
        public SelectorThreadRunnable cmp;

        public SelectorAlarm( SelectorThreadRunnable runnable )
        {
            cmp = runnable;
        }

        public void runInThreadPool()
        {
            if (m_strHead == cmp)
                m_selector.wakeup();
            cmp = null;
            m_alarm.compareAndSet( null, this );
        }
    }

    private static class SessionSharedData
    {
        private final RetainableDataBlockCache m_inputQueueDataBlockCache;
        private final int m_joinMessageMaxSize;
        private final RetainableByteBufferPool m_joinPool;

        public SessionSharedData(
                RetainableDataBlockCache inputQueueDataBlockCache,
                int joinMessageMaxSize,
                RetainableByteBufferPool joinPool )
        {
            m_inputQueueDataBlockCache = inputQueueDataBlockCache;
            m_joinMessageMaxSize = joinMessageMaxSize;
            m_joinPool = joinPool;
        }

        RetainableDataBlockCache getInputQueueDataBlockCache() { return m_inputQueueDataBlockCache; }
        int getJoinMessageMaxSize() { return m_joinMessageMaxSize; }
        RetainableByteBufferPool getJoinPool() { return m_joinPool; }
    }

    private SessionSharedData getSessionSharedData( final SessionEmitter sessionEmitter )
    {
        final Config config = getConfig();

        int inputQueueBlockSize = sessionEmitter.inputQueueBlockSize;
        if (inputQueueBlockSize == 0)
        {
            inputQueueBlockSize = config.inputQueueBlockSize;
            assert( inputQueueBlockSize > 0 );
        }

        int joinMessageMaxSize = sessionEmitter.joinMessageMaxSize;
        if (joinMessageMaxSize < 0)
        {
            joinMessageMaxSize = config.joinMessageMaxSize;
            if (joinMessageMaxSize < 0)
                joinMessageMaxSize = 0;
        }

        m_lock.lock();
        try
        {
            RetainableDataBlockCache cache = m_dataBlockCache.get( inputQueueBlockSize );
            if (cache == null)
            {
                cache = new RetainableDataBlockCache(
                            config.useDirectBuffers,
                            inputQueueBlockSize,
                            8 /* initial size */,
                            config.inputQueueCacheMaxSize );
                m_dataBlockCache.put( inputQueueBlockSize, cache );
            }

            RetainableByteBufferPool joinPool = null;
            if (joinMessageMaxSize > 0)
            {
                if (m_joinPool == null)
                {
                    /* Join pool chunk size should be related to the socket
                     * send buffer size, the question is how...
                     */
                    int socketSendBufferSize = sessionEmitter.socketSendBufSize;
                    if (socketSendBufferSize == 0)
                    {
                        socketSendBufferSize = config.socketSendBufSize;
                        if (socketSendBufferSize == 0)
                            socketSendBufferSize = (64 * 1024);
                    }

                    boolean useDirectBuffers;
                    if (sessionEmitter.useDirectBuffers > 0)
                        useDirectBuffers = true;
                    else if (sessionEmitter.useDirectBuffers == 0)
                        useDirectBuffers = false;
                    else
                        useDirectBuffers = getConfig().useDirectBuffers;

                    final int joinPoolChunkSize = socketSendBufferSize * 2;

                    /* For the join pool byte order does not matter */
                    m_joinPool = new RetainableByteBufferPool( joinPoolChunkSize, useDirectBuffers, ByteOrder.nativeOrder() );
                }
                joinPool = m_joinPool;
            }

            return new SessionSharedData( cache, joinMessageMaxSize, joinPool );
        }
        finally
        {
            m_lock.unlock();
        }
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

    public void removeEmitterNoWait( final SessionEmitter sessionEmitter )
    {
        /* Supposed to be called by SessionEmitterImpl itself. */
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

    public void removeDatagramListenerNoWait( final DatagramListener datagramListener )
    {
        /* Supposed to be called by DatagramListenerImpl */
        m_lock.lock();
        try
        {
            m_datagramListeners.remove( datagramListener );
        }
        finally
        {
            m_lock.unlock();
        }
    }

    private static final Logger s_logger = Logger.getLogger( Collider.class.getName() );

    private static final AtomicReferenceFieldUpdater<ColliderImpl, SelectorThreadRunnable> s_strHeadUpdater =
            AtomicReferenceFieldUpdater.newUpdater( ColliderImpl.class, SelectorThreadRunnable.class, "m_strHead" );

    private static final AtomicReferenceFieldUpdater<ColliderImpl, SelectorThreadRunnable> s_strTailUpdater =
            AtomicReferenceFieldUpdater.newUpdater( ColliderImpl.class, SelectorThreadRunnable.class, "m_strTail" );

    private static final AtomicReferenceFieldUpdater<SelectorThreadRunnable, SelectorThreadRunnable> s_nextSelectorThreadRunnableUpdater =
            AtomicReferenceFieldUpdater.newUpdater( SelectorThreadRunnable.class, SelectorThreadRunnable.class, "nextSelectorThreadRunnable" );

    private final Selector m_selector;
    private final ThreadPool m_threadPool;
    private boolean m_run;

    private final ReentrantLock m_lock;
    private final Map<SessionEmitter, SessionEmitterImpl> m_emitters;
    private final Map<DatagramListener, DatagramListenerImpl> m_datagramListeners;
    private final Map<Integer, RetainableDataBlockCache> m_dataBlockCache;
    private RetainableByteBufferPool m_joinPool;
    private boolean m_stop;

    private volatile SelectorThreadRunnable m_strHead;
    private volatile SelectorThreadRunnable m_strTail;
    private SelectorThreadRunnable m_strLater;
    private final AtomicReference<SelectorAlarm> m_alarm;

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

        m_run = true;

        m_lock = new ReentrantLock();
        m_emitters = new HashMap<SessionEmitter, SessionEmitterImpl>();
        m_datagramListeners = new HashMap<DatagramListener, DatagramListenerImpl>();
        m_dataBlockCache = new HashMap<Integer, RetainableDataBlockCache>();
        m_stop = false;

        m_alarm = new AtomicReference<SelectorAlarm>( new SelectorAlarm(null) );
    }

    public void run()
    {
        if (s_logger.isLoggable(Level.FINE))
            s_logger.fine( "start" );

        m_threadPool.start();

        final DummyRunnable dummyRunnable = new DummyRunnable();
        int statLoopIt = 0;
        int statLoopReadersG0 = 0;
        int readers = 0;

        try
        {
            for (;;)
            {
                statLoopIt++;
                if (m_run)
                {
                    if (readers > 0)
                    {
                        statLoopReadersG0++;
                        m_selector.selectNow();
                    }
                    else
                        m_selector.select();
                }
                else
                {
                    m_selector.selectNow();
                    if (m_selector.keys().size() == 0)
                    {
                        assert( readers == 0 );
                        break;
                    }
                }

                if (s_strTailUpdater.compareAndSet(this, null, dummyRunnable))
                    s_strHeadUpdater.lazySet( this, dummyRunnable );

                final Set<SelectionKey> selectedKeys = m_selector.selectedKeys();
                for (SelectionKey key : selectedKeys)
                {
                    final ChannelHandler channelHandler = (ChannelHandler) key.attachment();
                    readers += channelHandler.handleReadyOps( m_threadPool );
                }
                selectedKeys.clear();

                SelectorThreadRunnable runnable;
                while ((runnable = m_strHead) == null);

                for (;;)
                {
                    SelectorThreadRunnable next = runnable.nextSelectorThreadRunnable;
                    if (next == null)
                    {
                        m_strHead = null;
                        if (!s_strTailUpdater.compareAndSet(this, runnable, null))
                        {
                            while ((next = runnable.nextSelectorThreadRunnable) == null);
                            s_nextSelectorThreadRunnableUpdater.lazySet( runnable, null );
                        }
                    }
                    else
                        s_nextSelectorThreadRunnableUpdater.lazySet( runnable, null );

                    readers -= runnable.runInSelectorThread();
                    assert( readers >= 0 );

                    runnable = next;
                    if (runnable == null)
                    {
                        runnable = m_strHead;
                        if (runnable == null)
                            break;
                    }
                }

                SelectorThreadRunnable strLater = m_strLater;
                m_strLater = null;
                while (strLater != null)
                {
                    runnable = strLater;
                    strLater = runnable.nextSelectorThreadRunnable;
                    runnable.nextSelectorThreadRunnable = null;
                    final int rc = runnable.runInSelectorThread();
                    assert( rc == 0 );
                }

                /* End of select loop */
            }

            m_threadPool.stopAndWait();
        }
        catch (final IOException ex)
        {
            if (s_logger.isLoggable(Level.WARNING))
                s_logger.warning( ex.toString() );
        }
        catch (final InterruptedException ex)
        {
            if (s_logger.isLoggable(Level.WARNING))
                s_logger.warning( ex.toString() );
        }

        for (Map.Entry<Integer, RetainableDataBlockCache> me : m_dataBlockCache.entrySet())
            me.getValue().clear( s_logger );
        m_dataBlockCache.clear();

        if (s_logger.isLoggable(Level.FINE))
            s_logger.fine( "finish (" + statLoopIt + ", " + statLoopReadersG0 + ")." );
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
        final SelectorThreadRunnable tail = s_strTailUpdater.getAndSet( this, runnable );
        if (tail == null)
        {
            m_strHead = runnable;

            for (;;)
            {
                SelectorAlarm alarm = m_alarm.get();
                if (alarm == null)
                {
                    m_threadPool.execute( new SelectorAlarm(runnable) );
                    break;
                }
                else if (m_alarm.compareAndSet(alarm, null))
                {
                    alarm.cmp = runnable;
                    m_threadPool.execute( alarm );
                    break;
                }
            }
        }
        else
            tail.nextSelectorThreadRunnable = runnable;
    }

    public final void executeInSelectorThreadNoWakeup( SelectorThreadRunnable runnable )
    {
        assert( runnable.nextSelectorThreadRunnable == null );
        final SelectorThreadRunnable tail = s_strTailUpdater.getAndSet( this, runnable );
        if (tail == null)
            m_strHead = runnable;
        else
            tail.nextSelectorThreadRunnable = runnable;
    }

    public final void executeInSelectorThreadLater( SelectorThreadRunnable runnable )
    {
        assert( runnable.nextSelectorThreadRunnable == null );
        runnable.nextSelectorThreadRunnable = m_strLater;
        m_strLater = runnable;
    }

    public final void executeInThreadPool( ThreadPool.Runnable runnable )
    {
        m_threadPool.execute( runnable );
    }

    public void addAcceptor( Acceptor acceptor ) throws IOException
    {
        final ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.configureBlocking( false );

        final ServerSocket socket = serverSocketChannel.socket();
        socket.setReuseAddress( acceptor.reuseAddr );
        socket.bind( acceptor.getAddr() );

        SessionSharedData sessionSharedData = getSessionSharedData( acceptor );

        AcceptorImpl acceptorImpl = new AcceptorImpl(
                this,
                sessionSharedData.getInputQueueDataBlockCache(),
                acceptor,
                sessionSharedData.getJoinMessageMaxSize(),
                sessionSharedData.getJoinPool(),
                m_selector,
                serverSocketChannel );

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
            try
            {
                serverSocketChannel.close();
            }
            catch (final IOException ex1)
            {
                /* Should never happen */
                if (s_logger.isLoggable(Level.WARNING))
                    s_logger.warning( ex1.toString() );
            }
            throw ex;
        }
    }

    public void removeAcceptor( Acceptor acceptor ) throws InterruptedException
    {
        removeEmitter( acceptor );
    }

    public void addConnector( Connector connector )
    {
        SessionSharedData sessionSharedData = getSessionSharedData( connector );

        ConnectorImpl connectorImpl = new ConnectorImpl(
                this,
                sessionSharedData.getInputQueueDataBlockCache(),
                connector,
                sessionSharedData.getJoinMessageMaxSize(),
                sessionSharedData.getJoinPool(),
                m_selector );

        m_lock.lock();
        try
        {
            if (m_stop)
                throw new RuntimeException( "Collider is stopped." );
            else if (m_emitters.containsKey(connector))
                throw new RuntimeException( "Connector already registered." );
            else
                m_emitters.put( connector, connectorImpl );
        }
        finally
        {
            m_lock.unlock();
        }

        connectorImpl.start();
    }

    public void removeConnector( Connector connector ) throws InterruptedException
    {
        removeEmitter( connector );
    }

    public void addDatagramListener( DatagramListener datagramListener ) throws IOException
    {
        addDatagramListener( datagramListener, null );
    }

    public void addDatagramListener(
            DatagramListener datagramListener, NetworkInterface networkInterface ) throws IOException
    {
        final InetSocketAddress addr = datagramListener.getAddr();
        final DatagramChannel datagramChannel = DatagramChannel.open( StandardProtocolFamily.INET );
        final DatagramSocket socket = datagramChannel.socket();
        final Config config = getConfig();

        datagramChannel.configureBlocking( false );
        socket.setReuseAddress( true );

        int socketRecvBufSize = datagramListener.socketRecvBufSize;
        if (socketRecvBufSize == 0)
            socketRecvBufSize = config.socketRecvBufSize;
        if (socketRecvBufSize > 0)
            socket.setReceiveBufferSize( socketRecvBufSize );

        MembershipKey membershipKey = null;

        if (networkInterface == null)
        {
            if (addr.getAddress().isMulticastAddress())
            {
                datagramChannel.close();
                throw new IOException( "addDatagramListener(" + addr + "): " +
                                       "addDatagramListener(DatagramListener, NetworkInterface) " +
                                       "should be used for multicast addresses." );
            }
            datagramChannel.bind( addr );
            datagramChannel.connect( addr );
        }
        else
        {
            datagramChannel.bind( new InetSocketAddress( addr.getPort() ) );
            datagramChannel.setOption( StandardSocketOptions.IP_MULTICAST_IF, networkInterface );
            membershipKey = datagramChannel.join( addr.getAddress(), networkInterface );
        }

        int inputQueueBlockSize = datagramListener.inputQueueBlockSize;
        if (inputQueueBlockSize == 0)
            inputQueueBlockSize = config.inputQueueBlockSize;

        if (inputQueueBlockSize < 2*1024)
            inputQueueBlockSize = (2 * 1024);

        RetainableDataBlockCache dataBlockCache;
        m_lock.lock();
        try
        {
            dataBlockCache = m_dataBlockCache.get( inputQueueBlockSize );
            if (dataBlockCache == null)
            {
                dataBlockCache = new RetainableDataBlockCache(
                        config.useDirectBuffers,
                        inputQueueBlockSize,
                        4 /* initial size */,
                        config.inputQueueCacheMaxSize );
                m_dataBlockCache.put( inputQueueBlockSize, dataBlockCache );
            }
        }
        finally
        {
            m_lock.unlock();
        }

        DatagramListenerImpl datagramListenerImpl = new DatagramListenerImpl(
                this, m_selector, dataBlockCache, datagramListener, datagramChannel, membershipKey );

        IOException ex = null;

        m_lock.lock();
        try
        {
            if (m_stop)
                ex = new IOException( "Collider stopped" );
            else if (m_datagramListeners.containsKey(datagramListener))
                ex = new IOException( "DatagramListener already registered." );
            else
                m_datagramListeners.put( datagramListener, datagramListenerImpl );
        }
        finally
        {
            m_lock.unlock();
        }

        if (ex == null)
            datagramListenerImpl.start();
        else
        {
            if (membershipKey != null)
                membershipKey.drop();

            try
            {
                datagramChannel.close();
            }
            catch (final IOException ex1)
            {
                /* Should never happen */
                if (s_logger.isLoggable(Level.WARNING))
                    s_logger.warning( ex1.toString() );
            }
            throw ex;
        }
    }

    public void removeDatagramListener( DatagramListener datagramListener) throws InterruptedException
    {
        DatagramListenerImpl datagramListenerImpl;
        m_lock.lock();
        try
        {
            datagramListenerImpl = m_datagramListeners.get( datagramListener );
            if (datagramListenerImpl == null)
                return;
        }
        finally
        {
            m_lock.unlock();
        }
        datagramListenerImpl.stopAndWait();
    }

    public ThreadPool getThreadPool()
    {
        return m_threadPool;
    }
}
