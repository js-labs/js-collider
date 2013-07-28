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
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;


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
        public int outputQueueBlockSize;

        public Config()
        {
            threadPoolThreads = 4;
            useDirectBuffers  = true;
            shutdownTimeout   = 60;

            /* Use system default settings by default */
            socketSendBufSize = 0;
            socketRecvBufSize = 0;

            inputQueueBlockSize   = (32 * 1024);
            outputQueueBlockSize  = (16 * 1024);
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

    private Config m_config;
    private Selector m_selector;
    private ExecutorService m_executor;
    private volatile boolean m_run;
    private Map<Acceptor, AcceptorImpl> m_acceptors;
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
        m_executor = Executors.newFixedThreadPool( m_config.threadPoolThreads );
        m_run = true;
        m_acceptors = Collections.synchronizedMap( new HashMap<Acceptor, AcceptorImpl>() );
        m_strHead = null;
        m_strTail = new AtomicReference<SelectorThreadRunnable>();
    }

    public Config getConfig()
    {
        return m_config;
    }

    public void run() throws IOException
    {
        while (m_run)
        {
            m_selector.select();

            Set<SelectionKey> selectedKeys = m_selector.selectedKeys();
            Iterator<SelectionKey> keyIterator = selectedKeys.iterator();
            while (keyIterator.hasNext())
            {
                SelectionKey key = keyIterator.next();
                ChannelHandler channelHandler = (ChannelHandler) key.attachment();
                channelHandler.handleReadyOps( m_executor );
                keyIterator.remove();
            }

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
        }

        m_executor.shutdown();
        try
        {
            if (!m_executor.awaitTermination(m_config.shutdownTimeout, TimeUnit.SECONDS))
                m_executor.shutdownNow();
            m_executor.awaitTermination( m_config.shutdownTimeout, TimeUnit.SECONDS );
        }
        catch (InterruptedException ignored) {}
    }

    public void stop()
    {
        m_run = false;
        m_selector.wakeup();
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

    public void addAcceptor( Acceptor acceptor )
    {
        synchronized (m_acceptors)
        {
            if (m_acceptors.containsKey(acceptor))
            {
                acceptor.onAcceptorStartingFailure( "Acceptor already registered." );
                return;
            }
            m_acceptors.put( acceptor, null );
        }

        try
        {
            ServerSocketChannel channel = ServerSocketChannel.open();
            channel.configureBlocking( false );
            channel.setOption( StandardSocketOptions.SO_REUSEADDR, acceptor.reuseAddr );
            channel.socket().bind( acceptor.getAddr() );

            AcceptorImpl acceptorImpl = new AcceptorImpl( this, m_selector, channel, acceptor );
            synchronized (m_acceptors) { m_acceptors.put( acceptor, acceptorImpl ); }
            acceptorImpl.start();
        }
        catch (IOException e)
        {
            synchronized (m_acceptors) { m_acceptors.remove( acceptor ); }
            acceptor.onAcceptorStartingFailure( e.getMessage() );
        }
    }

    public void removeAcceptor( Acceptor acceptor )
    {
        AcceptorImpl acceptorImpl;
        synchronized (m_acceptors)
        {
            if (!m_acceptors.containsKey(acceptor))
                return;
            acceptorImpl = m_acceptors.get( acceptor );
        }
        assert( acceptorImpl != null );
        acceptorImpl.stop();
    }
}
