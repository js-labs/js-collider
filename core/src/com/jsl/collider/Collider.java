/*
 * JS-Collider framework.
 * Copyright (C) 2013 Sergey Zubarev
 * info@js-labs.com
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.jsl.collider;

import java.io.IOException;
import java.net.SocketAddress;
import java.net.StandardSocketOptions;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.util.Set;
import java.util.Iterator;
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
        public int readBlockSize;
        public int shutdownTimeout;

        public Config()
        {
            threadPoolThreads = 4;
            readBlockSize     = (1024 * 32);
            shutdownTimeout   = 60;
        }
    }

    public static interface ChannelHandler
    {
        abstract public void handleReadyOps( Executor executor );
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

    private volatile SelectorThreadRunnable m_strHead;
    private AtomicReference<SelectorThreadRunnable> m_strTail;

    public Collider( Config config ) throws IOException
    {
        m_config = config;
        m_selector = Selector.open();
        m_executor = Executors.newFixedThreadPool(m_config.threadPoolThreads);
        m_run = true;
        m_strTail = new AtomicReference<SelectorThreadRunnable>();
    }

    public Collider() throws IOException
    {
        this( new Config() );
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
        runnable.nextSelectorThreadRunnable = null;
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


    public Acceptor addAcceptor( SocketAddress addr, Acceptor.HandlerFactory handlerFactory )
    {
        try
        {
            ServerSocketChannel channel = ServerSocketChannel.open();
            channel.configureBlocking( false );
            channel.setOption( StandardSocketOptions.SO_REUSEADDR, handlerFactory.reuseAddr );
            channel.socket().bind( addr );
            return new AcceptorImpl( this, m_selector, channel, handlerFactory );
        }
        catch (IOException ignored)
        {
            return null;
        }
    }
}
