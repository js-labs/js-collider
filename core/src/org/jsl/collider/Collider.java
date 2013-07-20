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
        public int readBlockSize;
        public int shutdownTimeout;

        public Config()
        {
            threadPoolThreads = 4;
            readBlockSize     = (1024 * 32);
            shutdownTimeout   = 60;
        }
    }

    public interface ChannelHandler
    {
        abstract public void handleReadyOps( Executor executor );
    }

    public static abstract class SelectorThreadRunnable
    {
        public volatile SelectorThreadRunnable nextSelectorThreadRunnable;
        abstract public void runInSelectorThread();
    }

    /*
    private class AcceptorRegistrator extends SelectorThreadRunnable implements Runnable
    {
        private ServerSocketChannel m_channel;
        private Acceptor m_acceptor;
        private AcceptorImpl m_acceptorImpl;

        public AcceptorRegistrator( ServerSocketChannel channel, Acceptor acceptor )
        {
            m_channel = channel;
            m_acceptor = acceptor;
        }

        public void runInSelectorThread()
        {
            try
            {
                SelectionKey selectionKey = m_channel.register( m_selector, 0, null );
                m_acceptorImpl = new AcceptorImpl(
                        Collider.this, Collider.this.m_selector, m_channel, selectionKey, m_acceptor );
                selectionKey.attach( m_acceptorImpl );
                m_executor.execute( this );
            }
            catch (IOException ex)
            {
                m_acceptor.onAcceptorStartingFailure( ex.getMessage() );
            }
        }

        public void run()
        {
            int localPort = m_channel.socket().getLocalPort();
            if (localPort == -1)
            {
                m_acceptor.onAcceptorStartingFailure( "Not connected." );
            }
            else
            {
                m_acceptor.onAcceptorStarted( localPort );
                executeInSelectorThread( m_acceptorImpl );
            }
        }
    }
    */

    private Config m_config;
    private Selector m_selector;
    private ExecutorService m_executor;
    private volatile boolean m_run;
    private final Map<Acceptor, AcceptorImpl> m_acceptors;
    private volatile SelectorThreadRunnable m_strHead;
    private AtomicReference<SelectorThreadRunnable> m_strTail;

    public Collider() throws IOException
    {
        m_config = new Config();
        m_selector = Selector.open();
        m_executor = Executors.newFixedThreadPool(m_config.threadPoolThreads);
        m_run = true;
        m_acceptors = Collections.synchronizedMap( new HashMap<Acceptor, AcceptorImpl>() );
        m_strHead = null;
        m_strTail = new AtomicReference<SelectorThreadRunnable>();
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
            channel.setOption( StandardSocketOptions.SO_REUSEADDR, acceptor.getReuseAddr() );
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
