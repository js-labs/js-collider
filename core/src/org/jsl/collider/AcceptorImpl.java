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
import java.nio.channels.*;
import java.util.HashSet;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;


public class AcceptorImpl extends Collider.SelectorThreadRunnable
        implements Runnable, Collider.ChannelHandler
{
    private class SessionRegistrator extends Collider.SelectorThreadRunnable implements Runnable
    {
        private SocketChannel m_socketChannel;
        private SelectionKey m_selectionKey;

        public SessionRegistrator( SocketChannel socketChannel )
        {
            m_socketChannel = socketChannel;
        }

        public void runInSelectorThread()
        {
            try
            {
                m_selectionKey = m_socketChannel.register( m_selector, 0, null );
                m_collider.executeInThreadPool( this );
            }
            catch (IOException ex)
            {
                System.out.println( ex.toString() );
                m_lock.lock();
                if ((--m_state == 0) && (m_stop > 0))
                    m_cond.signalAll();
                m_lock.unlock();
            }
        }

        public void run()
        {
            SessionImpl sessionImpl = new SessionImpl( m_collider, m_socketChannel, m_selectionKey );
            Thread currentThread = Thread.currentThread();

            m_lock.lock();
            m_callbackThreads.add( currentThread );
            m_lock.unlock();

            Session.Listener sessionListener = m_acceptor.createSessionListener( sessionImpl );

            m_lock.lock();
            if (m_callbackThreads.remove(currentThread))
            {
                if ((--m_state == 0) && (m_stop > 0))
                    m_cond.signalAll();
            }
            /* else stop() called from the listener callback. */
            m_lock.unlock();

            if (sessionListener == null)
            {
            }
            else
            {
                sessionImpl.setListener( sessionListener );
            }
        }
    }

    private class ErrorNotifier implements Runnable
    {
        String m_errorText;

        public ErrorNotifier( String errorText )
        {
            m_errorText = errorText;
        }

        public void run()
        {
            Thread currentThread = Thread.currentThread();

            m_lock.lock();
            assert( m_callbackThreads.contains(currentThread) );
            m_callbackThreads.add( currentThread );
            m_lock.unlock();

            m_acceptor.onAcceptorStartingFailure( m_errorText );

            m_lock.lock();
            if (m_callbackThreads.remove(currentThread))
            {
                m_state = 0;
                if (m_stop > 0)
                    m_cond.signalAll();
            }
            /* else stop() was called from the listener callback. */
            m_lock.unlock();

            try { m_channel.close(); }
            catch (IOException ignored) { assert(false); }
            m_channel = null;
        }
    }

    private class ListenerNotifier extends Collider.SelectorThreadRunnable implements Runnable
    {
        public void run()
        {
            Thread currentThread = Thread.currentThread();

            m_lock.lock();
            m_callbackThreads.add( currentThread );
            m_lock.unlock();

            m_acceptor.onAcceptorStarted( m_channel.socket().getLocalPort() );

            m_lock.lock();
            m_callbackThreads.remove( currentThread );
            if (m_stop > 0)
            {
                m_state = 0;
                m_cond.signalAll();
                m_lock.unlock();
                m_collider.executeInSelectorThread( this );
            }
            else
            {
                m_state = 2;
                m_lock.unlock();
                m_collider.executeInSelectorThread( AcceptorImpl.this );
            }
        }

        public void runInSelectorThread()
        {
            m_selectionKey.cancel();
            m_selectionKey = null;

            try { m_channel.close(); }
            catch (IOException ignored) {}
            m_channel = null;
        }
    }

    private class Starter extends Collider.SelectorThreadRunnable
    {
        public void runInSelectorThread()
        {
            try
            {
                m_selectionKey = m_channel.register( m_selector, 0, AcceptorImpl.this );
                m_collider.executeInThreadPool( new ListenerNotifier() );
            }
            catch ( IOException ex )
            {
                m_collider.executeInThreadPool( new ErrorNotifier(ex.getMessage()) );
            }
        }
    }

    private class Stopper extends Collider.SelectorThreadRunnable
    {
        public void runInSelectorThread()
        {
            int interestOps = m_selectionKey.interestOps();
            if ((interestOps & SelectionKey.OP_ACCEPT) != 0)
            {
                m_lock.lock();
                m_state -= 2;
                if (m_state == 0)
                    m_cond.signalAll();
                m_lock.unlock();
            }

            m_selectionKey.cancel();
            m_selectionKey = null;

            try { m_channel.close(); }
            catch (IOException ignored) {}
            m_channel = null;
        }
    }

    private Collider m_collider;
    private Selector m_selector;
    private ServerSocketChannel m_channel;
    private SelectionKey m_selectionKey;
    private Acceptor m_acceptor;

    private ReentrantLock m_lock;
    private Condition m_cond;
    private int m_stop;
    private int m_state;
    private HashSet<Thread> m_callbackThreads;

    public AcceptorImpl(
            Collider collider,
            Selector selector,
            ServerSocketChannel channel,
            Acceptor acceptor )
    {
        m_collider = collider;
        m_selector = selector;
        m_channel = channel;
        m_acceptor = acceptor;

        m_lock = new ReentrantLock();
        m_cond = m_lock.newCondition();
        m_stop = 0;
        m_state = 1;
        m_callbackThreads = new HashSet<Thread>();
    }

    public void start()
    {
        m_collider.executeInSelectorThread( new Starter() );
    }

    public void stop()
    {
        Thread currentThread = Thread.currentThread();

        m_lock.lock();
        int stop = ++m_stop;
        if ((stop == 1) && (m_state > 1))
            m_collider.executeInSelectorThread( new Stopper() );

        if (m_callbackThreads.remove(currentThread))
            m_state--;

        while (m_state > 0)
        {
            try { m_cond.await(); }
            catch (InterruptedException ignored) {}
        }
        m_lock.unlock();
    }

    public void handleReadyOps( Executor executor )
    {
        m_selectionKey.interestOps( 0 );
        executor.execute( this );
    }

    public void runInSelectorThread()
    {
        assert( (m_selectionKey.interestOps() & SelectionKey.OP_ACCEPT) == 0 );
        m_selectionKey.interestOps( SelectionKey.OP_ACCEPT );
    }

    public void run()
    {
        for (;;)
        {
            SocketChannel socketChannel = null;

            try { socketChannel = m_channel.accept(); }
            catch (IOException ignored) {}

            m_lock.lock();
            if (socketChannel == null)
            {
                if (m_stop > 0)
                {
                    m_state -= 2;
                    if (m_state == 0)
                        m_cond.signalAll();
                    m_lock.unlock();
                }
                else
                {
                    m_lock.unlock();
                    m_collider.executeInSelectorThread( this );
                }
                break;
            }

            try { socketChannel.configureBlocking( false ); }
            catch (IOException ex)
            {
                /* Having unblocking mode is critical */
                try { socketChannel.close(); }
                catch (IOException ignored) {}
                System.out.println( ex.toString() );
                continue;
            }

            try
            {
                socketChannel.setOption( StandardSocketOptions.TCP_NODELAY, m_acceptor.getTcpNoDelay() );
            }
            catch (IOException ex)
            {
                /* Not a critical problem */
                System.out.print( ex.toString() );
            }

            m_state++;
            if (m_stop > 0)
            {
                m_state -= 2;
                m_lock.unlock();
                break;
            }
            m_lock.unlock();

            m_collider.executeInSelectorThread( new SessionRegistrator(socketChannel) );
        }
    }
}
