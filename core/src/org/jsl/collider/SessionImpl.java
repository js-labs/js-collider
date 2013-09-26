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
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;


public class SessionImpl extends ThreadPool.Runnable
        implements Session, ColliderImpl.ChannelHandler
{
    private static final long LENGTH_MASK   = 0x000000FFFFFFFFFFL;
    private static final long CLOSE         = 0x0000010000000000L;
    private static final long WAIT_WRITE    = 0x0000020000000000L;
    private static final long STATE_MASK    = 0x0000300000000000L;
    private static final long ST_STARTING   = 0x0000000000000000L;
    private static final long ST_RUNNING    = 0x0000100000000000L;
    private static final long SOCK_RC_MASK  = 0x0003000000000000L;
    private static final long SOCK_RC       = 0x0001000000000000L;

    private static final Logger s_logger = Logger.getLogger( SessionImpl.class.getName() );

    private final ColliderImpl m_collider;
    private SocketChannel m_socketChannel;
    private SelectionKey m_selectionKey;
    private final SocketAddress m_localSocketAddress;
    private final SocketAddress m_remoteSocketAddress;

    private final Starter m_starter;
    private final AtomicLong m_state;
    private final OutputQueue m_outputQueue;
    private final ByteBuffer [] m_iov;
    private InputQueue m_inputQueue;

    private static class DummyListener implements Listener
    {
        public void onDataReceived(ByteBuffer data) { }
        public void onConnectionClosed() { }
    }

    private class SelectorDeregistrator extends ColliderImpl.SelectorThreadRunnable
    {
        public void runInSelectorThread()
        {
            if (s_logger.isLoggable(Level.FINE))
                s_logger.fine( m_remoteSocketAddress.toString() );

            m_selectionKey.cancel();
            m_selectionKey = null;

            try
            {
                m_socketChannel.close();
            }
            catch (IOException ex)
            {
                if (s_logger.isLoggable(Level.WARNING))
                    s_logger.warning( m_remoteSocketAddress.toString() + ": " + ex.toString() );
            }
            m_socketChannel = null;
        }
    }

    private class Starter extends ColliderImpl.SelectorThreadRunnable
    {
        public void runInSelectorThread()
        {
            int interestOps = m_selectionKey.interestOps();
            m_selectionKey.interestOps( interestOps | SelectionKey.OP_WRITE );
        }
    }

    private static String stateToString( long state )
    {
        String ret = "[";
        if ((state & CLOSE) != 0)
            ret += "CLOSE ";

        if ((state & WAIT_WRITE) != 0)
            ret += "WAIT_WRITE ";

        long length = (state & LENGTH_MASK);
        long sockRC = (state & SOCK_RC_MASK);

        state &= STATE_MASK;
        if (state == ST_STARTING)
            ret += "STARTING ";
        else if (state == ST_RUNNING)
            ret += "STARTED ";
        else
            ret += "??? ";

        sockRC /= SOCK_RC;
        ret += "RC=" + sockRC + " ";

        ret += length;
        ret += "]";
        return ret;
    }

    public final void handleReaderStopped()
    {
        long state = m_state.get();
        long newState;
        for (;;)
        {
            assert( (state & STATE_MASK) == ST_RUNNING );
            assert( (state & SOCK_RC_MASK) > 0 );

            newState = state;
            newState |= CLOSE;
            newState -= SOCK_RC;

            if ((state & LENGTH_MASK) == 0)
                newState -= SOCK_RC;

            if (m_state.compareAndSet(state, newState))
                break;

            state = m_state.get();
        }

        if (s_logger.isLoggable(Level.FINER))
        {
            s_logger.finer(
                    m_remoteSocketAddress.toString() +
                    ": " + stateToString(state) + " -> " + stateToString(newState) );
        }

        if ((newState & SOCK_RC_MASK) == 0)
            m_collider.executeInSelectorThread( new SelectorDeregistrator() );
    }

    public SessionImpl(
                ColliderImpl collider,
                SessionEmitter sessionEmitter,
                SocketChannel socketChannel,
                SelectionKey selectionKey,
                OutputQueue.DataBlockCache outputQueueDataBlockCache )
    {
        Collider.Config colliderConfig = collider.getConfig();

        int sendBufSize = sessionEmitter.socketSendBufSize;
        if (sendBufSize == 0)
            sendBufSize = colliderConfig.socketSendBufSize;
        if (sendBufSize == 0)
            sendBufSize = (64 * 1024);
        int sendIovMax = (sendBufSize / outputQueueDataBlockCache.getBlockSize()) + 1;

        m_collider = collider;
        m_socketChannel = socketChannel;
        m_selectionKey = selectionKey;
        m_localSocketAddress = socketChannel.socket().getLocalSocketAddress();
        m_remoteSocketAddress = socketChannel.socket().getRemoteSocketAddress();

        m_starter = new Starter();
        m_state = new AtomicLong( ST_STARTING + SOCK_RC + SOCK_RC );
        m_outputQueue = new OutputQueue( outputQueueDataBlockCache );
        m_iov = new ByteBuffer[sendIovMax];
    }

    public Collider getCollider() { return m_collider; }
    public SocketAddress getLocalAddress() { return m_localSocketAddress; }
    public SocketAddress getRemoteAddress() { return m_remoteSocketAddress; }

    public boolean sendData( ByteBuffer data )
    {
        long state = m_state.get();
        if ((state & CLOSE) != 0)
            return false;

        long bytesReady = m_outputQueue.addData( data );
        if (bytesReady > 0)
        {
            for (;;)
            {
                long newState = (state & LENGTH_MASK);
                newState += bytesReady;

                if (bytesReady > LENGTH_MASK)
                {
                    state = m_state.get();
                    continue;
                }

                newState |= (state & ~LENGTH_MASK);
                if (m_state.compareAndSet(state, newState))
                {
                    state = newState;
                    break;
                }

                state = m_state.get();
                if ((state & CLOSE) != 0)
                    return false;
            }

            if ((state & LENGTH_MASK) == bytesReady)
                m_collider.executeInThreadPool( this );
        }

        return true;
    }

    public boolean sendDataSync( ByteBuffer data )
    {
        try { m_socketChannel.write( data ); }
        catch (IOException ignored) {}
        return false;
    }

    public boolean closeConnection()
    {
        long state = m_state.get();
        long newState;
        for (;;)
        {
            if ((state & CLOSE) != 0)
                return false;

            newState = (state | CLOSE);
            if (m_state.compareAndSet(state, newState))
                break;

            state = m_state.get();
        }

        if (s_logger.isLoggable(Level.FINER))
        {
            s_logger.finer(
                    m_remoteSocketAddress.toString() +
                    ": " + stateToString(state) + " -> " + stateToString(newState) );
        }

        if ((state & STATE_MASK) == ST_RUNNING)
            m_inputQueue.stop();

        return true;
    }

    public final void initialize( InputQueue.DataBlockCache inputQueueDataBlockCache, Listener listener )
    {
        m_selectionKey.attach( this );

        if (listener == null)
            listener = new DummyListener();

        m_inputQueue = new InputQueue( m_collider,
                                       inputQueueDataBlockCache,
                                       this,
                                       m_socketChannel,
                                       m_selectionKey,
                                       listener );

        long state = m_state.get();
        long newState;
        for (;;)
        {
            assert( (state & STATE_MASK) == ST_STARTING );
            assert( (state & SOCK_RC_MASK) == (SOCK_RC+SOCK_RC) );

            newState = state;

            if ((state & CLOSE) == 0)
            {
                newState &= ~STATE_MASK;
                newState |= ST_RUNNING;
                if (m_state.compareAndSet(state, newState))
                {
                    m_inputQueue.start();
                    break;
                }
            }
            else
            {
                newState -= SOCK_RC;
                if ((state & LENGTH_MASK) == 0)
                    newState -= SOCK_RC;

                if (m_state.compareAndSet(state, newState))
                {
                    listener.onConnectionClosed();
                    if ((newState & SOCK_RC_MASK) == 0)
                        m_collider.executeInSelectorThread( new SelectorDeregistrator() );
                    break;
                }
            }
            state = m_state.get();
        }

        if (s_logger.isLoggable(Level.FINE))
        {
            s_logger.fine(
                    m_remoteSocketAddress.toString() +
                    ": " + stateToString(state) + " -> " + stateToString(newState) + "." );
        }

        if ((newState & WAIT_WRITE) != 0)
            m_collider.executeInSelectorThread( m_starter );
    }

    public void handleReadyOps( ThreadPool threadPool )
    {
        int readyOps = m_selectionKey.readyOps();
        m_selectionKey.interestOps( m_selectionKey.interestOps() & ~readyOps );

        if ((readyOps & SelectionKey.OP_READ) != 0)
            threadPool.execute( m_inputQueue );

        if ((readyOps & SelectionKey.OP_WRITE) != 0)
            threadPool.execute( this );
    }

    public void runInThreadPool()
    {
        long state = m_state.get();
        long bytesReady = (state & LENGTH_MASK);
        assert( bytesReady > 0 );

        bytesReady = m_outputQueue.getData( m_iov, bytesReady );
        int pos0 = m_iov[0].position();

        int iovc = 0;
        for (; iovc<m_iov.length && m_iov[iovc]!= null; iovc++);

        try
        {
            final long bytesSent = m_socketChannel.write( m_iov, 0, iovc );

            for (int idx=0; idx<iovc; idx++)
                m_iov[idx] = null;

            if (bytesSent == 0)
                System.out.println( "strange" );

            m_outputQueue.removeData( pos0, bytesSent );

            long newState;
            for (;;)
            {
                assert( (state & LENGTH_MASK) >= bytesSent );
                assert( (state & SOCK_RC_MASK) > 0 );

                newState = (state - bytesSent);
                if ((newState & LENGTH_MASK) == 0)
                {
                    if ((state & CLOSE) == 0)
                    {
                        if (m_state.compareAndSet(state, newState))
                            break;
                    }
                    else
                    {
                        newState -= SOCK_RC;
                        if (m_state.compareAndSet(state, newState))
                        {
                            if ((newState & SOCK_RC_MASK) == 0)
                                m_collider.executeInSelectorThread( new SelectorDeregistrator() );
                            break;
                        }
                    }
                }
                else if ((state & STATE_MASK) != ST_STARTING)
                {
                    if (m_state.compareAndSet(state, newState))
                    {
                        m_collider.executeInThreadPool( this );
                        break;
                    }
                }
                else /* ((state  & LENGTH_MASK) > 0) && ((state & STATE_MASK) == ST_STARTING) */
                {
                    newState |= WAIT_WRITE;
                    if (m_state.compareAndSet(state, newState))
                        break;
                }
                state = m_state.get();
            }

            if (s_logger.isLoggable(Level.FINEST))
            {
                s_logger.finest(
                        m_remoteSocketAddress.toString() +
                        ": ready=" + bytesReady + " sent=" + bytesSent +
                        " " + stateToString(state) + " -> " + stateToString(newState) );
            }
        }
        catch (IOException ex)
        {
            long newState;
            for (;;)
            {
                assert( (state & SOCK_RC_MASK) > 0 );

                newState = state;
                newState |= CLOSE;
                newState -= SOCK_RC;
                newState &= ~LENGTH_MASK;
                if (m_state.compareAndSet(state, newState))
                {
                    state = newState;
                    break;
                }
                state = m_state.get();
            }

            if (s_logger.isLoggable(Level.FINER))
            {
                s_logger.finer(
                        m_socketChannel.socket().getRemoteSocketAddress().toString() +
                        ": " + ex.toString() +
                        " " + stateToString(state) + " -> " + stateToString(newState) + "." );
            }

            if ((state & SOCK_RC_MASK) == 0)
                m_collider.executeInSelectorThread( new SelectorDeregistrator() );
        }
    }
}
