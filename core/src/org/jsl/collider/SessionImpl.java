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
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;


public class SessionImpl extends Collider.SelectorThreadRunnable
        implements Session, Collider.ChannelHandler, Runnable
{
    private static final long LENGTH_MASK   = 0x000000FFFFFFFFFFL;
    private static final long CLOSED        = 0x0000010000000000L;
    private static final long WAIT_WRITE    = 0x0000020000000000L;
    private static final long STATE_MASK    = 0x0000300000000000L;
    private static final long ST_STARTING   = 0x0000000000000000L;
    private static final long ST_RUNNING    = 0x0000100000000000L;
    private static final long ST_STOPPED    = 0x0000200000000000L;

    private static final Logger s_logger = Logger.getLogger( SessionImpl.class.getName() );

    private final Collider m_collider;
    private SocketChannel m_socketChannel;
    private SelectionKey m_selectionKey;
    private final SocketAddress m_localSocketAddress;
    private final SocketAddress m_remoteSocketAddress;

    private final AtomicLong m_state;
    private final OutputQueue m_outputQueue;
    private final ByteBuffer [] m_iov;
    private InputQueue m_inputQueue;

    private static class DummyListener implements Listener
    {
        public void onDataReceived(ByteBuffer data) { }
        public void onConnectionClosed() { }
    }

    private class SelectorDeregistrator extends Collider.SelectorThreadRunnable
    {
        public void runInSelectorThread()
        {
            if (s_logger.isLoggable(Level.FINE))
                s_logger.fine( m_socketChannel.socket().getRemoteSocketAddress().toString() );

            m_selectionKey.cancel();
            m_selectionKey = null;

            try
            {
                m_socketChannel.close();
            }
            catch (IOException ex)
            {
                if (s_logger.isLoggable(Level.WARNING))
                    s_logger.warning( ex.toString() );
            }
            m_socketChannel = null;
        }
    }

    private static String stateToString( long state )
    {
        String ret = "(";
        if ((state & CLOSED) != 0)
            ret += "CLOSED ";

        if ((state & WAIT_WRITE) != 0)
            ret += "WAIT_WRITE ";

        long length = (state & LENGTH_MASK);

        state &= STATE_MASK;
        if (state == ST_STARTING)
            ret += "STARTING ";
        else if (state == ST_RUNNING)
            ret += "STARTED ";
        else if (state == ST_STOPPED)
            ret += "STOPPED ";
        else
            ret += "???";

        ret += length;
        ret += ")";
        return ret;
    }

    public void onReaderStopped()
    {
        long state = m_state.get();
        long newState;
        for (;;)
        {
            assert( (state & STATE_MASK) == ST_RUNNING );
            newState = state;

            newState |= CLOSED;
            newState &= ~STATE_MASK;
            newState |= ST_STOPPED;

            if (m_state.compareAndSet(state, newState))
                break;

            state = m_state.get();
        }

        if (s_logger.isLoggable(Level.FINE))
        {
            s_logger.fine( m_socketChannel.socket().getRemoteSocketAddress().toString()
                    + ": " + stateToString(state) + " -> " + stateToString(newState) );
        }

        if ((state & LENGTH_MASK) == 0)
            m_collider.executeInSelectorThread( new SelectorDeregistrator() );
    }

    public SessionImpl(
                Collider collider,
                SessionEmitter sessionEmitter,
                SocketChannel socketChannel,
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
        m_localSocketAddress = socketChannel.socket().getLocalSocketAddress();
        m_remoteSocketAddress = socketChannel.socket().getRemoteSocketAddress();

        m_state = new AtomicLong( ST_STARTING );
        m_outputQueue = new OutputQueue( outputQueueDataBlockCache );
        m_iov = new ByteBuffer[sendIovMax];
    }

    public Collider getCollider() { return m_collider; }
    public SocketAddress getLocalAddress() { return m_localSocketAddress; }
    public SocketAddress getRemoteAddress() { return m_remoteSocketAddress; }

    public boolean sendData( ByteBuffer data )
    {
        long state = m_state.get();
        if ((state & CLOSED) != 0)
            return false;

        long bytesReady = m_outputQueue.addData( data );
        if (bytesReady > 0)
        {
            for (;;)
            {
                if ((state & CLOSED) != 0)
                    return false;

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
            }

            if ((state & LENGTH_MASK) == bytesReady)
                m_collider.executeInThreadPool( this );
        }

        return true;
    }

    public boolean closeConnection()
    {
        long state = m_state.get();
        for (;;)
        {
            if ((state & CLOSED) != 0)
                return false;

            long newState = (state | CLOSED);

            if (m_state.compareAndSet(state, newState))
                break;

            state = m_state.get();
        }

        if (s_logger.isLoggable(Level.FINE))
        {
            s_logger.fine( m_socketChannel.socket().getRemoteSocketAddress().toString()
                           + ": " + stateToString(state) );
        }

        if ((state & STATE_MASK) == ST_RUNNING)
            m_inputQueue.stop();

        return true;
    }

    public final SocketChannel register( Selector selector )
    {
        try
        {
            m_selectionKey = m_socketChannel.register( selector, 0, this );
            return null;
        }
        catch (IOException ex)
        {
            if (s_logger.isLoggable(Level.WARNING))
                s_logger.warning( ex.toString() );

            SocketChannel socketChannel = m_socketChannel;
            m_socketChannel = null;
            return socketChannel;
        }
    }

    public void initialize( InputQueue.DataBlockCache inputQueueDataBlockCache, Listener listener )
    {
        if (listener == null)
            listener = new DummyListener();

        m_inputQueue = new InputQueue( m_collider,
                                       inputQueueDataBlockCache,
                                       this,
                                       m_socketChannel,
                                       m_selectionKey,
                                       listener );

        long state = m_state.get();
        for (;;)
        {
            assert( (state & STATE_MASK) == ST_STARTING );

            if ((state & CLOSED) != 0)
            {
                listener.onConnectionClosed();
                break;
            }

            long newState = state;
            newState &= ~STATE_MASK;
            newState |= ST_RUNNING;

            if (m_state.compareAndSet(state, newState))
            {
                m_inputQueue.start();
                break;
            }

            state = m_state.get();
        }

        if ((state & WAIT_WRITE) != 0)
            m_collider.executeInSelectorThread( this );
    }

    public void handleReadyOps( Executor executor )
    {
        int readyOps = m_selectionKey.readyOps();
        m_selectionKey.interestOps( m_selectionKey.interestOps() & ~readyOps );

        if ((readyOps & SelectionKey.OP_READ) != 0)
            m_collider.executeInThreadPool( m_inputQueue );

        if ((readyOps & SelectionKey.OP_WRITE) != 0)
            executor.execute( this );
    }

    public void runInSelectorThread()
    {
        int interestOps = m_selectionKey.interestOps();
        m_selectionKey.interestOps( interestOps | SelectionKey.OP_WRITE );
    }

    public void run()
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
            long bytesSent = m_socketChannel.write( m_iov, 0, iovc );

            for (int idx=0; idx<iovc; idx++)
                m_iov[idx] = null;

            if (bytesSent == 0)
                System.out.println( "strange" );

            m_outputQueue.removeData( pos0, bytesSent );

            assert( bytesSent <= (state & STATE_MASK) );
            state = m_state.addAndGet( -bytesSent );

            if (bytesSent < bytesReady)
            {
                for (;;)
                {
                    if ((state & STATE_MASK) == ST_STARTING)
                    {
                        /* SelectionKey is not initialized yet. */
                        long newState = (state | WAIT_WRITE);
                        if (m_state.compareAndSet(state, newState))
                            break;
                    }
                    else
                    {
                        m_collider.executeInSelectorThread( this );
                        break;
                    }
                    state = m_state.get();
                }
            }
            else if ((state & LENGTH_MASK) > 0)
                m_collider.executeInThreadPool( this );
            else if ((state & CLOSED) != 0)
            {
                if ((state & STATE_MASK) == ST_STOPPED)
                    m_collider.executeInSelectorThread( new SelectorDeregistrator() );
            }

            /*
            if (s_logger.isLoggable(Level.FINE))
            {
                s_logger.fine( m_socketChannel.socket().getRemoteSocketAddress().toString()
                        + ": sent " + bytesSent + " bytes, " + stateToString(state) + "." );
            }
            */
        }
        catch (IOException ex)
        {
            for (;;)
            {
                long newState = state;
                newState |= CLOSED;
                newState &= ~LENGTH_MASK;
                if (m_state.compareAndSet(state, newState))
                    break;
                state = m_state.get();
            }

            if (s_logger.isLoggable(Level.FINE))
            {
                s_logger.fine( m_socketChannel.socket().getRemoteSocketAddress().toString()
                        + ": " + ex.toString() + ", " + stateToString(state) + "." );
            }

            if ((state & STATE_MASK) == ST_STOPPED)
                m_collider.executeInSelectorThread( new SelectorDeregistrator() );
            /* else { socket channel read listener will be notified soon } */
        }
    }
}
