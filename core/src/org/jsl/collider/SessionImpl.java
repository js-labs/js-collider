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
    private static final long ST_STARTING   = 0x0000100000000000L;
    private static final long ST_RUNNING    = 0x0000200000000000L;
    private static final long SOCK_RC_MASK  = 0x0003000000000000L;
    private static final long SOCK_RC       = 0x0001000000000000L;
    private static final long WRITERS_MASK  = 0x00F0000000000000L;
    private static final long WRITER        = 0x0010000000000000L;

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

    private long addAndSendDataAsync( long state, final ByteBuffer data )
    {
        final long bytesReady = m_outputQueue.addData( data );
        for (;;)
        {
            assert( (state & WRITERS_MASK) != 0 );
            long newState = state;
            newState += bytesReady;
            newState -= WRITER;
            if (m_state.compareAndSet(state, newState))
            {
                newState &= LENGTH_MASK;
                if ((bytesReady > 0) && (newState == bytesReady))
                    m_collider.executeInThreadPool( this );
                return newState;
            }
            state = m_state.get();
        }
    }

    private long decState( long state, final long bytesSent )
    {
        for (;;)
        {
            assert( (state & LENGTH_MASK) >= bytesSent );
            assert( (state & SOCK_RC_MASK) > 0 );

            long newState = (state - bytesSent);
            if ((newState & LENGTH_MASK) == 0)
            {
                if ((state & CLOSE) == 0)
                {
                    if (m_state.compareAndSet(state, newState))
                        return newState;
                }
                else
                {
                    newState -= SOCK_RC;
                    if (m_state.compareAndSet(state, newState))
                    {
                        if ((newState & SOCK_RC_MASK) == 0)
                            m_collider.executeInSelectorThread( new SelectorDeregistrator() );
                        return newState;
                    }
                }
            }
            else if ((state & STATE_MASK) != ST_STARTING)
            {
                if (m_state.compareAndSet(state, newState))
                {
                    m_collider.executeInThreadPool( this );
                    return newState;
                }
            }
            else /* ((state  & LENGTH_MASK) > 0) && ((state & STATE_MASK) == ST_STARTING) */
            {
                if ((newState & WAIT_WRITE) == 0)
                {
                    newState |= WAIT_WRITE;
                    if (m_state.compareAndSet(state, newState))
                        return newState;
                }
                else
                    return newState;
            }
            state = m_state.get();
        }
    }

    private long sendDataSync( long state, ByteBuffer data )
    {
        try
        {
            final long bytesSent = m_socketChannel.write( data );
            if (bytesSent == 0)
                System.out.println( "strange" );

            final long newState = decState( state, bytesSent );
            final long length = (newState & LENGTH_MASK);

            if (length > 0)
                m_collider.executeInThreadPool( this );

            if (s_logger.isLoggable(Level.FINEST))
            {
                s_logger.finest(
                        m_remoteSocketAddress.toString() +
                        ": ready=" + (state & LENGTH_MASK) + " sent=" + bytesSent + " " +
                        stateToString(state) + " -> " + stateToString(newState) );
            }

            return length;
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
                    break;
                state = m_state.get();
            }

            if (s_logger.isLoggable(Level.FINER))
            {
                s_logger.finer(
                        m_remoteSocketAddress.toString() +
                        ": " + ex.toString() + " " +
                        stateToString(state) + " -> " + stateToString(newState) + "." );
            }

            if ((newState & SOCK_RC_MASK) == 0)
                m_collider.executeInSelectorThread( new SelectorDeregistrator() );

            return -1;
        }
    }

    public SessionImpl(
                ColliderImpl collider,
                SocketChannel socketChannel,
                SelectionKey selectionKey,
                OutputQueue.DataBlockCache outputQueueDataBlockCache )
    {
        m_collider = collider;
        m_socketChannel = socketChannel;
        m_selectionKey = selectionKey;
        m_localSocketAddress = socketChannel.socket().getLocalSocketAddress();
        m_remoteSocketAddress = socketChannel.socket().getRemoteSocketAddress();

        m_starter = new Starter();
        m_state = new AtomicLong( ST_STARTING + SOCK_RC + SOCK_RC );
        m_outputQueue = new OutputQueue( outputQueueDataBlockCache );
        m_iov = new ByteBuffer[8];
    }

    public Collider getCollider() { return m_collider; }
    public SocketAddress getLocalAddress() { return m_localSocketAddress; }
    public SocketAddress getRemoteAddress() { return m_remoteSocketAddress; }

    public long sendData( ByteBuffer data )
    {
        final int dataSize = data.remaining();
        long state = m_state.get();
        for (;;)
        {
            if ((state & CLOSE) != 0)
                return -1;

            if ((state & (WRITERS_MASK|LENGTH_MASK)) == 0)
            {
                long newState = (state + dataSize);
                if (m_state.compareAndSet(state, newState))
                    return sendDataSync( newState, data );
            }
            else if ((state & WRITERS_MASK) != WRITERS_MASK)
            {
                long newState = (state + WRITER);
                if (m_state.compareAndSet(state, newState))
                    return addAndSendDataAsync( newState, data );
            }
            /* else maximum writers number reached */
            state = m_state.get();
        }
    }

    public long sendDataAsync( ByteBuffer data )
    {
        long state = m_state.get();
        for (;;)
        {
            if ((state & CLOSE) != 0)
                return -1;

            if ((state & WRITERS_MASK) != WRITERS_MASK)
            {
                long newState = (state + WRITER);
                if (m_state.compareAndSet(state, newState))
                    return addAndSendDataAsync( newState, data );
            }
            /* else maximum number of writers reached,
             * let's wait a bit.
             */
            state = m_state.get();
        }
    }

    public long closeConnection()
    {
        long state = m_state.get();
        long newState;
        for (;;)
        {
            if ((state & CLOSE) != 0)
                return -1;

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

        return (newState & LENGTH_MASK);
    }

    public final void initialize(
                int inputQueueMaxSize,
                InputQueue.DataBlockCache inputQueueDataBlockCache,
                Listener listener )
    {
        m_selectionKey.attach( this );

        if (listener == null)
            listener = new DummyListener();

        m_inputQueue = new InputQueue( m_collider,
                                       inputQueueMaxSize,
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

        int iovc = 1; /* at least one ByteBuffer should be ready */
        for (; iovc<m_iov.length && m_iov[iovc]!= null; iovc++);

        try
        {
            final long bytesSent = m_socketChannel.write( m_iov, 0, iovc );
            if (bytesSent == 0)
                System.out.println( "strange" );

            for (int idx=0; idx<iovc; idx++)
                m_iov[idx] = null;

            m_outputQueue.removeData( pos0, bytesSent );
            final long newState = decState( state, bytesSent );

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
                    break;
                state = m_state.get();
            }

            if (s_logger.isLoggable(Level.FINER))
            {
                s_logger.finer(
                        m_remoteSocketAddress.toString() +
                        ": " + ex.toString() + " " +
                        stateToString(state) + " -> " + stateToString(newState) + "." );
            }

            if ((newState & SOCK_RC_MASK) == 0)
                m_collider.executeInSelectorThread( new SelectorDeregistrator() );
        }
    }
}
