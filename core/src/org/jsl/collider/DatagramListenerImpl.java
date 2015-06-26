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
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.DatagramChannel;
import java.nio.channels.Selector;
import java.nio.channels.MembershipKey;
import java.nio.channels.SelectionKey;
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DatagramListenerImpl extends ThreadPool.Runnable
                                  implements ColliderImpl.ChannelHandler
{
    private static final Logger s_logger = Logger.getLogger( "org.jsl.collider.Datagram" );

    private static final AtomicIntegerFieldUpdater<DatagramListenerImpl>
        s_stateUpdater = AtomicIntegerFieldUpdater.newUpdater( DatagramListenerImpl.class, "m_state" );

    private static final DummyListener s_dummyListener = new DummyListener();

    private static final int STATE_MASK  = 0x30000000;
    private static final int ST_STARTING = 0x00000000;
    private static final int ST_RUNNING  = 0x10000000;
    private static final int STOP        = 0x40000000;
    private static final int LENGTH_MASK = 0x0FFFFFFF;

    private final ColliderImpl m_collider;
    private final Selector m_selector;
    private final RetainableDataBlockCache m_dataBlockCache;
    private final DatagramListener m_datagramListener;
    private DatagramChannel m_datagramChannel;
    private MembershipKey m_membershipKey;
    private final Starter0 m_starter0;
    private final Starter1 m_starter1;
    private final Suspender m_suspender;
    private final int m_readMinSize;
    private final InetSocketAddress m_addr;
    private final int m_forwardReadMaxSize;

    private volatile DatagramListener m_listener;
    private SelectionKey m_selectionKey;
    private volatile int m_state;
    private PacketInfo m_packetInfoHead;
    private PacketInfo m_packetInfoTail;
    private RetainableDataBlock m_dataBlockHead;
    private RetainableDataBlock m_dataBlockTail;
    private long m_threadID;

    /* Collider.removeDatagramListener() definitely should guarantee
     * that no one thread process input data on return.
     * There is one more resource: socket, but not sure we need to wait it's close.
     */
    private final ReentrantLock m_lock;
    private final Condition m_cond;
    private boolean m_run;

    private static class PacketInfo
    {
        public final int length;
        public final SocketAddress addr;
        public PacketInfo next;

        public PacketInfo( int length, SocketAddress addr )
        {
            this.length = length;
            this.addr = addr;
        }
    }

    private static class DummyListener extends DatagramListener
    {
        public DummyListener()
        {
            super( null );
        }

        public void onDataReceived( RetainableByteBuffer data, SocketAddress sourceAddr )
        {
        }
    }

    private class SelectorRegistrator extends ColliderImpl.SelectorThreadRunnable
    {
        public int runInSelectorThread()
        {
            for (;;)
            {
                final int state = m_state;
                if ((state & STOP) != 0)
                {
                    /* Socket and data block are released by stopAndWait() */
                    return 0;
                }

                assert( state == ST_STARTING );
                final int newState = ST_RUNNING;
                if (s_stateUpdater.compareAndSet(DatagramListenerImpl.this, state, ST_RUNNING))
                {
                    if (s_logger.isLoggable(Level.FINE))
                    {
                        s_logger.log( Level.FINE,
                                m_addr + ": " + stateToString(state) + " -> " + stateToString(newState) + "." );
                    }
                    break;
                }
            }

            try
            {
                m_selectionKey = m_datagramChannel.register(
                        m_selector, SelectionKey.OP_READ, DatagramListenerImpl.this );
            }
            catch (ClosedChannelException ex)
            {
                /* Should not happen, means a but in the framework. */
                if (s_logger.isLoggable(Level.WARNING))
                    s_logger.log( Level.WARNING, ex.toString() );
            }

            return 0;
        }
    }

    private class Starter0 extends ColliderImpl.SelectorThreadRunnable
    {
        public int runInSelectorThread()
        {
            assert( m_selectionKey.interestOps() == 0 );
            m_selectionKey.interestOps( SelectionKey.OP_READ );
            return 0;
        }
    }

    private class Starter1 extends ColliderImpl.SelectorThreadRunnable
    {
        public int runInSelectorThread()
        {
            assert( m_selectionKey.interestOps() == 0 );
            m_selectionKey.interestOps( SelectionKey.OP_READ );
            return 1;
        }
    }

    private static class Suspender extends ColliderImpl.SelectorThreadRunnable
    {
        public int runInSelectorThread()
        {
            return 1;
        }
    }

    private class Stopper extends ColliderImpl.SelectorThreadRunnable
    {
        public int runInSelectorThread()
        {
            final int interestOps = m_selectionKey.interestOps();
            if ((interestOps & SelectionKey.OP_READ) == 0)
                m_collider.executeInSelectorThreadLater( this );
            else
            {
                m_selectionKey.cancel();
                m_selectionKey = null;
                closeSocket();

                if ((m_state & LENGTH_MASK) == 0)
                {
                    boolean removeListener = false;
                    m_lock.lock();
                    try
                    {
                        if (m_run)
                        {
                            m_run = false;
                            m_cond.signalAll();
                            removeListener = true;
                        }
                    }
                    finally
                    {
                        m_lock.unlock();
                    }

                    if (removeListener)
                        m_collider.removeDatagramListenerNoWait( m_datagramListener );
                }
            }
            return 0;
        }
    }

    private static String stateToString( int state )
    {
        String ret = "[";
        final int st = (state & STATE_MASK);
        if (st == ST_STARTING)
            ret += "STARTING";
        else if (st == ST_RUNNING)
            ret += "RUNNING";
        else
            ret += "?";

        ret += " ";
        ret += (state & LENGTH_MASK);
        ret += "]";

        return ret;
    }

    private void closeSocket()
    {
        if (m_membershipKey != null)
        {
            m_membershipKey.drop();
            m_membershipKey = null;
        }

        try
        {
            m_datagramChannel.close();
        }
        catch (IOException ex)
        {
            /* Should not happen, considered as a bug. */
            if (s_logger.isLoggable(Level.WARNING))
                s_logger.log( Level.WARNING, ex.toString() );
        }
        m_datagramChannel = null;
    }

    private void handleData( int state )
    {
        assert( m_threadID == -1 );
        final long threadID = Thread.currentThread().getId();
        m_threadID = threadID;

        int bytesReady = (state & LENGTH_MASK);
        int bytesRemaining = bytesReady;
        RetainableDataBlock dataBlock = m_dataBlockHead;
        RetainableByteBuffer rw = dataBlock.rw;
        int position = rw.position();
        int capacity = rw.capacity();

        for (;;)
        {
            final PacketInfo packetInfo = m_packetInfoHead.next;
            m_packetInfoHead.next = null;
            m_packetInfoHead = packetInfo;

            position = (position + 3) & -4;
            if ((capacity - position) < m_readMinSize)
            {
                final RetainableDataBlock next = dataBlock.next;
                dataBlock.next = null;
                dataBlock.release();
                m_dataBlockHead = next;
                dataBlock = next;
                rw = dataBlock.rw;
                position = rw.position();
                capacity = rw.capacity();
            }

            final int limit = (position + packetInfo.length);
            rw.position( position );
            rw.limit( limit );

            m_listener.onDataReceived( rw, packetInfo.addr );

            position = limit;

            assert( bytesRemaining >= packetInfo.length );
            bytesRemaining -= packetInfo.length;
            if (bytesRemaining > 0)
                continue;

            rw.position( position );
            rw.limit( capacity );

            for (;;)
            {
                assert( (state & LENGTH_MASK) >= bytesReady );
                final int newState = (state - bytesReady);

                if ((newState & LENGTH_MASK) == 0)
                {
                    /* m_threadID should be reset before CAS to avoid race. */
                    m_threadID = -1;
                    if (s_stateUpdater.compareAndSet(this, state, newState))
                    {
                        if ((newState & STOP) == 0)
                        {
                            if ((state & LENGTH_MASK) >= m_forwardReadMaxSize)
                                m_collider.executeInSelectorThread( m_starter0 );
                        }
                        else
                        {
                            boolean removeListener = false;
                            m_lock.lock();
                            try
                            {
                                /*        thread 1              thread 2
                                 *     m_state |= STOP     |  read packet
                                 *     .                   |  process packet
                                 *     .                   |  wait packet
                                 *     .                   |  process packet
                                 *     schedule Stopper()
                                 * So, this branch can work more than one time while datagram
                                 * reader is being stopped, should be handled properly.
                                 */
                                if (m_run)
                                {
                                    m_run = false;
                                    m_cond.signalAll();
                                    removeListener = true;
                                }
                            }
                            finally
                            {
                                m_lock.unlock();
                            }

                            if (removeListener)
                                m_collider.removeDatagramListenerNoWait( m_datagramListener );
                        }
                        return;
                    }
                    m_threadID = threadID;
                }
                else
                {
                    if (s_stateUpdater.compareAndSet(this, state, newState))
                    {
                        /* In a case if listener is being stopped we still need to process
                         * all received data to be sure data blocks will be properly recycled.
                         */
                        bytesReady = (newState & LENGTH_MASK);
                        if (((state & LENGTH_MASK) >= m_forwardReadMaxSize) &&
                            (bytesReady < m_forwardReadMaxSize))
                        {
                            if ((newState & STOP) == 0)
                                m_collider.executeInSelectorThread( m_starter0 );
                        }
                        break;
                    }
                }
                state = m_state;
            }

            bytesRemaining = bytesReady;
        }
    }

    public DatagramListenerImpl(
            ColliderImpl collider,
            Selector selector,
            RetainableDataBlockCache dataBlockCache,
            DatagramListener datagramListener,
            DatagramChannel datagramChannel,
            MembershipKey membershipKey )
    {
        m_collider = collider;
        m_selector = selector;
        m_dataBlockCache = dataBlockCache;
        m_datagramListener = datagramListener;
        m_datagramChannel = datagramChannel;
        m_membershipKey = membershipKey;
        m_starter0 = new Starter0();
        m_starter1 = new Starter1();
        m_suspender = new Suspender();

        int readMinSize = datagramListener.readMinSize;
        if (readMinSize == 0)
            readMinSize = collider.getConfig().datagramReadMinSize;
        m_readMinSize = readMinSize;
        m_addr = datagramListener.getAddr();

        int forwardReadMaxSize = datagramListener.forwardReadMaxSize;
        if (forwardReadMaxSize == 0)
            forwardReadMaxSize = collider.getConfig().forwardReadMaxSize;
        m_forwardReadMaxSize = forwardReadMaxSize;

        m_listener = datagramListener;
        m_selectionKey = null;
        m_state = ST_STARTING;
        m_packetInfoHead = new PacketInfo( 0, null );
        m_packetInfoTail = m_packetInfoHead;
        m_dataBlockHead = dataBlockCache.get(1);
        m_dataBlockTail = m_dataBlockHead;
        m_threadID = -1;

        m_lock = new ReentrantLock();
        m_cond = m_lock.newCondition();
        m_run = true;
    }

    public void start()
    {
        m_collider.executeInSelectorThread( new SelectorRegistrator() );
    }

    public void stopAndWait() throws InterruptedException
    {
        /* Not a problem if each possible stopping thread will change the value. */
        m_listener = s_dummyListener;

        final long threadID = Thread.currentThread().getId();
        if (threadID == m_threadID)
        {
            /* Called from the onDataReceived() */
            for (;;)
            {
                final int state = m_state;
                assert( (state & LENGTH_MASK) > 0 );
                if ((state & STOP) == 0)
                {
                    final int newState = (state | STOP);
                    if (s_stateUpdater.compareAndSet(this, m_state, newState))
                    {
                        s_logger.log( Level.FINER,
                                m_addr + ": state=" + stateToString(state) + " -> " + stateToString(newState) );
                        if ((newState & LENGTH_MASK) < m_forwardReadMaxSize)
                            m_collider.executeInSelectorThread( new Stopper() );
                        break;
                    }
                }
                else
                    break;
            }
        }
        else
        {
            for (;;)
            {
                final int state = m_state;
                if ((state & STOP) == 0)
                {
                    final int newState = (state | STOP);
                    if (s_stateUpdater.compareAndSet(this, state, newState))
                    {
                        if (s_logger.isLoggable(Level.FINER))
                        {
                            s_logger.log( Level.FINER,
                                    m_addr + ": state=" + stateToString(state) + " -> " + stateToString(newState) );
                        }

                        if ((newState & STATE_MASK) == ST_STARTING)
                        {
                            closeSocket();

                            assert( m_dataBlockHead == m_dataBlockTail );
                            m_dataBlockHead.release();
                            m_dataBlockHead = null;
                            m_dataBlockTail = null;

                            m_collider.removeDatagramListenerNoWait( m_datagramListener );

                            m_lock.lock();
                            try
                            {
                                assert( m_run );
                                m_run = false;
                                m_cond.signalAll();
                            }
                            finally
                            {
                                m_lock.unlock();
                            }

                            return;
                        }
                        else
                        {
                            m_collider.executeInSelectorThread( new Stopper() );
                            break;
                        }
                    }
                }
                else
                    break;
            }

            m_lock.lock();
            try
            {
                while (m_run)
                    m_cond.await();
            }
            finally
            {
                m_lock.unlock();
            }
        }
    }

    public void runInThreadPool()
    {
        RetainableDataBlock dataBlock = m_dataBlockTail;
        int pos = (dataBlock.ww.position() + 3) & -4;
        int space = (dataBlock.ww.limit() - pos);
        if (space < m_readMinSize)
        {
            dataBlock = m_dataBlockCache.get(1);
            pos = 0;
        }
        else
            dataBlock.ww.position( pos );

        try
        {
            SocketAddress sourceAddr = m_datagramChannel.receive( dataBlock.ww );

            int bytesReceived = dataBlock.ww.position() - pos;
            if ((sourceAddr == null) || (bytesReceived == 0))
            {
                /* Very strange, should not happen. */
                if (s_logger.isLoggable(Level.WARNING))
                {
                    s_logger.log( Level.WARNING,
                            m_addr + ": sourceAddr=" + sourceAddr + ": bytesReceived=" + bytesReceived );
                }

                if (dataBlock != m_dataBlockTail)
                    dataBlock.release();

                m_collider.executeInSelectorThreadNoWakeup( m_starter1 );
                return;
            }

            for (;;)
            {
                if (dataBlock != m_dataBlockTail)
                {
                    m_dataBlockTail.next = dataBlock;
                    m_dataBlockTail = dataBlock;
                }

                final PacketInfo packetInfo = new PacketInfo( bytesReceived, sourceAddr );
                m_packetInfoTail.next = packetInfo;
                m_packetInfoTail = packetInfo;

                for (;;)
                {
                    final int state = m_state;
                    assert( ((state & LENGTH_MASK) + bytesReceived) <= LENGTH_MASK );
                    final int newState = (state + bytesReceived);

                    if (s_stateUpdater.compareAndSet( this, state, newState ))
                    {
                        if ((state & LENGTH_MASK) == 0)
                        {
                            m_collider.executeInSelectorThreadNoWakeup( m_starter1 );
                            handleData( newState );
                            return;
                        }

                        if ((newState & LENGTH_MASK) >= m_forwardReadMaxSize)
                        {
                            m_collider.executeInSelectorThreadNoWakeup( m_suspender );
                            return;
                        }

                        break;
                    }
                }

                /* Let's try read more */

                pos = (dataBlock.ww.position() + 3) & -4;
                space = (dataBlock.ww.limit() - pos);
                if (space < m_readMinSize)
                {
                    dataBlock = m_dataBlockCache.get(1);
                    pos = 0;
                }
                else
                    dataBlock.ww.position( pos );

                sourceAddr = m_datagramChannel.receive( dataBlock.ww );

                bytesReceived = (dataBlock.ww.position() - pos);
                if ((sourceAddr == null) || (bytesReceived == 0))
                {
                    if (dataBlock != m_dataBlockTail)
                        dataBlock.release();

                    m_collider.executeInSelectorThreadNoWakeup( m_starter1 );
                    return;
                }
            }
        }
        catch (final IOException ex)
        {
            /* Should not happen, considered as a bug. */
            if (s_logger.isLoggable(Level.WARNING))
                s_logger.log( Level.WARNING, m_addr + ": " + ex.toString() );

            if (dataBlock != m_dataBlockTail)
                dataBlock.release();

            m_collider.executeInSelectorThreadNoWakeup( m_starter1 );
        }
    }

    public int handleReadyOps( ThreadPool threadPool )
    {
        assert( m_selectionKey.readyOps() == SelectionKey.OP_READ );
        threadPool.execute( this );
        m_selectionKey.interestOps( 0 );
        return 1;
    }
}
