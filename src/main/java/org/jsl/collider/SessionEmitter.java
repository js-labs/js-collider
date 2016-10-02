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

import java.net.InetSocketAddress;
import java.nio.ByteOrder;


public abstract class SessionEmitter
{
    private final InetSocketAddress m_addr;

    /*  > 0 : use direct buffers
     * == 0 : use heap buffers
     *  < 0 : use collider default buffer type
     */
    public int useDirectBuffers;
    public ByteOrder byteOrder;

    public boolean reuseAddr;
    public boolean tcpNoDelay;

    public int socketRecvBufSize;
    public int socketSendBufSize;
    public int forwardReadMaxSize;
    public int inputQueueBlockSize;

    public int joinMessageMaxSize;

    public SessionEmitter( InetSocketAddress addr )
    {
        m_addr = addr;

        useDirectBuffers = -1;
        byteOrder = null;

        reuseAddr = false;
        tcpNoDelay = true;

        /* Use collider global settings by default */
        socketRecvBufSize = 0;
        socketSendBufSize = 0;
        forwardReadMaxSize = 0;
        inputQueueBlockSize = 0;

        /* -1 - use collider global value,
         *  0 - disable message join,
         */
        joinMessageMaxSize = -1;
    }

    public InetSocketAddress getAddr()
    {
        return m_addr;
    }

    /**
     * Called by framework to create session listener instance.
     * See <tt>Acceptor.createSessionListener</tt> and
     * <tt>Connector.createSessionListener</tt> for detailed description.
     */
    public abstract Session.Listener createSessionListener( Session session );
}
