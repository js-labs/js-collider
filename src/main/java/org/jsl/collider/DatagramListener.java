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
import java.net.SocketAddress;
import java.nio.ByteBuffer;

public abstract class DatagramListener
{
    private final InetSocketAddress m_addr;

    public int socketRecvBufSize;
    public int inputQueueBlockSize;
    public int forwardReadMaxSize;
    public int readMinSize;

    public DatagramListener( InetSocketAddress addr )
    {
        m_addr = addr;

        /* use collider global default values */
        socketRecvBufSize = 0;
        inputQueueBlockSize = 0;
        forwardReadMaxSize = 0;
        readMinSize = 0;
    }

    public InetSocketAddress getAddr()
    {
        return m_addr;
    }

    public abstract void onDataReceived( RetainableByteBuffer data, SocketAddress sourceAddr );
}
