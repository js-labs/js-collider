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

import java.net.SocketAddress;
import java.nio.ByteBuffer;

public interface Session
{
    public static final int DISCARD_IN_DATA  = 0x0001;
    public static final int DISCARD_OUT_DATA = 0x0002;
    public static final int DISCARD_ALL = (DISCARD_IN_DATA | DISCARD_OUT_DATA);

    public interface Handler
    {
        public abstract void onDataReceived( ByteBuffer data );
        public abstract void onConnectionClosed();
    }

    public Collider getCollider();
    public SocketAddress getLocalAddress();
    public SocketAddress getRemoteAddress();

    public int sendData( ByteBuffer data );
    public int closeConnection( int flags );
}
