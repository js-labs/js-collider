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

import java.net.SocketAddress;
import java.nio.ByteBuffer;


public interface Session
{
    public interface Listener
    {
        /**
         * Called by framework when new data received.
         */
        public abstract void onDataReceived( ByteBuffer data );

        /**
         * Called by framework when underlying socket is closed.
         */
        public abstract void onConnectionClosed();
    }

    /**
     * Returns Collider instance the session is linked with.
     */
    public Collider getCollider();

    /**
     * Returns local socket address of the session.
     */
    public SocketAddress getLocalAddress();

    /**
     * Returns remote socket address of the session.
     */
    public SocketAddress getRemoteAddress();

    /**
     * Writes data to the session socket if it is the first thread
     * calling the <tt>sendData</tt> or <tt>sendDataAsync</tt>.
     * Otherwise data will be copied into internal buffer
     * and will be sent as soon as socket will be available for writing.
     * @return  0 - data written to socket
     *         >0 - the amount of data waiting to be sent
     *         -1 - the session is closed
     */
    public long sendData( ByteBuffer data );

    public long sendDataAsync( ByteBuffer data );
    public long closeConnection();
}
