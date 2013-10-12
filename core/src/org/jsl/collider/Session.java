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
         * Called by framework when connection is closed.
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
     * Writes data to the session socket if it is the single thread calling
     * the <tt>sendData</tt> or <tt>sendDataAsync</tt>. Otherwise data will
     * be copied into internal buffer and will be sent as soon as socket
     * will be ready for writing.
     * @return  0 - data written to socket
     *         >0 - another thread writing the socket,
     *              return value is the amount of data waiting to be sent
     *         -1 - the session is closed
     */
    public long sendData( ByteBuffer data );

    /**
     * Copies data into internal buffer and send it asynchronously from
     * another thread. Method is useful when some small messages needs
     * to be sent into the session at once.
     * @return  0 - data written to socket
     *         >0 - another thread writing the socket,
     *              return value is the amount of data waiting to be sent
     *         -1 - the session is closed
     */
    public long sendDataAsync( ByteBuffer data );

    /**
     * Method to be used to close session.
     * Works asynchronously so connection will not be closed immediately
     * after function return. Outgoing data scheduled but not sent yet
     * will be sent. Any data already read from the socket but not processed
     * yet will be processed. <tt>onConnectionClosed</tt> will be called
     * after all received data will be processed. All further <tt>sendData</tt>
     * and <tt>sendDataAsync</tt> calls will return -1.
     * @return >0 - amount of data waiting to be sent
     *         <0 - session already closed and has no data to be sent
     */
    public long closeConnection();
}
