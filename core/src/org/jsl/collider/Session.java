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
         * Called by framework when some data is available.
         * Executed serially in a one thread, but not necessary always the same.
         * ByteBuffer is read only and valid only during function call.
         * Should not be retained for further use. If some data needs
         * to be used later then it should be copied.
         * Position in the byte buffer can be greater than 0,
         * limit can be less than capacity.
         */
        public abstract void onDataReceived( ByteBuffer data );

        /**
         * Called by framework when underlying socket is closed
         * and all income data is processed.
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
     * Sends data to the underlying socket channel. In most cases data will be written
     * asynchronously, but sometimes not. Retains the data buffer if it going to be sent
     * asynchronously.
     * @return  0 - data written to the socket or internal memory, byte buffer can be reused
     *         >0 - byte buffer is retained by the framework, will be send as soon as possible
     *         -1 - the session is closed
     */
    public int sendData( ByteBuffer data );

    /**
     * Method tries to writes data to the session underlying socket channel
     * if it is the single thread calling the <em>sendData</em> or <em>sendDataSync</em>.
     * Otherwise data will sent as <em>sendData</em> would be called.
     * @return  0 - data written to socket or internal memory, byte buffer can be reused
     *         >0 - byte buffer is retained by the framework, will be send as soon as possible
     *         -1 - the session is closed
     */
    public int sendDataSync( ByteBuffer data );

    /**
     * Method to be used to close the session.
     * Works asynchronously so connection will not be closed immediately
     * after function return. Outgoing data scheduled but not sent yet
     * will be sent. Any data already read from the socket at the moment
     * but not processed yet will be processed. <em>onConnectionClosed</em>
     * will be called after all received data will be processed.
     * All further <em>sendData</em>, <em>sendDataAsync</em> and
     * <em>closeConnection</em> calls will return -1.
     * @return >0 - amount of data waiting to be sent
     *         <0 - session already closed and has no data to be sent
     */
    public int closeConnection();
}
