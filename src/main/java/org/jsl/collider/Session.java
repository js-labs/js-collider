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

import java.net.SocketAddress;
import java.nio.ByteBuffer;

public interface Session
{
    public interface Listener
    {
        /**
         * Called by framework when some data is available.
         * Executed serially in a one thread, but not necessary always the same.
         * Position in the byte buffer can be greater than 0,
         * limit can be less than capacity.
         */
        public abstract void onDataReceived( RetainableByteBuffer data );

        /**
         * Called by framework when underlying socket channel
         * is closed and all income data is processed.
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
     * Schedules data to be sent to the underlying socket channel.
     * Retains the data buffer, but buffer remains unchanged
     * (even it's attributes like a position, limit etc)
     * so the buffer can be reused to send the same data
     * to the different sessions.
     * @return >=0 - byte buffer is retained by the framework, will be sent as soon as possible
     *          -1 - the session is closed
     */
    public int sendData( ByteBuffer data );
    public int sendData( RetainableByteBuffer data );

    /**
     * Method makes an attempt to write data synchronously to the underlying socket channel.
     * It can happen if it is the single thread calling the <em>sendData</em> or <em>sendDataSync</em>.
     * Otherwise data will sent as <em>sendData</em> would be called.
     * @return  0 - data written to socket, byte buffer can be reused
     *         >0 - byte buffer is retained by the framework, will be sent as soon as possible
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

    /**
     * Replaces the current session listener with a new one.
     * Supposed to be called only from the <tt>onDataReceived()</tt> callback.
     * Calling it not from the <tt>onDataReceived</tt> callback will result
     * in undefined behaviour.
     */
    public Listener replaceListener( Listener newListener );

    public int accelerate( ShMem shMem, ByteBuffer message );
}
