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

/**
 * {@code Acceptor} is used to accepot inbound socket connections.
 * See {@link Collider} for details.
 */

public abstract class Acceptor extends SessionEmitter
{
    public Acceptor()
    {
        this(new InetSocketAddress(0));
    }

    public Acceptor(InetSocketAddress addr)
    {
        super(addr);
    }

    /**
     * Called by the framework to create <tt>Session.Listener</tt> instance
     * to be linked with the session and underlying socket. METHOD IS NOT [MT] SAFE,
     * can be executed concurrently in a number of threads.
     * Connection and underlying socket will be closed if returns <tt>null</tt>,
     * but any data scheduled with <tt>sendData</tt> still will be sent.
     */
    public abstract Session.Listener createSessionListener(Session session);

    /**
     * Called by framework right before the acceptor is ready to accept connections.
     * It is still safe to remove the <tt>Acceptor</tt> instance from the collider
     * within this method, no one connection will be accepted then.
     * @param collider the collider instance to work in
     * @param localPort the local port to accept connections on
     */
    public void onAcceptorStarted(Collider collider, int localPort)
    {
    }
}
