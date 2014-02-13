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

public abstract class Connector extends SessionEmitter
{
    public Connector( InetSocketAddress addr )
    {
        super( addr );
    }

    /**
     * Creates <tt>Session.Listener</tt> instance to be linked with the session.
     * Called by framework, derived class supposed to override the method.
     * Connection will be closed if returns <tt>null</tt>, but any data
     * scheduled with <tt>sendData</tt> call before return will be sent.
     */
    public abstract Session.Listener createSessionListener( Session session );

    /**
     * Called by framework in a case if asynchronous operation
     * with underlying socket channel throws some exception.
     */
    public abstract void onException( IOException ex );
}
