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

package org.jsl.collider;

import java.net.InetSocketAddress;

public abstract class Acceptor
{
    private InetSocketAddress m_addr;
    private boolean m_reuseAddr;
    public boolean m_tcpNoDelay;

    public Acceptor( InetSocketAddress addr )
    {
        m_addr = addr;
        m_reuseAddr = false;
        m_tcpNoDelay = false;
    }

    public InetSocketAddress getAddr() { return m_addr; }

    public void setReuseAddr( boolean reuseAddr ) { m_reuseAddr = reuseAddr; }
    public boolean getReuseAddr() { return m_reuseAddr; }

    public void setTcpNoDelay( boolean tcpNoDelay ) { m_tcpNoDelay = tcpNoDelay; }
    public boolean getTcpNoDelay() { return m_tcpNoDelay; }

    public abstract Session.Listener createSessionListener( Session session );

    public void onAcceptorStarted( int localPort ) {}
    public void onAcceptorStartingFailure( String errorText ) {}
}
