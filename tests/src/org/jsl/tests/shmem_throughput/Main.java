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

package org.jsl.tests.shmem_throughput;

import org.jsl.collider.Collider;
import java.io.IOException;

public class Main
{
    public static void main( String [] args )
    {
        int sessions = 1;
        int messages = 100000;
        int messageLength = 500;

        try
        {
            final Collider collider = Collider.create();
            collider.addAcceptor( new Server.Acceptor(sessions, messages, messageLength) );
            collider.run();
        }
        catch (IOException ex)
        {
            ex.printStackTrace();
        }
    }
}
