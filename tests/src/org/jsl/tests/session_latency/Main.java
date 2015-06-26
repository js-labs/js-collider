/*
 * JS-Collider framework tests.
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

package org.jsl.tests.session_latency;

public class Main
{
    public static void main( String [] args )
    {
        int sessions = 2;
        int messages = 1000;
        int messageLength = 500;

        if (args.length > 0)
            sessions = Integer.parseInt( args[0] );

        if (args.length > 1)
            messages = Integer.parseInt( args[1] );

        if (args.length > 2)
            messageLength = Integer.parseInt( args[2] );

        System.out.println(
                "Latency test: " +
                sessions + " sessions, " +
                messages + " messages, " +
                messageLength + " bytes/message." );

        final Client client = new Client( sessions );
        new Server(sessions, messages, messageLength, client).run();
    }
}
