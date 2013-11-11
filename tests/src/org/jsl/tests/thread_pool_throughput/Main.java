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

package org.jsl.tests.thread_pool_throughput;

public class Main
{
    private Main()
    {
    }

    public void run()
    {
        int TOTAL_EVENTS = 1000000;

        Test [] tests =
        {
            new ThreadPoolTest( TOTAL_EVENTS, 1, 1 ),
            new ThreadPoolTest( TOTAL_EVENTS, 4, 4 ),
            new ExecutorTest(TOTAL_EVENTS, 1, 1),
            new ExecutorTest( TOTAL_EVENTS, 4, 4 )
        };

        for (Test test : tests)
        {
            System.out.println( test.getName() + ":" );
            long tm = test.runTest();
            tm /= 1000;
            System.out.println( TOTAL_EVENTS + " events processed at " +
                                String.format( "%d.%06d", tm/1000000, tm%1000000 ) + " sec: " +
                                test.getProducers() + " -> " +
                                test.getWorkers() + " workers." );
        }
    }

    public static void main( String [] args )
    {
        new Main().run();
    }
}
