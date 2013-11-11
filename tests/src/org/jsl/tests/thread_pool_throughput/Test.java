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

public abstract class Test
{
    protected final int m_totalEvents;
    protected final int m_producers;
    protected final int m_workers;

    protected Test( int totalEvents, int producers, int workers )
    {
        assert( (totalEvents % producers) == 0 );
        m_totalEvents = totalEvents;
        m_producers = producers;
        m_workers = workers;
    }

    public final int getProducers() { return m_producers; }
    public final int getWorkers() { return m_workers; }

    public abstract String getName();
    public abstract long runTest();
}
