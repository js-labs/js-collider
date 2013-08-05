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

import java.util.Calendar;
import java.util.Date;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;

public class LogFormatter extends Formatter
{
    private static final Calendar s_calendar = Calendar.getInstance();

    public String format( LogRecord logRecord )
    {
        StringBuilder sb = new StringBuilder();

        s_calendar.setTime(new Date(logRecord.getMillis()));

        String str = String.format( "%04d-%02d-%02d %02d:%02d:%02d.%03d t@%02d ",
                s_calendar.get(Calendar.YEAR),
                s_calendar.get(Calendar.MONTH),
                s_calendar.get(Calendar.DAY_OF_MONTH),
                s_calendar.get(Calendar.HOUR_OF_DAY),
                s_calendar.get(Calendar.MINUTE),
                s_calendar.get(Calendar.SECOND),
                s_calendar.get(Calendar.MILLISECOND),
                logRecord.getThreadID() );
        sb.append( str );

        sb.append( logRecord.getSourceClassName() );
        sb.append( "." );
        sb.append( logRecord.getSourceMethodName() );
        sb.append( " " );

        sb.append( logRecord.getMessage() );
        sb.append( "\n" );

        return sb.toString();
    }
}
