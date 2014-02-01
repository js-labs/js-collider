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

import java.util.Calendar;
import java.util.Date;
import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.LogRecord;

public class LogFormatter extends Formatter
{
    private static final String s_packageName = getPackageName();

    private static String getPackageName()
    {
        Class cls = LogFormatter.class;
        String canonicalName = cls.getCanonicalName();
        String simpleName = cls.getSimpleName();
        int length = (canonicalName.length() - simpleName.length());
        return canonicalName.substring( 0, length );
    }

    public String format( LogRecord logRecord )
    {
        StringBuilder sb = new StringBuilder();
        Calendar calendar = Calendar.getInstance();

        calendar.setTime( new Date(logRecord.getMillis()) );
        String str = String.format( "%04d-%02d-%02d %02d:%02d:%02d.%03d t@%02d ",
                calendar.get(Calendar.YEAR),
                calendar.get(Calendar.MONTH),
                calendar.get(Calendar.DAY_OF_MONTH),
                calendar.get(Calendar.HOUR_OF_DAY),
                calendar.get(Calendar.MINUTE),
                calendar.get(Calendar.SECOND),
                calendar.get(Calendar.MILLISECOND),
                logRecord.getThreadID() );
        sb.append( str );

        if (logRecord.getLevel().intValue() >= Level.CONFIG.intValue())
        {
            sb.append( logRecord.getLevel().getName().toUpperCase() );
            sb.append( " " );
        }

        String className = logRecord.getSourceClassName();
        if (className.startsWith(s_packageName))
            sb.append( className.substring(s_packageName.length()) );

        sb.append( "." );
        sb.append( logRecord.getSourceMethodName() );

        String msg = logRecord.getMessage();
        if (msg.length() > 0)
        {
            sb.append( ": " );
            sb.append( logRecord.getMessage() );
        }

        sb.append( "\n" );
        return sb.toString();
    }
}
