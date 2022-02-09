/*
 * Copyright (C) 2022 Sergey Zubarev, info@js-labs.org
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

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;

public class LogFormatter extends Formatter
{
    private static String getPackageName()
    {
        Class cls = LogFormatter.class;
        String canonicalName = cls.getCanonicalName();
        String simpleName = cls.getSimpleName();
        int length = (canonicalName.length() - simpleName.length());
        return canonicalName.substring( 0, length );
    }

    public String format(LogRecord logRecord)
    {
        final StringBuilder sb = new StringBuilder();
        final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        final Date date = new Date(logRecord.getMillis());
        sb.append(simpleDateFormat.format(date));
        sb.append("t@");
        sb.append(logRecord.getThreadID());
        sb.append(' ');
        sb.append(logRecord.getLevel().getName());
        sb.append(' ');

        final String className = logRecord.getSourceClassName();
        final int idx = className.lastIndexOf('.');
        if (idx < 0)
            sb.append(className);
        else
            sb.append(className.substring(idx+1));

        sb.append('.');
        sb.append(logRecord.getSourceMethodName());

        final String msg = logRecord.getMessage();
        if (msg.length() > 0)
        {
            sb.append(": ");
            sb.append(logRecord.getMessage());
        }

        sb.append('\n');
        return sb.toString();
    }
}
