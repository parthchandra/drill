/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.drill.exec.util;

import org.joda.time.Days;
import org.joda.time.Hours;
import org.joda.time.Interval;
import org.joda.time.Minutes;
import org.joda.time.Months;
import org.joda.time.MutableDateTime;
import org.joda.time.Seconds;
import org.joda.time.Weeks;
import org.joda.time.Years;

public enum TSI {
      SQL_TSI_FRAC_SECOND {
            @Override
            public void addCount(MutableDateTime dateTime, int count) {
                  dateTime.addMillis(count);
            }

            @Override
            public long getDiff(Interval interval) {
                  return interval.getEndMillis() - interval.getStartMillis();
            }
      },
      SQL_TSI_SECOND {
            @Override
            public void addCount(MutableDateTime dateTime, int count) {
                  dateTime.addSeconds(count);
            }

            @Override
            public long getDiff(Interval interval) {
                  return Seconds.secondsIn(interval).getSeconds();
            }
      },
      SQL_TSI_MINUTE {
            @Override
            public void addCount(MutableDateTime dateTime, int count) {
                  dateTime.addMinutes(count);
            }

            @Override
            public long getDiff(Interval interval) {
                  return Minutes.minutesIn(interval).getMinutes();
            }
      },
      SQL_TSI_HOUR {
            @Override
            public void addCount(MutableDateTime dateTime, int count) {
                  dateTime.addHours(count);
            }

            @Override
            public long getDiff(Interval interval) {
                  return Hours.hoursIn(interval).getHours();
            }
      },
      SQL_TSI_DAY {
            @Override
            public void addCount(MutableDateTime dateTime, int count) {
                  dateTime.addDays(count);
            }

            @Override
            public long getDiff(Interval interval) {
                  return Days.daysIn(interval).getDays();
            }
      },
      SQL_TSI_WEEK {
            @Override
            public void addCount(MutableDateTime dateTime, int count) {
                  dateTime.addWeeks(count);
            }

            @Override
            public long getDiff(Interval interval) {
                  return Weeks.weeksIn(interval).getWeeks();
            }
      },
      SQL_TSI_MONTH {
            @Override
            public void addCount(MutableDateTime dateTime, int count) {
                  dateTime.addMonths(count);
            }

            @Override
            public long getDiff(Interval interval) {
                  return Months.monthsIn(interval).getMonths();
            }
      },
      SQL_TSI_QUARTER {
            @Override
            public void addCount(MutableDateTime dateTime, int count) {
                  dateTime.addMonths(4 * count);
            }

            @Override
            public long getDiff(Interval interval) {
                  return Months.monthsIn(interval).getMonths() / 4;
            }
      },
      SQL_TSI_YEAR {
            @Override
            public void addCount(MutableDateTime dateTime, int count) {
                  dateTime.addYears(count);
            }

            @Override
            public long getDiff(Interval interval) {
                  return Years.yearsIn(interval).getYears();
            }
      };

      public abstract void addCount(MutableDateTime dateTime, int count);

      public abstract long getDiff(Interval interval);

    }