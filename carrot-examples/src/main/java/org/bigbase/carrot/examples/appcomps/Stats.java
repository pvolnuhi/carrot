/**
 *    Copyright (C) 2021-present Carrot, Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the Server Side Public License, version 1,
 *    as published by MongoDB, Inc.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    Server Side Public License for more details.
 *
 *    You should have received a copy of the Server Side Public License
 *    along with this program. If not, see
 *    <http://www.mongodb.com/licensing/server-side-public-license>.
 */

package org.bigbase.carrot.examples.appcomps;

import java.util.Calendar;
import java.util.Properties;
import java.util.Random;

import org.bigbase.carrot.Key;
import org.bigbase.carrot.examples.util.KeyValues;
import org.bigbase.carrot.util.Bytes;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;

public class Stats extends KeyValues {

  final static String MIN = "min";
  final static String MAX = "max";
  final static String COUNT = "count";
  final static String SUM = "sum";
  final static String SUMSQ = "sumsq";
  
  long hourStartTime;
  
  protected Stats(Properties p) {
    super(p);
  }
  
  private static long getHourStartTime(int hoursBefore) {
    Calendar cal = Calendar.getInstance();
    int year = cal.get(Calendar.YEAR);
    int month = cal.get(Calendar.MONTH);
    int day = cal.get(Calendar.DAY_OF_MONTH);
    int hour = cal.get(Calendar.HOUR_OF_DAY);
    cal.set (year, month, day, hour, 0);
    long time0 = cal.getTimeInMillis();
    long time = time0 - hoursBefore * 3600 * 1000;
    return time;
  }
  
  public static Stats newStats(int hoursBefore) {
    Properties p = new Properties();
    Random r = new Random();
    p.setProperty(MIN, Integer.toString(r.nextInt(100)));
    p.setProperty(MAX, Integer.toString(r.nextInt(100) + 100));
    p.setProperty(COUNT, Long.toString(r.nextInt(10000)));
    p.setProperty(SUM, Long.toString(r.nextInt(1000000)));
    p.setProperty(SUMSQ, Long.toString(Math.abs(r.nextLong())));
    Stats st = new Stats(p);
    st.hourStartTime = getHourStartTime(hoursBefore);;
    return st;
  }
  
  @Override
  public String getKey() {
    return "stats:profilepage:accesstime:";
  }

  @Override
  public Key getKeyNative() {
    String key = getKey();
    long ptr = UnsafeAccess.allocAndCopy(key, 0, key.length() + Utils.SIZEOF_LONG);
    UnsafeAccess.putLong(ptr + key.length(), Long.MAX_VALUE - hourStartTime);
    return new Key(ptr, key.length() + Utils.SIZEOF_LONG);
  }
  
  @Override
  public byte[] getKeyBytes() {
    return Bytes.add(getKey().getBytes(), Bytes.toBytes(Long.MAX_VALUE - hourStartTime));
  }
}
