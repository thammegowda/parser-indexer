package edu.usc.cs.ir.cwork.util;

import org.apache.commons.math3.util.Pair;

import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;

/**
 * this generator generates a range of dates for months sequentially from
 * given year and month
 * @author Thamme Gowda N
 */
public class MonthRangeGenerator implements Iterator<Pair<Date, Date>> {

    private Calendar startCalendar;
    private Calendar endCalendar;

    /**
     * Creates a generator with given year and month
     * @param startYear the start year
     * @param startMonth the end year
     */
    public MonthRangeGenerator(int startYear, int startMonth) {
        startCalendar = Calendar.getInstance();
        endCalendar = Calendar.getInstance();
        startCalendar.set(startYear, startMonth, 1, 0, 0, 0);
        endCalendar.set(startYear, startMonth, 1, 23, 59, 59);
        endCalendar.set(Calendar.DATE, endCalendar.getActualMaximum(Calendar.DATE));
    }

    @Override
    public boolean hasNext() {
        return true;
    }

    /**
     *
     * @return pair of month start and month end
     */
    @Override
    public Pair<Date, Date> next() {
        Date start = startCalendar.getTime();
        Date end = endCalendar.getTime();

        //for the next one;
        startCalendar.add(Calendar.MONTH, 1); //next month
        endCalendar.add(Calendar.MONTH, 1);
        endCalendar.set(Calendar.DATE, endCalendar.getActualMaximum(Calendar.DATE));
        return new Pair<>(start, end);
    }
}
