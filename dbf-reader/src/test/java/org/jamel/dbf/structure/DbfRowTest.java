package org.jamel.dbf.structure;

import org.jamel.dbf.exception.DbfException;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.Date;

import static java.nio.charset.Charset.defaultCharset;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DbfRowTest {

    private static final String COLUMN_1 = "column 1";
    private static final String COLUMN_2 = "column 2";
    private static final String UNKNOWN_COLUMN = "unknown column";

    private DbfHeader header = mock(DbfHeader.class);

    @Before
    public void setUp() {
        when(header.getFieldIndex(COLUMN_1))
                .thenReturn(0);
        when(header.getFieldIndex(COLUMN_2))
                .thenReturn(1);
        when(header.getFieldIndex(UNKNOWN_COLUMN))
                .thenReturn(-1);
    }

    @Test
    public void getBigDecimal() {
        final BigDecimal value = BigDecimal.valueOf(1000.0009);
        DbfRow dbfRow = createRow(value, null);

        assertEquals(value, dbfRow.getBigDecimal(COLUMN_1));
        assertNull(dbfRow.getBigDecimal(COLUMN_2));
    }

    @Test
    public void getDate() {
        final Date value = new Date();
        DbfRow dbfRow = createRow(null, value);

        assertNull(dbfRow.getDate(COLUMN_1));
        assertEquals(value, dbfRow.getDate(COLUMN_2));
    }

    @Test
    public void getString() {
        final String value = "String value";
        DbfRow dbfRow = createRow(null, value.getBytes());

        assertNull(dbfRow.getString(COLUMN_1));
        assertEquals(value, dbfRow.getString(COLUMN_2));
    }

    @Test
    public void getBoolean() {
        when(header.getFieldIndex("column 3"))
                .thenReturn(2);

        DbfRow dbfRow = createRow(true, false, null);

        assertEquals(true, dbfRow.getBoolean(COLUMN_1));
        assertEquals(false, dbfRow.getBoolean(COLUMN_2));
        assertEquals(false, dbfRow.getBoolean("column 3"));
    }

    @Test
    public void getInt() {
        DbfRow dbfRow = createRow(100, null);

        assertEquals(100, dbfRow.getInt(COLUMN_1));
        assertEquals(0, dbfRow.getInt(COLUMN_2));
    }

    @Test
    public void getShort() {
        final short value = 100;
        DbfRow dbfRow = createRow(null, value);

        assertEquals(0, dbfRow.getShort(COLUMN_1));
        assertEquals(value, dbfRow.getShort(COLUMN_2));
    }

    @Test
    public void getByte() {
        byte value = 2;
        DbfRow dbfRow = createRow(null, value);

        assertEquals(0, dbfRow.getByte(COLUMN_1));
        assertEquals(value, dbfRow.getByte(COLUMN_2));
    }

    @Test
    public void getLong() {
        long value = 10000000000L;
        DbfRow dbfRow = createRow(null, value);

        assertEquals(0, dbfRow.getLong(COLUMN_1));
        assertEquals(value, dbfRow.getLong(COLUMN_2));
    }

    @Test
    public void getFloat() {
        float value = 10.004f;
        DbfRow dbfRow = createRow(null, value);

        assertEquals(0, dbfRow.getFloat(COLUMN_1), 0.5);
        assertEquals(value, dbfRow.getFloat(COLUMN_2), 0.5);
    }

    @Test
    public void getDouble() {
        double value = 10.004;
        DbfRow dbfRow = createRow(null, value);

        assertEquals(0, dbfRow.getDouble(COLUMN_1), 0.5);
        assertEquals(value, dbfRow.getDouble(COLUMN_2), 0.5);
    }

    @Test
    public void getObject() {
        Object value = new Object();
        DbfRow dbfRow = createRow(null, value);

        assertNull(dbfRow.getObject(COLUMN_1));
        assertEquals(value, dbfRow.getObject(COLUMN_2));
    }

    @Test(expected = DbfException.class)
    public void fieldDoesNotExist() throws DbfException {
        createRow().getObject(UNKNOWN_COLUMN);
    }

    private DbfRow createRow(Object... row) {
        return new DbfRow(header, defaultCharset(), row);
    }

}