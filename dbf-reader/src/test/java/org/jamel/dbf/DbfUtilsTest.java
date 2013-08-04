package org.jamel.dbf;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;

import org.jamel.dbf.utils.DbfUtils;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.assertFalse;

/**
 * @author Sergey Polovko
 */
public class DbfUtilsTest {

    @Test
    public void readLittleEndianInt() throws Exception {
        byte[] buf = {0x12, 0x34, 0x56, 0x78, (byte) 0x9a, (byte) 0xbc, (byte) 0xde, (byte) 0xff};
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(buf));
        assertEquals(0x78563412, DbfUtils.readLittleEndianInt(in));
        assertEquals(0xffdebc9a, DbfUtils.readLittleEndianInt(in));
    }

    @Test
    public void readLittleEndianShort() throws Exception {
        byte[] buf = {0x12, 0x34, 0x56, 0x78, (byte) 0x9a, (byte) 0xbc, (byte) 0xde, (byte) 0xff};
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(buf));
        assertEquals((short) 0x3412, DbfUtils.readLittleEndianShort(in));
        assertEquals((short) 0x7856, DbfUtils.readLittleEndianShort(in));
        assertEquals((short) 0xbc9a, DbfUtils.readLittleEndianShort(in));
        assertEquals((short) 0xffde, DbfUtils.readLittleEndianShort(in));
    }

    @Test
    public void trimLeftSpaces() throws Exception {
        assertEquals("asdf", new String(DbfUtils.trimLeftSpaces("asdf".getBytes())));
        assertEquals("asd",  new String(DbfUtils.trimLeftSpaces("asd ".getBytes())));
        assertEquals("as",   new String(DbfUtils.trimLeftSpaces("as  ".getBytes())));
        assertEquals("a",    new String(DbfUtils.trimLeftSpaces("a   ".getBytes())));
        assertEquals("",     new String(DbfUtils.trimLeftSpaces("    ".getBytes())));
        assertEquals("",     new String(DbfUtils.trimLeftSpaces("".getBytes())));
    }

    @Test
    public void contains() throws Exception {
        assertTrue(DbfUtils.contains("some?string".getBytes(), (byte) '?'));
        assertFalse(DbfUtils.contains("some".getBytes(), (byte) '?'));
    }

    @Test
    public void parseInt() throws Exception {
        assertEquals(1234, DbfUtils.parseInt("1234".getBytes()));
        assertEquals(123,  DbfUtils.parseInt("0123".getBytes()));
        assertEquals(123,  DbfUtils.parseInt("123 ".getBytes()));
        assertEquals(12,   DbfUtils.parseInt("12  ".getBytes()));
        assertEquals(0,    DbfUtils.parseInt("    ".getBytes()));
        assertEquals(0,    DbfUtils.parseInt("".getBytes()));

        assertEquals(3456,       DbfUtils.parseInt("1234567890".getBytes(), 2, 6));
        assertEquals(1234567890, DbfUtils.parseInt("1234567890".getBytes(), 0, 1000));
    }

    @Test
    public void parseLong() throws Exception {
        assertEquals(1234, DbfUtils.parseLong("1234".getBytes()));
        assertEquals(123,  DbfUtils.parseLong("0123".getBytes()));
        assertEquals(123,  DbfUtils.parseLong("123 ".getBytes()));
        assertEquals(12,   DbfUtils.parseLong("12  ".getBytes()));
        assertEquals(0,    DbfUtils.parseLong("    ".getBytes()));
        assertEquals(0,    DbfUtils.parseLong("".getBytes()));

        assertEquals(3456,       DbfUtils.parseLong("1234567890".getBytes(), 2, 6));
        assertEquals(1234567890, DbfUtils.parseLong("1234567890".getBytes(), 0, 1000));
    }
}
