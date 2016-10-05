package org.jamel.dbf;

import org.jamel.dbf.structure.DbfDataType;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNull;

public class MemoTest {

    @Test
    public void testMemo4() {
        DbfReader reader = new DbfReader(getClass().getResourceAsStream("memo/vfp3-memo4.DBF"));
        int rowsCount = 20;
        int memoFieldIndex = 25;
        int[] memoLinkValues = new int[]{8, 11, 17, 30, 32, 35, 38, 40, 45, 48, 51, 55, 55, 55, 55, 55, 218, 63, 185, 200};

        assertEquals("headers.count", 39, reader.getHeader().getFieldsCount());
        assertEquals("records.count", rowsCount, reader.getHeader().getNumberOfRecords());
        assertEquals("header[].type", DbfDataType.MEMO, reader.getHeader().getField(memoFieldIndex).getDataType());
        for (int rowIndex = 0; rowIndex < rowsCount; rowIndex++) {
            Object[] row = reader.nextRecord();
            Number memoLink = (Number) row[memoFieldIndex];
            assertEquals("memo-" + rowIndex, memoLinkValues[rowIndex], memoLink.intValue());
        }
        assertNull("end", reader.nextRecord());
    }
}
