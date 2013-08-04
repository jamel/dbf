package org.jamel.dbf;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.GregorianCalendar;

import org.jamel.dbf.exception.DbfException;
import org.jamel.dbf.structure.DbfField;
import org.jamel.dbf.structure.DbfHeader;
import org.jamel.dbf.utils.DbfUtils;

/**
 * Dbf file reader.
 * This class is not thread safe.
 *
 * @author Sergey Polovko
 * @see <a href="http://www.fship.com/dbfspecs.txt">DBF specification</a>
 */
public class DbfReader {

    protected final int END_OF_DATA = 0x1A;

    private final DataInputStream dataInputStream;
    private final DbfHeader header;

    private boolean isClosed = true;


    /**
     * <p>Initializes a DbfReader object.</p>
     *
     * <p>When this constructor returns the object will have completed reading the header (meta date) and
     * header information can be queried there on. And it will be ready to return the first row.</p>
     *
     * @param file where the data is read from.
     */
    public DbfReader(File file) {
        try {
            dataInputStream = new DataInputStream(new BufferedInputStream(new FileInputStream(file)));
            isClosed = false;
            header = DbfHeader.read(dataInputStream);

			// it might be required to jump to the start of records at times
            int dataStartIndex = header.getHeaderLength() - 32 * (header.getFieldsCount() + 1) - 1;
            if (dataStartIndex > 0) {
                dataInputStream.skip(dataStartIndex);
            }
        } catch (IOException e) {
            throw new DbfException("Cannot open Dbf file " + file, e);
        }
    }

    /**
     * Reads and returns the next row in the Dbf stream.
     *
     * @return The next row as an Object array. Types of the elements these arrays
     *         follow the convention mentioned in the class description.
     */
    public Object[] nextRecord() {
        if (isClosed) {
            throw new IllegalStateException("Source is not open");
        }

        Object recordObjects[] = new Object[header.getFieldsCount()];
        try {
            boolean isDeleted = false;
            do {
                if (isDeleted) {
                    dataInputStream.skip(header.getRecordLength() - 1);
                }

                int nextByte = dataInputStream.readByte();
                if (nextByte == END_OF_DATA) {
                    return null;
                }

                isDeleted = (nextByte == '*');
            } while (isDeleted);

            for (int i = 0; i < header.getFieldsCount(); i++) {
                recordObjects[i] = readFieldValue(header.getField(i));
            }
        } catch (EOFException e) {
            return null; // we currently end reading file
        } catch (IOException e) {
            throw new DbfException("Cannot read next record form Dbf file", e);
        }

        return recordObjects;
    }

    private Object readFieldValue(DbfField field) throws IOException {
        switch (field.getDataType()) {
            case 'C':
                byte buf[] = new byte[field.getFieldLength()];
                dataInputStream.read(buf);
                return buf;

            case 'D':
                byte dateBuf[] = new byte[4 + 2 + 2];
                dataInputStream.read(dateBuf);

                int year = DbfUtils.parseInt(dateBuf, 0, 4);
                int month = DbfUtils.parseInt(dateBuf, 4, 6);
                int day = DbfUtils.parseInt(dateBuf, 6, 8);

                return new GregorianCalendar(year, month - 1, day).getTime();

            case 'F':
                try {
                    byte floatBuf[] = new byte[field.getFieldLength()];
                    dataInputStream.read(floatBuf);
                    floatBuf = DbfUtils.trimLeftSpaces(floatBuf);
                    if (floatBuf.length > 0 && !DbfUtils.contains(floatBuf, (byte) '?')) {
                        return Float.valueOf(new String(floatBuf));
                    } else {
                        return null;
                    }
                } catch (NumberFormatException e) {
                    throw new DbfException("Failed to parse Float from " + field.getName(), e);
                }

            case 'N':
                try {
                    byte numericBuf[] = new byte[field.getFieldLength()];
                    dataInputStream.read(numericBuf);
                    numericBuf = DbfUtils.trimLeftSpaces(numericBuf);

                    if (numericBuf.length > 0 && !DbfUtils.contains(numericBuf, (byte) '?')) {
                        return Double.valueOf(new String(numericBuf));
                    } else {
                        return null;
                    }
                } catch (NumberFormatException e) {
                    throw new DbfException("Failed to parse Number from " + field.getName(), e);
                }

            case 'L':
                byte logicalByte = dataInputStream.readByte();
                if (logicalByte == 'Y' || logicalByte == 'y' || logicalByte == 'T' || logicalByte == 't') {
                    return Boolean.TRUE;
                } else {
                    return Boolean.FALSE;
                }

            default:
                return null;
        }
    }

    /**
     * @return the number of records in the Dbf.
     */
    public int getRecordCount() {
        return header.getNumberOfRecords();
    }

    /**
     * @return Dbf header info.
     */
    public DbfHeader getHeader() {
        return header;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(256);
        sb.append(header.getYear()).append('-').append(header.getMonth()).append('-').append(header.getDay()).append('\n')
                .append("Total records: ").append(getRecordCount()).append('\n')
                .append("Header length: ").append(header.getHeaderLength()).append('\n');

        for (int i = 0; i < header.getFieldsCount(); i++) {
            sb.append(header.getField(i).getName()).append('\n');
        }

        return sb.toString();
    }

    public void close() {
        try {
            dataInputStream.close();
        } catch (IOException e) {
            // ignore
        }
    }
}
