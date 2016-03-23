package org.jamel.dbf;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.util.Date;
import java.util.GregorianCalendar;

import org.jamel.dbf.exception.DbfException;
import org.jamel.dbf.structure.DbfField;
import org.jamel.dbf.structure.DbfHeader;
import org.jamel.dbf.utils.DbfUtils;

/**
 * Dbf reader.
 * This class is not thread safe.
 *
 * @author Sergey Polovko
 * @see <a href="http://www.fship.com/dbfspecs.txt">DBF specification</a>
 */
public class DbfReader implements Closeable {
    protected final byte DATA_ENDED = 0x1A;
    protected final byte DATA_DELETED = 0x2A;

    private DataInput dataInput;
    private final DbfHeader header;

    public DbfReader(File file) throws DbfException {
        try {
            dataInput = new RandomAccessFile(file, "r");
            header = DbfHeader.read(dataInput);
            skipToDataBeginning();
        } catch (IOException e) {
            throw new DbfException("Cannot open Dbf file " + file, e);
        }
    }

    public DbfReader(InputStream in) throws DbfException {
        try {
            dataInput = new DataInputStream(new BufferedInputStream(in));
            header = DbfHeader.read(dataInput);
            skipToDataBeginning();
        } catch (IOException e) {
            throw new DbfException("Cannot read Dbf", e);
        }
    }

    private void skipToDataBeginning() throws IOException {
        // it might be required to jump to the start of records at times
        int dataStartIndex = header.getHeaderLength() - 32 * (header.getFieldsCount() + 1) - 1;
        if (dataStartIndex > 0) {
            dataInput.skipBytes(dataStartIndex);
        }
    }

    /**
     * @return {@code true} if the reader can seek forward or backward to a specified record index,
     * {@code false} otherwise.
     */
    public boolean canSeek() {
        return dataInput instanceof RandomAccessFile;
    }

    /**
     * Attempt to seek to a specified record index. If successful the record can be read
     * by calling {@link DbfReader#nextRecord()}.
     *
     * @param n The zero-based record index.
     */
    public void seekToRecord(int n) {
        if (!canSeek()) {
            throw new DbfException("Seeking is not supported.");
        }
        if (n < 0 || n >= header.getNumberOfRecords()) {
            throw new DbfException(String.format("Record index out of range [0, %d]: %d",
                    header.getNumberOfRecords(), n));
        }
        long position = header.getHeaderLength() + n * header.getRecordLength();
        try {
            ((RandomAccessFile) dataInput).seek(position);
        } catch (IOException e) {
            throw new DbfException(
                    String.format("Failed to seek to record %d of %d", n, header.getNumberOfRecords()), e);
        }
    }

    /**
     * Reads and returns the next row in the Dbf stream
     *
     * @return The next row as an Object array.
     */
    public Object[] nextRecord() {
        try {
            int nextByte;
            do {
                nextByte = dataInput.readByte();
                if (nextByte == DATA_ENDED) {
                    return null;
                } else if (nextByte == DATA_DELETED) {
                    dataInput.skipBytes(header.getRecordLength() - 1);
                }
            } while (nextByte == DATA_DELETED);

            Object recordObjects[] = new Object[header.getFieldsCount()];
            for (int i = 0; i < header.getFieldsCount(); i++) {
                recordObjects[i] = readFieldValue(header.getField(i));
            }
            return recordObjects;
        } catch (EOFException e) {
            return null; // we currently end reading file
        } catch (IOException e) {
            throw new DbfException("Cannot read next record form Dbf file", e);
        }
    }

    private Object readFieldValue(DbfField field) throws IOException {
        byte buf[] = new byte[field.getFieldLength()];
        dataInput.readFully(buf);

        switch (field.getDataType()) {
            case CHAR: return readCharacterValue(field, buf);
            case DATE: return readDateValue(field, buf);
            case FLOAT: return readFloatValue(field, buf);
            case LOGICAL: return readLogicalValue(field, buf);
            case NUMERIC: return readNumericValue(field, buf);
            case INTEGER: return readIntegerValue(field, buf);
            default:  return null;
        }
    }

    protected Object readCharacterValue(DbfField field, byte[] buf) throws IOException {
        return buf;
    }

    protected Date readDateValue(DbfField field, byte[] buf) throws IOException {
        int year = DbfUtils.parseInt(buf, 0, 4);
        int month = DbfUtils.parseInt(buf, 4, 6);
        int day = DbfUtils.parseInt(buf, 6, 8);
        return new GregorianCalendar(year, month - 1, day).getTime();
    }

    protected Float readFloatValue(DbfField field, byte[] buf) throws IOException {
        try {
            byte[] floatBuf = DbfUtils.trimLeftSpaces(buf);
            boolean processable = (floatBuf.length > 0 && !DbfUtils.contains(floatBuf, (byte) '?'));
            return processable ? Float.valueOf(new String(floatBuf)) : null;
        } catch (NumberFormatException e) {
            throw new DbfException("Failed to parse Float from " + field.getName(), e);
        }
    }

    protected Boolean readLogicalValue(DbfField field, byte[] buf) throws IOException {
        boolean isTrue = (buf[0] == 'Y' || buf[0] == 'y' || buf[0] == 'T' || buf[0] == 't');
        return isTrue ? Boolean.TRUE : Boolean.FALSE;
    }

    protected Number readNumericValue(DbfField field, byte[] buf) throws IOException {
        try {
            byte[] numericBuf = DbfUtils.trimLeftSpaces(buf);
            boolean processable = numericBuf.length > 0 && !DbfUtils.contains(numericBuf, (byte) '?');
            return processable ? Double.valueOf(new String(numericBuf)) : null;
        } catch (NumberFormatException e) {
            throw new DbfException("Failed to parse Number from " + field.getName(), e);
        }
    }

    protected Integer readIntegerValue(DbfField field, byte[] buf) throws IOException {
        try {
        	if (buf.length != 4) throw new DbfException("Failed to parse Integer from " + field.getName());
        	int x = java.nio.ByteBuffer.wrap(buf).order(java.nio.ByteOrder.LITTLE_ENDIAN).getInt();
            return x;
        } catch (NumberFormatException e) {
            throw new DbfException("Failed to parse Integer from " + field.getName(), e);
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
    public void close() {
        try {
            // this method should be idempotent
            if (dataInput instanceof Closeable) {
                ((Closeable) dataInput).close();
                dataInput = null;
            }
        } catch (IOException e) {
            // ignore
        }
    }
}
