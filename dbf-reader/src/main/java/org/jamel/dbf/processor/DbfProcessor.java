package org.jamel.dbf.processor;

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import org.jamel.dbf.DbfReader;
import org.jamel.dbf.exception.DbfException;
import org.jamel.dbf.structure.DbfDataType;
import org.jamel.dbf.structure.DbfField;
import org.jamel.dbf.structure.DbfHeader;
import org.jamel.dbf.utils.StringUtils;

import static org.jamel.dbf.utils.StringUtils.rightPad;

/**
 * @author Sergey Polovko
 */
public final class DbfProcessor {

    private static final int INDEX_WIDTH = 4;
    private static final int NAME_WIDTH = 16;
    private static final int TYPE_WIDTH = 8;
    private static final int LENGTH_WIDTH = 8;
    private static final int DECIMAL_WIDTH = 8;


    private DbfProcessor() {
    }
    
    /**
     * 
     * @param <T> Output type
     * @param inputStream Input stream
     * @param rowMapper Row mapper
     * @return Mapped rows
     * @throws DbfException 
     */
    public static <T> List<T> loadData(InputStream inputStream, DbfRowMapper<T> rowMapper) throws DbfException
    {
         try (DbfReader reader = new DbfReader(inputStream)) {
            List<T> result = new ArrayList<>(reader.getRecordCount());
            Object[] row;
            while ((row = reader.nextRecord()) != null) {
                result.add(rowMapper.mapRow(row));
            }

            return result;
        }
    }

    public static <T> List<T> loadData(File dbf, DbfRowMapper<T> rowMapper) throws DbfException {
        try (DbfReader reader = new DbfReader(dbf)) {
            List<T> result = new ArrayList<>(reader.getRecordCount());
            Object[] row;
            while ((row = reader.nextRecord()) != null) {
                result.add(rowMapper.mapRow(row));
            }

            return result;
        }
    }

    public static void processDbf(File dbf, DbfRowProcessor rowProcessor) throws DbfException {
        try (DbfReader reader = new DbfReader(dbf)) {
            Object[] row;
            while ((row = reader.nextRecord()) != null) {
                rowProcessor.processRow(row);
            }
        }
    }

    public static void writeToTxtFile(File dbf, File txt, Charset dbfEncoding) {
        try (
                DbfReader reader = new DbfReader(dbf);
                PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(txt)))
            )
        {
            DbfHeader header = reader.getHeader();

            String[] titles = new String[header.getFieldsCount()];
            for (int i = 0; i < header.getFieldsCount(); i++) {
                DbfField field = header.getField(i);
                titles[i] = StringUtils.rightPad(field.getName(), field.getFieldLength(), ' ');
            }

            for (String title : titles) writer.print(title);
            writer.println();

            Object[] row;
            while ((row = reader.nextRecord()) != null) {
                for (int i = 0; i < header.getFieldsCount(); i++) {
                    DbfField field = header.getField(i);
                    String value = field.getDataType() == DbfDataType.CHAR
                            ? new String((byte[]) row[i], dbfEncoding)
                            : String.valueOf(row[i]);
                    writer.print(StringUtils.rightPad(value, field.getFieldLength(), ' '));
                }
                writer.println();
            }
        } catch (IOException e) {
            throw new DbfException("Cannot write .dbf file to .txt", e);
        }
    }

    /**
     * Create string with dbf information:
     *   - creation date
     *   - total records count
     *   - columns info
     * @param dbf  .dbf file
     * @return  string with dbf information
     */
    public static String readDbfInfo(File dbf) {
        try (DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(dbf)))) {
            DbfHeader header = DbfHeader.read(in);
            StringBuilder sb = new StringBuilder(512);
            sb.append("Created at: ")
                    .append(header.getYear()).append('-').append(header.getMonth())
                    .append('-').append(header.getDay()).append('\n')
                    .append("Total records: ").append(header.getNumberOfRecords()).append('\n')
                    .append("Header length: ").append(header.getHeaderLength()).append('\n')
                    .append("Columns: ").append('\n');

            sb.append("  ").append(rightPad("#", INDEX_WIDTH, ' '))
                    .append(rightPad("Name", NAME_WIDTH, ' '))
                    .append(rightPad("Type", TYPE_WIDTH, ' '))
                    .append(rightPad("Length", LENGTH_WIDTH, ' '))
                    .append(rightPad("Decimal", DECIMAL_WIDTH, ' '))
                    .append('\n');

            int totalWidth = INDEX_WIDTH + NAME_WIDTH + TYPE_WIDTH + LENGTH_WIDTH + DECIMAL_WIDTH + 2;
            for (int i = 0; i < totalWidth; i++) sb.append('-');

            for (int i = 0; i < header.getFieldsCount(); i++) {
                DbfField field = header.getField(i);
                sb.append('\n')
                        .append("  ").append(rightPad(String.valueOf(i), INDEX_WIDTH, ' '))
                        .append(rightPad(field.getName(), NAME_WIDTH, ' '))
                        .append(rightPad(String.valueOf((char) field.getDataType().byteValue), TYPE_WIDTH, ' '))
                        .append(rightPad(String.valueOf(field.getFieldLength()), LENGTH_WIDTH, ' '))
                        .append(rightPad(String.valueOf(field.getDecimalCount()), DECIMAL_WIDTH, ' '));
            }

            return sb.toString();
        } catch (IOException e) {
            throw new DbfException("Cannot read header of .dbf file " + dbf, e);
        }
    }
}
