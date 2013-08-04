package org.jamel.dbf.processor;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.jamel.dbf.DbfReader;
import org.jamel.dbf.exception.DbfException;
import org.jamel.dbf.structure.DbfField;
import org.jamel.dbf.structure.DbfHeader;
import org.jamel.dbf.utils.StringUtils;

/**
 * @author Sergey Polovko
 */
public final class DbfProcessor {

    private DbfProcessor() {
    }

    public static <T> List<T> loadData(File dbf, DbfRowMapper<T> rowMapper) throws DbfException {
        DbfReader reader = new DbfReader(dbf);

        try {
            List<T> result = new ArrayList<>(reader.getRecordCount());
            Object[] row;
            while ((row = reader.nextRecord()) != null) {
                result.add(rowMapper.mapRow(row));
            }

            return result;
        } finally {
            reader.close();
        }
    }

    public static void processDbf(File dbf, DbfRowProcessor rowProcessor) throws DbfException {
        DbfReader reader = new DbfReader(dbf);

        try {
            Object[] row;
            while ((row = reader.nextRecord()) != null) {
                rowProcessor.processRow(row);
            }
        } finally {
            reader.close();
        }
    }

    public static void writeToTxtFile(File dbf, File txt, Charset dbfEncoding) throws DbfException {
        DbfReader reader = new DbfReader(dbf);
        PrintWriter writer = null;

        try {
            DbfHeader header = reader.getHeader();

            String[] titles = new String[header.getFieldsCount()];
            for (int i = 0; i < header.getFieldsCount(); i++) {
                DbfField field = header.getField(i);
                titles[i] = StringUtils.rightPad(field.getName(), field.getFieldLength(), ' ');
            }

            writer = new PrintWriter(new BufferedWriter(new FileWriter(txt)));
            for (String title : titles) writer.print(title);
            writer.println();

            Object[] row;
            while ((row = reader.nextRecord()) != null) {
                for (int i = 0; i < header.getFieldsCount(); i++) {
                    DbfField field = header.getField(i);
                    String value = field.getDataType() == 'C'
                            ? new String((byte[]) row[i], dbfEncoding)
                            : String.valueOf(row[i]);
                    writer.print(StringUtils.rightPad(value, field.getFieldLength(), ' '));
                }
                writer.println();
            }
        } catch (IOException e) {
            throw new DbfException("Cannot write Dbf to text file", e);
        } finally {
            if (writer != null) writer.close();
            reader.close();
        }
    }
}
