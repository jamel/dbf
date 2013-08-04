package org.jamel.dbf.processor;

/**
 * Map raw data from dbf row to object type T.
 * @param <T>
 *
 * @author Sergey Polovko
 */
public interface DbfRowMapper<T> {

    T mapRow(Object[] row);

}
