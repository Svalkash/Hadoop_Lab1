package shlabs.lab1;

/**
 * Тип счётчика для подсчета неверных входных данных:
 *  MALFORMED - неверный формат строки
 *  METRICNF - указан несуществующий номер metricID.
 */
public enum CounterType {
    MALFORMED,
    METRICNF
}
