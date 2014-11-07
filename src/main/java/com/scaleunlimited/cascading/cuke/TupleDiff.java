package com.scaleunlimited.cascading.cuke;

public class TupleDiff {
    public final String field;
    public final Type type;
    public final String expected;
    public final String actual;

    public TupleDiff(String field, String expected, String actual, Type type) {
        this.field = field;
        this.expected = expected;
        this.actual = actual;
        this.type = type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TupleDiff tupleDiff = (TupleDiff) o;

        if (actual != null ? !actual.equals(tupleDiff.actual) : tupleDiff.actual != null) return false;
        if (expected != null ? !expected.equals(tupleDiff.expected) : tupleDiff.expected != null) return false;
        if (!field.equals(tupleDiff.field)) return false;
        if (type != tupleDiff.type) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = field.hashCode();
        result = 31 * result + (expected != null ? expected.hashCode() : 0);
        result = 31 * result + (actual != null ? actual.hashCode() : 0);
        result = 31 * result + type.hashCode();
        return result;
    }

    @Override public String toString() {
        final StringBuilder sb = new StringBuilder("TupleDiff{");
        sb.append("field='").append(field).append('\'');
        sb.append(", type=").append(type);
        sb.append(", expected='").append(expected).append('\'');
        sb.append(", actual='").append(actual).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public static enum Type {
        NULL_EXPECTED,  // target was null, tuple not null
        MISSING,        // target not null, tuple was null
        NOT_EQUAL,      // target not null, tuple not null, values not equal
        ADDITIONAL,     // field in tuple but not in target
        EXCEPTION
    }
}
