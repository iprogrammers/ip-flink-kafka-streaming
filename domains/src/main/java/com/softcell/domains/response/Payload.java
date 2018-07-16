/*
 * 
 */
package com.softcell.domains.response;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.ToString;
import org.springframework.util.Assert;

import java.io.Serializable;

// TODO: Auto-generated Javadoc

/**
 * The Class Payload.
 *
 * @param <T> the generic type
 * @author kishorp
 */

@ToString
public class Payload<T> implements Serializable {

    /**
     * The t.
     */
    @JsonProperty("payload")
    private T t;

    /**
     * Instantiates a new payload.
     *
     * @param body the body
     */
    public Payload(T body) {
        Assert.notNull(body);
        this.t = body;
    }

    /**
     * Gets the t.
     *
     * @return the t
     */
    public T getT() {
        return t;
    }

    /**
     * Sets the t.
     *
     * @param t the new t
     */
    public void setT(T t) {
        this.t = t;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((t == null) ? 0 : t.hashCode());
        return result;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Payload other = (Payload) obj;
        if (t == null) {
            if (other.t != null)
                return false;
        } else if (!t.equals(other.t))
            return false;
        return true;
    }
}