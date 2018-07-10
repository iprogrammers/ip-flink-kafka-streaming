/*
 * 
 */
package com.iprogrammer.streaming.model.response;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.io.Serializable;
import java.util.Collection;


/**
 * The Class Response.
 */
@Data
public class Response implements Serializable{

    /**
     * The payload.
     */
    @JsonProperty("oBody")
    private Payload<?> payload;

    /**
     * The status.
     */
    @JsonProperty("status")
    private Status status;

    /**
     * The errors.
     */
    @JsonProperty("errors")
    private Collection<Error> errors;

    @JsonProperty("error")
    private Error error;

    /**
     * The Class Builder.
     */
    public static class Builder {

        /**
         * The base response.
         */
        private Response baseResponse = new Response();

        /**
         * Builds the.
         *
         * @return the response
         */
        public Response build() {
            return this.baseResponse;
        }

        /**
         * Payload.
         *
         * @param payload the payload
         * @return the builder
         */
        public Builder payload(Payload<?> payload) {
            this.baseResponse.setPayload(payload);
            return this;
        }

        /**
         * Errors.
         *
         * @param errors the errors
         * @return the builder
         */
        public Builder errors(Collection<Error> errors) {
            this.baseResponse.setErrors(errors);
            return this;
        }

        /**
         * Status.
         *
         * @param status the status
         * @return the builder
         */
        public Builder status(Status status) {
            this.baseResponse.setStatus(status);
            return this;
        }


        public Builder error(Error error) {
            this.baseResponse.setError(error);
            return this;

        }

    }
}
