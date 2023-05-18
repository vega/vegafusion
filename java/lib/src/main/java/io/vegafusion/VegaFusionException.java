package io.vegafusion;

/**
 * This class represents exceptions specific to VegaFusion
 */
public class VegaFusionException extends RuntimeException {
    /**
     * Constructs a new VegaFusionException with the specified detail message.
     *
     * @param message the detail message. The detail message is saved for
     *                later retrieval by the {@link #getMessage()} method.
     */
    public VegaFusionException(String message) {
        super(message);
    }
}
