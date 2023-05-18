package io.vegafusion;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.io.IOException;

/**
 * This class provides a VegaFusion runtime that may be used to perform
 * Vega transform operations.
 */
class VegaFusionRuntime {
    /**
     * Returns the version of the VegaFusion
     *
     * @return A string storing the version of the VegaFusion
     */
    public static native String version();

    /**
     * Creates a native VegaFusionRuntime object with specified capacity
     * and memory limit. This is a native method, implemented in native code.
     *
     * @param capacity The capacity of the runtime cache
     * @param memoryLimit The memory limit for the runtime cache
     * @return A long representing a pointer to the native runtime.
     */
    private static native long innerCreate(int capacity, int memoryLimit);

    /**
     * Destroys a native VegaFusionRuntime object.
     * This is a native method, implemented in native code.
     *
     * @param pointer The pointer to the state to be destroyed.
     */
    private static native void innerDestroy(long pointer);

    /**
     * Patches a previously pre-transformed Vega specification.
     * This is a native method, implemented in native code.
     *
     * @param spec1 Previous input to preTransformSpec
     * @param preTransformedSpec1 Previous result of preTransformSpec
     * @param spec2 New Vega spec that is potentially similar to spec1
     * @return The pre-transformed version of spec2, or null if patching
     *         is not possible.
     */
    private static native String innerPatchPreTransformedSpec(
            String spec1, String preTransformedSpec1, String spec2
    );

    /**
     * Evaluate the transforms in a Vega specification, returning a new
     * specification with transformed data included inline.
     * This is a native method, implemented in native code.
     *
     * @param pointer The pointer to the native VegaFusionRuntime
     * @param spec The Vega specification to be transformed
     * @param localTz The local time zone (defaults to "UTC" if null)
     * @param defaultInputTz The default input time zone (defaults to localTz if null)
     * @param rowLimit The row limit for the inline transformed data.
     *                 If zero then no limit is imposed.
     * @param preserveInteractivity Whether to preserve interactivity in the resulting
     *                              Vega specification
     * @return A PreTransformSpecResult containing the transformed specification and any warnings.
     */
    private static native PreTransformSpecResult innerPreTransformSpec(
            long pointer, String spec, String localTz, String defaultInputTz, int rowLimit, boolean preserveInteractivity
    );

    static {
        String libPath = System.getenv("VEGAFUSION_JNI_LIBRARY");
        if (libPath != null) {
            // Use explicit path to jni library
            System.load(libPath);
        } else {
            // Use library bundled in jar
            try {
                System.loadLibrary("vegafusion_jni");
            } catch (LinkageError e) {
                // Build path based on os and architecture

                String osName = System.getProperty("os.name").toLowerCase();
                String osArch = System.getProperty("os.arch").toLowerCase();

                String libName;
                String directory;
                if  (osName.contains("win")) {
                    libName = "vegafusion_jni.dll";
                    if (osArch.equals("amd64") || osArch.equals("x86_64")) {
                        directory = "win-64";
                    } else {
                        throw new UnsupportedOperationException("Unsupported architecture for Windows: " + osArch);
                    }
                } else if (osName.contains("mac")) {
                    libName = "libvegafusion_jni.dylib";
                    if (osArch.equals("amd64") || osArch.equals("x86_64")) {
                        directory = "osx-64";
                    } else if (osArch.equals("aarch64") || osArch.equals("arm64")) {
                        directory = "osx-arm64";
                    } else {
                        throw new UnsupportedOperationException("Unsupported architecture for macOS: " + osArch);
                    }
                } else if (osName.contains("nix") || osName.contains("nux")) {
                    libName = "libvegafusion_jni.so";
                    if (osArch.equals("amd64") || osArch.equals("x86_64")) {
                        directory = "linux-64";
                    } else {
                        throw new UnsupportedOperationException("Unsupported architecture for Linux: " + osArch);
                    }
                } else {
                    throw new UnsupportedOperationException("Unsupported operating system: " + osName);
                }

                // Path in the jar file to the compiled library
                String libPathInJar = "/native/" + directory + "/" + libName;

                // Extract the library to a temporary file
                InputStream libStream = VegaFusionRuntime.class.getResourceAsStream(libPathInJar);
                if (libStream == null) {
                    throw new RuntimeException("Failed to find " + libPathInJar + " in jar file");
                }

                // Create a temp file and get its path
                try {
                    String tempFileName = Paths.get(libName).getFileName().toString();
                    java.nio.file.Path temp = Files.createTempFile(tempFileName, "");

                    // Copy the library to the temp file
                    Files.copy(libStream, temp, StandardCopyOption.REPLACE_EXISTING);

                    // Load the library
                    System.load(temp.toAbsolutePath().toString());

                    // Schedule the temp file to be deleted on exit
                    temp.toFile().deleteOnExit();
                } catch (IOException ioe) {
                    e.printStackTrace();
                }
            }
        }
    }

    private long state_ptr;

    /**
     * Constructs a new VegaFusionRuntime with the specified cache capacity and memory limit.
     *
     * @param capacity The cache capacity (in number of cache entries)
     * @param memoryLimit The cache memory limit (in bytes)
     */
    public VegaFusionRuntime(int capacity, int memoryLimit) {
        state_ptr = VegaFusionRuntime.innerCreate(capacity, memoryLimit);
    }

    /**
     * Destroys the native VegaFusionRuntime.
     * The class instance should not be used after destroy is called.
     */
    public void destroy() {
        if (state_ptr != 0) {
            innerDestroy(state_ptr);
            state_ptr = 0;
        }
    }

    /**
     * Patches a previously pre-transformed Vega specification.
     *
     * @param spec1 Previous input to preTransformSpec
     * @param preTransformedSpec1 Previous result of preTransformSpec
     * @param spec2 New Vega spec that is potentially similar to spec1
     * @return The pre-transformed version of spec2, or null if patching
     *         is not possible.
     * @throws IllegalStateException if the destroy method was called previously
     */
    public String patchPreTransformedSpec(String spec1, String preTransformedSpec1, String spec2) {
        validatePtr();
        return innerPatchPreTransformedSpec(spec1, preTransformedSpec1, spec2);
    }

    /**
     * This class encapsulates the results of the preTransformSpec method
     */
    record PreTransformSpecResult(String preTransformedSpec, String preTransformWarnings) {}

    /**
     * Evaluate the transforms in a Vega specification, returning a new
     * specification with transformed data included inline.
     *
     * @param spec The Vega specification to be transformed
     * @param localTz The local time zone (defaults to "UTC" if null)
     * @param defaultInputTz The default input time zone (defaults to localTz if null)
     * @param rowLimit The row limit for the inline transformed data.
     *                 If zero then no limit is imposed.
     * @param preserveInteractivity Whether to preserve interactivity in the resulting
     *                              Vega specification
     * @return A PreTransformSpecResult containing the transformed specification and any warnings.
     * @throws IllegalStateException if the destroy method was called previously
     */
    public PreTransformSpecResult preTransformSpec(String spec, String localTz, String defaultInputTz, int rowLimit, boolean preserveInteractivity) {
        validatePtr();
        return innerPreTransformSpec(state_ptr, spec, localTz, defaultInputTz, rowLimit, preserveInteractivity);
    }

    /**
     * Checks if the VegaFusionRuntime state is valid
     * (if the destroy method was not called previously)
     *
     * @return A boolean indicating whether the state is valid.
     */
    public boolean valid() {
        return state_ptr != 0;
    }

    /**
     * Validates the pointer to the native VegaFusionRuntime
     *
     * @throws IllegalStateException If the VegaFusionRuntime pointer is invalid.
     */
    private void validatePtr() throws IllegalStateException {
        if (state_ptr == 0) {
            throw new IllegalStateException("VegaFusionRuntime may not be used after calling destroy()");
        }
    }

    /**
     * The main entry point for the program.
     * This method retrieves the version of the VegaFusionRuntime and prints it out,
     * along with a brief description of VegaFusion.
     *
     * @param args An array of command-line arguments for the application.
     */
    public static void main(String[] args) {
        String version = VegaFusionRuntime.version();
        System.out.println("VegaFusion: Server-side scaling for Vega visualizations");
        System.out.println("Version: " + version);
    }
}
