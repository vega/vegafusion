package io.vegafusion;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.io.IOException;

class VegaFusionRuntime {
    // This declares that the static `hello` method will be provided
    // a native library.
    private static native String hello(String input);
    public static native String version();

    private static native long innerCreate();

    private static native void innerDestroy(long pointer);

    private static native String innerPatchPreTransformedSpec(String spec1, String preTransformedSpec1, String spec2) throws IllegalArgumentException;

    private static native String innerPreTransformSpec(long pointer, String spec, String localTz, String defaultInputTz);

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
                // Path in the jar file to the compiled library
                String libPathInJar = "/native/macos/libvegafusion_jni.dylib";

                // Extract the library to a temporary file
                InputStream libStream = VegaFusionRuntime.class.getResourceAsStream(libPathInJar);
                if (libStream == null) {
                    throw new RuntimeException("Failed to find " + libPathInJar + " in jar file");
                }

                // Create a temp file and get its path
                try {
                    String tempFileName = Paths.get("libvegafusion_jni.dylib").getFileName().toString();
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

    public VegaFusionRuntime() {
        state_ptr = VegaFusionRuntime.innerCreate();
    }

    public void destroy() {
        innerDestroy(state_ptr);
        state_ptr = 0;
    }

    public String patchPreTransformedSpec(String spec1, String preTransformedSpec1, String spec2) {
        return innerPatchPreTransformedSpec(spec1, preTransformedSpec1, spec2);
    }

    public String preTransformSpec(String spec, String localTz, String defaultInputTz) {
        return innerPreTransformSpec(state_ptr, spec, localTz, defaultInputTz);
    }

    public static void main(String[] args) {
        String version = VegaFusionRuntime.version();
        System.out.println("VegaFusion: Server-side scaling for Vega visualizations");
        System.out.println("Version: " + version);
    }
}

