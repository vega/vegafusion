package io.vegafusion;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.io.IOException;

class VegaFusionRuntime {
    public static native String version();

    private static native long innerCreate(int capacity, int memoryLimit);

    private static native void innerDestroy(long pointer);

    private static native String innerPatchPreTransformedSpec(
            String spec1, String preTransformedSpec1, String spec2
    );

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
                String libName = "libvegafusion_jni";
                String osName = System.getProperty("os.name").toLowerCase();
                String osArch = System.getProperty("os.arch").toLowerCase();

                String libExtension;
                String directory;
                if  (osName.contains("win")) {
                    libExtension = ".dll";
                    if (osArch.equals("amd64") || osArch.equals("x86_64")) {
                        directory = "win-64";
                    } else {
                        throw new UnsupportedOperationException("Unsupported architecture for Windows: " + osArch);
                    }
                } else if (osName.contains("mac")) {
                    if (osArch.equals("amd64") || osArch.equals("x86_64")) {
                        directory = "osx-64";
                    } else if (osArch.equals("aarch64") || osArch.equals("arm64")) {
                        directory = "osx-arm64";
                    } else {
                        throw new UnsupportedOperationException("Unsupported architecture for macOS: " + osArch);
                    }
                    libExtension = ".dylib";
                } else if (osName.contains("nix") || osName.contains("nux")) {
                    libExtension = ".so";
                    if (osArch.equals("amd64") || osArch.equals("x86_64")) {
                        directory = "linux-64";
                    } else {
                        throw new UnsupportedOperationException("Unsupported architecture for Linux: " + osArch);
                    }
                } else {
                    throw new UnsupportedOperationException("Unsupported operating system: " + osName);
                }

                // Path in the jar file to the compiled library
                String libPathInJar = "/native/" + directory + "/" + libName + libExtension;

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

    static class PreTransformSpecResult {
        public String preTransformedSpec;
        public String preTransformWarnings;

        public PreTransformSpecResult(String preTransformedSpec, String preTransformWarnings) {
            this.preTransformedSpec = preTransformedSpec;
            this.preTransformWarnings = preTransformWarnings;
        }
    }

    private long state_ptr;

    public VegaFusionRuntime(int capacity, int memoryLimit) {
        state_ptr = VegaFusionRuntime.innerCreate(capacity, memoryLimit);
    }

    public void destroy() {
        if (state_ptr != 0) {
            innerDestroy(state_ptr);
            state_ptr = 0;
        }
    }

    public String patchPreTransformedSpec(String spec1, String preTransformedSpec1, String spec2) {
        validatePtr();
        return innerPatchPreTransformedSpec(spec1, preTransformedSpec1, spec2);
    }

    public PreTransformSpecResult preTransformSpec(String spec, String localTz, String defaultInputTz, int rowLimit, boolean preserveInteractivity) {
        validatePtr();
        return innerPreTransformSpec(state_ptr, spec, localTz, defaultInputTz, rowLimit, preserveInteractivity);
    }

    public boolean valid() {
        return state_ptr != 0;
    }

    private void validatePtr() throws IllegalStateException {
        if (state_ptr == 0) {
            throw new IllegalStateException("VegaFusionRuntime may not be used after calling destroy()");
        }
    }

    public static void main(String[] args) {
        String version = VegaFusionRuntime.version();
        System.out.println("VegaFusion: Server-side scaling for Vega visualizations");
        System.out.println("Version: " + version);
    }
}

