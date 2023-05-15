package io.vegafusion;

import java.io.IOException;
import java.nio.file.*;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class VegaFusionRuntimeTest {
    @Test
    void testVersion() throws IOException {
        String expectedVersion = new String(Files.readAllBytes(Paths.get("../version.txt"))).trim();
        System.out.println("Expected Version: " + expectedVersion);

        var version = VegaFusionRuntime.version();
        assertEquals(version, "1.3.0-rc1");
    }

    @Test
    void testCreate() {
        VegaFusionRuntime runtime = new VegaFusionRuntime();
        runtime.destroy();
    }

    @Test
    void testPatchPretransformedSpec() {
        VegaFusionRuntime runtime = new VegaFusionRuntime();
        String spec1 = "{\"width\": 100, \"height\": 200}";
        String preTransformedSpec1 = "{\"width\": 100, \"height\": 150}";
        String spec2 = "{\"width\": 150, \"height\": 200}";

        String preTransformedSpec2 = runtime.patchPreTransformedSpec(spec1, preTransformedSpec1, spec2);
        System.out.println(preTransformedSpec2);

        assertEquals(preTransformedSpec2, "{\"$schema\":\"https://vega.github.io/schema/vega/v5.json\",\"width\":150,\"height\":150}");
        runtime.destroy();
    }
}
