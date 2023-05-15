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
}
