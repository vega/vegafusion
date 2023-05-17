package io.vegafusion;

import java.io.IOException;
import java.nio.file.*;
import java.util.Locale;
import java.util.Map;
import java.util.List;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;

import static org.junit.jupiter.api.Assertions.*;

public class VegaFusionRuntimeTest {
    private VegaFusionRuntime makeRuntime() {
        return new VegaFusionRuntime(32, 1000000000);
    }

    @Test
    void testVersion() throws IOException {
        String expectedVersion = new String(Files.readAllBytes(Paths.get("../version.txt"))).trim();
        var version = VegaFusionRuntime.version();
        assertEquals(version, expectedVersion);
    }

    @Test
    void testCreate() {
        VegaFusionRuntime runtime = makeRuntime();
        assertTrue(runtime.valid());

        // Destroy should invalidate
        runtime.destroy();
        assertFalse(runtime.valid());

        // Destroy should be idempotent
        runtime.destroy();
        assertFalse(runtime.valid());
    }

    @Test
    void testPatchPretransformedSpec() throws JsonProcessingException {
        // Create runtime
        VegaFusionRuntime runtime = makeRuntime();

        // Define simple specs that are compatible with patching
        String spec1 = "{\"width\": 100, \"height\": 200}";
        String preTransformedSpec1 = "{\"width\": 100, \"height\": 150}";
        String spec2 = "{\"width\": 150, \"height\": 200}";

        // Perform patch
        String preTransformedSpec2 = runtime.patchPreTransformedSpec(spec1, preTransformedSpec1, spec2);

        // Validate results
        ObjectMapper mapper = new ObjectMapper();
        Map<String, JsonNode> specMap = mapper.readValue(
                preTransformedSpec2, new TypeReference<>(){}
        );
        assertEquals(specMap.get("width").asInt(), 150);
        assertEquals(specMap.get("height").asInt(), 150);

        // Cleanup
        runtime.destroy();

        // Calling again after destroy should raise an exception
        assertThrows(IllegalStateException.class, () -> {
            runtime.patchPreTransformedSpec(spec1, preTransformedSpec1, spec2);
        });
    }

    @Test
    void testUnsuccessfulPatchPretransformedSpec() {
        // Create runtime
        VegaFusionRuntime runtime = makeRuntime();

        // Define specs that are not compatible with patching
        String spec1 = "{\"data\": [{\"name\": \"foo\"}]}";
        String preTransformedSpec1 = "{\"data\": []}";
        String spec2 = "{\"data\": [{\"name\": \"bar\"}]}";
        String preTransformedSpec2 = runtime.patchPreTransformedSpec(spec1, preTransformedSpec1, spec2);
        assertNull(preTransformedSpec2);

        // Cleanup
        runtime.destroy();

        // Calling again after destroy should raise an exception
        assertThrows(IllegalStateException.class, () -> {
            runtime.patchPreTransformedSpec(spec1, preTransformedSpec1, spec2);
        });
    }

    @Test
    void testInvalidPatchPretransformedSpec1() {
        // Create runtime
        VegaFusionRuntime runtime = makeRuntime();

        // Define spec strings that are not valid JSON
        String spec1 = "{\"data\"";
        String preTransformedSpec1 = "{\"data\"";
        String spec2 = "{\"data\"";

        VegaFusionException e = assertThrows(VegaFusionException.class, () -> {
            runtime.patchPreTransformedSpec(spec1, preTransformedSpec1, spec2);
        });

        assertTrue(e.getMessage().toLowerCase().contains("serde"));

        // Cleanup
        runtime.destroy();
    }

    @Test
    void testInvalidPatchPretransformedSpec2() {
        // Create runtime
        VegaFusionRuntime runtime = makeRuntime();

        // Define spec strings that are valid JSON but invalid Vega specs
        String spec1 = "{\"data\": 23}";
        String preTransformedSpec1 = "{}";
        String spec2 = "{}";

        VegaFusionException e = assertThrows(VegaFusionException.class, () -> {
            runtime.patchPreTransformedSpec(spec1, preTransformedSpec1, spec2);
        });

        assertTrue(e.getMessage().toLowerCase().contains("serde"));

        // Cleanup
        runtime.destroy();
    }


    @Test
    void testPretransformSpec() throws JsonProcessingException {
        // Build VegaFusionRuntime
        VegaFusionRuntime runtime = makeRuntime();

        // Construct histogram spec
        String spec = histSpec();

        // Pre-transform spec with rowLimit of 3 so that inline data is truncated
        VegaFusionRuntime.PreTransformSpecResult preTransformedSpecResult = runtime.preTransformSpec(
                spec, "UTC", "UTC", 3, true
        );

        // Check that resulting spec is reasonable
        String preTransformedSpec = preTransformedSpecResult.preTransformedSpec;
        assertTrue(preTransformedSpec.startsWith("{\"$schema\":\"https://vega.github.io/schema/vega/v5.json\""));

        // Parse warnings as JSON
        String preTransformedSpecWarnings = preTransformedSpecResult.preTransformWarnings;
        ObjectMapper mapper = new ObjectMapper();
        List<Map<String, JsonNode>> warningList = mapper.readValue(
                preTransformedSpecWarnings, new TypeReference<>(){}
        );

        // We should have 1 RowLimitExceeded warning
        assertEquals(warningList.size(), 1);
        var firstWarningType = warningList.get(0).get("type").asText();
        assertEquals(firstWarningType, "RowLimitExceeded");

        // Clean up Runtime
        runtime.destroy();

        // Calling again after destroy should raise an exception
        assertThrows(IllegalStateException.class, () -> {
            runtime.preTransformSpec(
                    spec, "UTC", "UTC", 3, true
            );
        });
    }

    @Test
    void testInvalidPretransformSpec() {
        // Build VegaFusionRuntime
        VegaFusionRuntime runtime = makeRuntime();

        // Construct invalid Vega spec
        String spec = "{\"data\": \"foo\"}";

        // Check that exception is raised by preTransformSpec
        VegaFusionException e = assertThrows(VegaFusionException.class, () -> {
            runtime.preTransformSpec(
                    spec, "UTC", "UTC", 0, true
            );
        });

        assertTrue(e.getMessage().toLowerCase().contains("serde"));

        // Clean up Runtime
        runtime.destroy();
    }

    String histSpec() {
        return """
                 {
                   "$schema": "https://vega.github.io/schema/vega/v5.json",
                   "background": "white",
                   "description": "https://vega.github.io/vega-lite/examples/histogram.html",
                   "padding": 5,
                   "width": 200,
                   "height": 200,
                   "style": "cell",
                   "data": [
                     {
                       "name": "source_0",
                       "url": "https://raw.githubusercontent.com/vega/vega-datasets/master/data/movies.json",
                       "format": {"type": "json"},
                       "transform": [
                         {
                           "type": "extent",
                           "field": "IMDB Rating",
                           "signal": "bin_maxbins_10_IMDB_Rating_extent"
                         },
                         {
                           "type": "bin",
                           "field": "IMDB Rating",
                           "as": [
                             "bin_maxbins_10_IMDB Rating",
                             "bin_maxbins_10_IMDB Rating_end"
                           ],
                           "signal": "bin_maxbins_10_IMDB_Rating_bins",
                           "extent": {"signal": "bin_maxbins_10_IMDB_Rating_extent"},
                           "maxbins": 10
                         },
                         {
                           "type": "aggregate",
                           "groupby": [
                             "bin_maxbins_10_IMDB Rating",
                             "bin_maxbins_10_IMDB Rating_end"
                           ],
                           "ops": ["count"],
                           "fields": [null],
                           "as": ["__count"]
                         },
                         {
                           "type": "filter",
                           "expr": "isValid(datum[\\"bin_maxbins_10_IMDB Rating\\"]) && isFinite(+datum[\\"bin_maxbins_10_IMDB Rating\\"])"
                         }
                       ]
                     }
                   ],
                   "marks": [
                     {
                       "name": "marks",
                       "type": "rect",
                       "style": ["bar"],
                       "from": {"data": "source_0"},
                       "encode": {
                         "update": {
                           "fill": {"value": "#4c78a8"},
                           "ariaRoleDescription": {"value": "bar"},
                           "description": {
                             "signal": "\\"IMDB Rating (binned): \\" + (!isValid(datum[\\"bin_maxbins_10_IMDB Rating\\"]) || !isFinite(+datum[\\"bin_maxbins_10_IMDB Rating\\"]) ? \\"null\\" : format(datum[\\"bin_maxbins_10_IMDB Rating\\"], \\"\\") + \\" – \\" + format(datum[\\"bin_maxbins_10_IMDB Rating_end\\"], \\"\\")) + \\"; Count of Records: \\" + (format(datum[\\"__count\\"], \\"\\"))"
                           },
                           "x2": [
                             {
                               "test": "!isValid(datum[\\"bin_maxbins_10_IMDB Rating\\"]) || !isFinite(+datum[\\"bin_maxbins_10_IMDB Rating\\"])",
                               "value": 0
                             },
                             {"scale": "x", "field": "bin_maxbins_10_IMDB Rating", "offset": 1}
                           ],
                           "x": [
                             {
                               "test": "!isValid(datum[\\"bin_maxbins_10_IMDB Rating\\"]) || !isFinite(+datum[\\"bin_maxbins_10_IMDB Rating\\"])",
                               "value": 0
                             },
                             {"scale": "x", "field": "bin_maxbins_10_IMDB Rating_end"}
                           ],
                           "y": {"scale": "y", "field": "__count"},
                           "y2": {"scale": "y", "value": 0}
                         }
                       }
                     }
                   ],
                   "scales": [
                     {
                       "name": "x",
                       "type": "linear",
                       "domain": {
                         "signal": "[bin_maxbins_10_IMDB_Rating_bins.start, bin_maxbins_10_IMDB_Rating_bins.stop]"
                       },
                       "range": [0, {"signal": "width"}],
                       "bins": {"signal": "bin_maxbins_10_IMDB_Rating_bins"},
                       "zero": false
                     },
                     {
                       "name": "y",
                       "type": "linear",
                       "domain": {"data": "source_0", "field": "__count"},
                       "range": [{"signal": "height"}, 0],
                       "nice": true,
                       "zero": true
                     }
                   ],
                   "axes": [
                     {
                       "scale": "y",
                       "orient": "left",
                       "gridScale": "x",
                       "grid": true,
                       "tickCount": {"signal": "ceil(height/40)"},
                       "domain": false,
                       "labels": false,
                       "aria": false,
                       "maxExtent": 0,
                       "minExtent": 0,
                       "ticks": false,
                       "zindex": 0
                     },
                     {
                       "scale": "x",
                       "orient": "bottom",
                       "grid": false,
                       "title": "IMDB Rating (binned)",
                       "labelFlush": true,
                       "labelOverlap": true,
                       "tickCount": {"signal": "ceil(width/10)"},
                       "zindex": 0
                     },
                     {
                       "scale": "y",
                       "orient": "left",
                       "grid": false,
                       "title": "Count of Records",
                       "labelOverlap": true,
                       "tickCount": {"signal": "ceil(height/40)"},
                       "zindex": 0
                     }
                   ]
                 }
                """;
    }
}
