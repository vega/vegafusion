## vegafusion-core
This crate contains all the logic to convert an input Vega specification into a VegaFusion task graph. In particular, it contains:
 - JSON parsing
 - Planning
 - Protocol buffer definitions and generated types for the task graph
 - Vega expression parsing

It may be desirable to split the crate in the future, but we don't yet have any applications that require only a subset of this functionality.