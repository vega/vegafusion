## vegafusion-datafusion-udfs
This crate contains the definitions of the DataFusion UDFs that are used to implement select Vega expression functions and transforms. These UDFs are used in two places.
 - The `DataFusionConnection` provided by `vegafusion-sql` adds these UDFs to its `SessionContext` so that they are available for use in SQL querires.
 - The `vegafusion-runtime` crate uses these UDFs for the evaluation of signal expressions and for simplifying expressions passed to the `filter` and `formula` transforms. Note: Even when a non-DataFusion Connection is used, DataFusion is still used for signal evaluation and expression simplification.
