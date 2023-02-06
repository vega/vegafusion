## vegafusion-sql
This crate provides the `SqlConnection` and `SqlDataFrame` structs with implement the `Connection` and `DataFrame` traits from the `vegafusion-dataframe` crate using SQL.  The functionality for generating SQL string across dialects is always available in the crate. Optional support for evaluating the queries is enabled by feature flags with a `-conn` suffix.
