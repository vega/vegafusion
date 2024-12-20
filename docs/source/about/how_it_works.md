# How it Works
VegaFusion has two main components: The Planner and the Runtime.

(planner)=
## Planner
The Planner starts with an arbitrary Vega specification (typically generated by Vega-Lite, but this is not a requirement). The Planner's job is to partition the specification into two valid Vega specifications, one that will execute in the browser with the Vega JavaScript library, and one that will execute on the server with the VegaFusion Runtime.

VegaFusion does not (yet) provide full coverage of all of Vega's transforms and all the features of the Vega expression language.  The planner uses information about which transforms and expression functions are supported to make decisions about which parts of the original Vega specification can be included in the resulting server specification.  The advantage of this approach is that VegaFusion can accept any Vega specification, and as more support is added over time, more of the input specification will be eligible for inclusion in the server specification.

Along with the client and server specifications, the planner also creates a communication plan.  The communication plan is a specification of the datasets and signals that must be passed from server to client, and from client to server in order for the interactive behavior of the original specification to be preserved.

## Runtime
After planning, the server specification is compiled into a VegaFusion task graph.  The job of the Runtime is to calculate the value of requested nodes within a task graph.

A task graph includes the values of the root nodes (those with no parents), but it does not include the values of any of the interior nodes (those with parents).  Each node in the task graph is a pure function of the values of its parents.  This enables the Runtime to calculate the value of any node in the Task graph from the specification, while keeping the overall task graph size small enough to be efficiently transferred between the client and the server.  The Runtime uses fingerprinting and precise caching to avoid repeated calculations of the same nodes.  The cache is "precise" in the sense that cached values can be shared across visualizations that share a common substructure, even if the full specifications are not identical.
