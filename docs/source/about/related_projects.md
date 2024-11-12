# Related Projects
Here are a few related projects that have some overlap with the goals of VegaFusion.

## [`altair-transform`](https://github.com/altair-viz/altair-transform)
`altair-transform` is a Python library created by Jake Vanderplas, one of the creators of Altair. It consists of pandas implementations of most of the Vega expression language and Vega Transforms that are available through Altair. It supports two main use cases:

 1. [Extracting Data](https://github.com/altair-viz/altair-transform#example-extracting-data): Given a Chart with transforms, `altair-transform` can be used to construct a Pandas DataFrame representing the result of the Chart's transforms, and the input to the Chart's mark.
 2. [Pre-Aggregating Large Datasets](https://github.com/altair-viz/altair-transform#example-pre-aggregating-large-datasets): Given a Chart with transforms, `altair-transform` can be used to evaluate the transforms and create a new Chart instance that refers only to this evaluated dataset. For aggregation charts like histograms, this can result in a much smaller dataset being transferred to the browser.

These workflows were not supported by the initial version of VegaFusion, but support for both was added in version 1.0.

`altair-transform` does not support evaluating transforms on the server in interactive workflows like linked histogram brushing, which was the initial focus of VegaFusion. 

## [`ibis-vega-transform`](https://github.com/Quansight/ibis-vega-transform)
`ibis-vega-transform` is a Python library and JupyterLab extension developed by [Quansight](https://www.quansight.com/). It translates pipelines of Vega transforms into [Ibis](https://ibis-project.org/) query expressions, which can then be evaluated with a variety of Ibis database backends (in particular, OmniSci). 

The JupyterLab extension makes two-way communication between the browser and the Python kernel possible, and this is used to support interactive visualizations like histogram brushing.

In contrast to the Planner approach used by VegaFusion, `ibis-vega-transform` replaces pipelines of Vega transforms with a custom transform type and then registers a JavaScript handler for this custom transform type.  This JavaScript handler then uses Jupyter Comms to communicate with the Python portion of the library. The Python library converts the requested Vega transforms into an ibis query, evaluates the query, and sends the resulting dataset back to the browser using a Jupyter Comm.

An advantage of this approach is that the Vega JavaScript library remains in control of the entire specification so the external `ibis-vega-transform` library does not need to maintain an independent task graph in order to support interactivity.  A downside of this approach is that the result of every transform pipeline must be sent back to the client and be stored in the Vega dataflow graph.  Often times this is not a problem, because the transform pipeline includes an aggregation stage that significantly reduces the dataset size.  However, sometimes the result of a transform pipeline is quite large, but it is only used as input to other transform pipelines.  In this case, it is advantageous to keep the large intermediary result cached on the server and to not send it to the client at all.  This use case is one of the reasons that VegaFusion uses the Planner+Runtime architecture described previously.

Currently, VegaFusion implements all of its transform logic in the Python process (with efficient multi-threading) and has no capability to connect to external data providers like databases.  This is certainly a desirable capability, and may be enabled in VegaFusion by the [datafusion-federation](https://github.com/datafusion-contrib/datafusion-federation) project.
