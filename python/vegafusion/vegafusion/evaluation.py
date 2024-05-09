def transformed_data(chart, row_limit=None, exclude=None):
    """
    Evaluate the transform associated with a Chart and return the transformed
    data as one or more DataFrames

    :param chart: altair Chart, LayerChart, HConcatChart, VConcatChart, or ConcatChart
    :param row_limit: Maximum number of rows to return. None (default) for unlimited
    :param exclude: list of mark names to exclude.
    :return:
        - DataFrame of transformed data for Chart input
        - list of DataFrames for LayerChart, HConcatChart, VConcatChart, ConcatChart
    """
    return chart.transformed_data(row_limit=row_limit, exclude=exclude)
