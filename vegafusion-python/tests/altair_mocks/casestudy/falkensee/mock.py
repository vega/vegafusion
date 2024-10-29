# https://altair-viz.github.io/gallery/falkensee.html

import altair as alt
import polars as pl
from datetime import date

source = [
    {"year": date(1875, 1, 1), "population": 1309},
    {"year": date(1890, 1, 1), "population": 1558},
    {"year": date(1910, 1, 1), "population": 4512},
    {"year": date(1925, 1, 1), "population": 8180},
    {"year": date(1933, 1, 1), "population": 15915},
    {"year": date(1939, 1, 1), "population": 24824},
    {"year": date(1946, 1, 1), "population": 28275},
    {"year": date(1950, 1, 1), "population": 29189},
    {"year": date(1964, 1, 1), "population": 29881},
    {"year": date(1971, 1, 1), "population": 26007},
    {"year": date(1981, 1, 1), "population": 24029},
    {"year": date(1985, 1, 1), "population": 23340},
    {"year": date(1989, 1, 1), "population": 22307},
    {"year": date(1990, 1, 1), "population": 22087},
    {"year": date(1991, 1, 1), "population": 22139},
    {"year": date(1992, 1, 1), "population": 22105},
    {"year": date(1993, 1, 1), "population": 22242},
    {"year": date(1994, 1, 1), "population": 22801},
    {"year": date(1995, 1, 1), "population": 24273},
    {"year": date(1996, 1, 1), "population": 25640},
    {"year": date(1997, 1, 1), "population": 27393},
    {"year": date(1998, 1, 1), "population": 29505},
    {"year": date(1999, 1, 1), "population": 32124},
    {"year": date(2000, 1, 1), "population": 33791},
    {"year": date(2001, 1, 1), "population": 35297},
    {"year": date(2002, 1, 1), "population": 36179},
    {"year": date(2003, 1, 1), "population": 36829},
    {"year": date(2004, 1, 1), "population": 37493},
    {"year": date(2005, 1, 1), "population": 38376},
    {"year": date(2006, 1, 1), "population": 39008},
    {"year": date(2007, 1, 1), "population": 39366},
    {"year": date(2008, 1, 1), "population": 39821},
    {"year": date(2009, 1, 1), "population": 40179},
    {"year": date(2010, 1, 1), "population": 40511},
    {"year": date(2011, 1, 1), "population": 40465},
    {"year": date(2012, 1, 1), "population": 40905},
    {"year": date(2013, 1, 1), "population": 41258},
    {"year": date(2014, 1, 1), "population": 41777},
]

source2 = [
    {"start": date(1933, 1, 1), "end": date(1945, 1, 1), "event": "Nazi Rule"},
    {"start": date(1948, 1, 1), "end": date(1989, 1, 1), "event": "GDR (East Germany)"},
]


source = pl.DataFrame(source)
source2 = pl.DataFrame(source2)

line = (
    alt.Chart(source)
    .mark_line(color="#333")
    .encode(alt.X("year:T", axis=alt.Axis(format="%Y")), y="population")
    .properties(width=500, height=300)
)

point = line.mark_point(color="#333")

rect = alt.Chart(source2).mark_rect().encode(x="start:T", x2="end:T", color="event:N")

rect + line + point
