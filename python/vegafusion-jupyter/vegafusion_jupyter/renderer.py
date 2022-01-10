# VegaFusion
# Copyright (C) 2022, Jon Mease
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import altair as alt

def vegafusion_renderer(spec, **widget_options):
    """
    Altair renderer that displays charts using a VegaFusionWidget
    """
    import json
    from IPython.display import display
    from vegafusion_jupyter import VegaFusionWidget

    # Display widget as a side effect, then return empty string text representation
    # so that Altair doesn't also display a string representation
    widget = VegaFusionWidget(spec, **widget_options)
    display(widget)
    return {'text/plain': ""}


alt.renderers.register('vegafusion', vegafusion_renderer)
