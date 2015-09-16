# Copyright (c) 2014 CERN
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

"""
Computer Centre target cell filter.
"""

from nova.cells import filters
from oslo_log import log as logging

LOG = logging.getLogger(__name__)


class TargetCellCCentreFilter(filters.BaseCellFilter):
    """Target Cell CCentre Filter"""

    def filter_all(self, cells, filter_properties):
        """Override filter_all() which operates on the full list
        of cells...
        """
        request_spec = filter_properties.get('request_spec', {})
        instance_properties = request_spec['instance_properties']
        instance_metadata = instance_properties['metadata']
        instance_ccentre = instance_metadata.get('cern-datacentre')

        if instance_ccentre == None:
            return cells

        filtered_cells = []

        for cell in cells:
            cell_ccentre = cell.capabilities.get('datacentre')
            if cell_ccentre != None:
                if instance_ccentre in cell_ccentre:
                    filtered_cells.append(cell)

        return filtered_cells

