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
AVZ target cell filter.
"""

from nova.cells import filters


class TargetCellAVZFilter(filters.BaseCellFilter):
    """Target Cell AVZ Filter"""

    def filter_all(self, cells, filter_properties):
        """Override filter_all() which operates on the full list
        of cells...
        """
        request_spec = filter_properties.get('request_spec', {})
        instance_properties = request_spec['instance_properties']
        instance_avz = instance_properties['availability_zone']

        if instance_avz == None:
            return cells

        filtered_cells = []

        for cell in cells:
            cell_avzs = cell.capabilities.get('avzs')
            if cell_avzs != None:
                if instance_avz in cell_avzs:
                    filtered_cells.append(cell)

        return filtered_cells

