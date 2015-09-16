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
Hypervisor target cell filter.
"""

from nova.cells import filters
from oslo_log import log as logging

LOG = logging.getLogger(__name__)


class TargetCellHypervisorFilter(filters.BaseCellFilter):
    """Target Cell Hypervisor Filter"""

    def filter_all(self, cells, filter_properties):
        """Override filter_all() which operates on the full list
        of cells...
        """
        request_spec = filter_properties.get('request_spec', {})
        image_properties = request_spec.get('image', {}).get('properties', {})
        hypervisor_type = image_properties.get(
            'hypervisor_type')

        if hypervisor_type == None or hypervisor_type.lower() != 'hyperv':
            hypervisor_type = 'qemu'

        filtered_cells = []

        for cell in cells:
            cell_hypervisors = cell.capabilities.get('hypervisor')
            if cell_hypervisors != None:
                if hypervisor_type in cell_hypervisors:
                    filtered_cells.append(cell)

        return filtered_cells
