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
Flavors target cell filter.
"""

from oslo_config import cfg
from nova.cells import filters
from oslo_log import log as logging

cell_flavor_target_cell_opts = [
        cfg.DictOpt('cells_flavors',
               default={},
               help='list of cells and flavors')
]


CONF = cfg.CONF
CONF.register_opts(cell_flavor_target_cell_opts, group='cells')

LOG = logging.getLogger(__name__)


class TargetCellFlavorFilter(filters.BaseCellFilter):
    """Target Cell Flavor Filter"""

    def filter_all(self, cells, filter_properties):
        """Override filter_all() which operates on the full list
        of cells...
        """
        request_spec = filter_properties.get('request_spec', {})
        instance_properties = request_spec['instance_properties']
        instance_system_metadata = instance_properties['system_metadata']
        instance_type_name = instance_system_metadata['instance_type_name']

        cells = list(cells)
        cells_names = [x.name for x in cells]
        cells_flavors = CONF.cells.cells_flavors

        scheduler = filter_properties['scheduler']
        if len(cells) == 1 and\
           cells[0].name == scheduler.state_manager.get_my_state().name:
            return cells

        for cell in cells_flavors.keys():
            flavors = [x.strip() for x in cells_flavors[cell].split(';')]
            if flavors != []:
                if instance_type_name not in flavors:
                    try:
                        cells_names.remove(cell)
                    except:
                        LOG.info("Cell not available in scheduling flavor: %s" % cell)
                        pass

        av_cells = [x for x in cells if x.name in cells_names]
        return av_cells