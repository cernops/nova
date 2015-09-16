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
Project target cell filter.
"""

from oslo.config import cfg
from nova.cells import filters

cell_project_target_cell_opts = [
        cfg.ListOpt('cells_default',
               default=[],
               help='list of default cells'),
        cfg.DictOpt('cells_projects',
               default={},
               help='list of cells and projects')
]


CONF = cfg.CONF
CONF.register_opts(cell_project_target_cell_opts, group='cells')

class TargetCellProjectFilter(filters.BaseCellFilter):
    """Target Cell Project Filter"""

    def filter_all(self, cells, filter_properties):
        """Override filter_all() which operates on the full list
        of cells...
        """
        request_spec = filter_properties.get('request_spec', {})
        instance_properties = request_spec['instance_properties']
        instance_project_id = instance_properties['project_id']

        cells = list(cells)
        project_cells = []
        cells_projects = CONF.cells.cells_projects
        cells_default = CONF.cells.cells_default

        scheduler = filter_properties['scheduler']
        if len(cells) == 1 and\
           cells[0].name == scheduler.state_manager.get_my_state().name:
            return cells

        generic_cells = [x for x in cells if x.name in cells_default]

        for cell in cells_projects.keys():
            projects = [x.strip() for x in cells_projects[cell].split(';')]
            if projects != []:
                if instance_project_id in projects:
                    project_cells.append(cell)

        if project_cells != []:
            return [x for x in cells if x.name in project_cells]

        return generic_cells

