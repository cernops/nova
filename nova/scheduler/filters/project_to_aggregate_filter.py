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

from nova import db
from oslo_log import log as logging
from nova.scheduler import filters

LOG = logging.getLogger(__name__)


class ProjectsToAggregateFilter(filters.BaseHostFilter):
    """Isolate projects in aggregates."""

    def host_passes(self, host_state, filter_properties):
        """If the metadata key "projects_to_aggregate" is defined in an
        aggregate only instances from the specified projects can be created on
        the aggregate hosts.
        """
        spec = filter_properties.get('request_spec', {})
        props = spec.get('instance_properties', {})
        project_id = props.get('project_id')
        context = filter_properties['context'].elevated()
        aggr_meta = db.aggregate_metadata_get_by_host(context,
                                 host_state.host, key='projects_to_aggregate')
        if aggr_meta != {}:
            aggr_meta_ids = aggr_meta['projects_to_aggregate'].pop()
            filter_projects_id = [x.strip() for x in aggr_meta_ids.split(',')]
            if project_id not in filter_projects_id:
                LOG.debug("%(host_state)s fails project id on "
                    "aggregate", {'host_state': host_state})
                return False
        else:
            aggrs_meta = db.aggregate_metadata_get_all_by_key(context,
                                                      'projects_to_aggregate')
            aggrs_projects_id = []
            for projects_id in aggrs_meta['projects_to_aggregate']:
                for x in projects_id.split(','):
                    aggrs_projects_id.append(x.strip())
            if project_id in aggrs_projects_id:
                LOG.debug("%(host_state)s fails project id on "
                    "aggregate", {'host_state': host_state})
                return False
        return True
