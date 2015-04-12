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

# This is a placeholder for Havana backports.
# Do not use this number for new Icehouse work.  New Icehouse work starts after
# all the placeholders.
#
# See blueprint backportable-db-migrations-icehouse
# http://lists.openstack.org/pipermail/openstack-dev/2013-March/006827.html


# CERN
from sqlalchemy import Boolean, Column, DateTime, Integer
from sqlalchemy import MetaData, String, Table
from nova.openstack.common import log as logging

meta = MetaData()

cern_network = Table('cern_network', meta,
        Column('created_at', DateTime(timezone=False)),
        Column('updated_at', DateTime(timezone=False)),
        Column('deleted_at', DateTime(timezone=False)),
        Column('deleted', Boolean(create_constraint=True, name=None)),
        Column('id', Integer, primary_key=True),
        Column('netcluster', String(255)),
        Column('host', String(255)),
        )

# (fixed_ips)
column_mac = Column('mac',  String(255))
column_netcluster = Column('netcluster',  String(255))
column_address_v6 = Column('address_v6',  String(255))


def upgrade(migrate_engine):
    meta.bind = migrate_engine

    # create CERN tables
    for table in (cern_network, ):
        try:
            table.create()
        except Exception:
            pass

    # alter fixed_ips table
    table = Table('fixed_ips', meta, autoload=True)
    try:
        table.create_column(column_mac)
    except Exception:
        pass

    try:
        table.create_column(column_netcluster)
    except Exception:
        pass

    try:
        table.create_column(column_address_v6)
    except Exception:
        pass

def downgrade(migration_engine):
    pass
# CERN
