# Copyright 2016 Hewlett Packard Enterprise Development Company LP
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

from oslo_config import cfg
from sqlalchemy import create_engine
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.orm import sessionmaker

from monasca_transform.offset_specs import OffsetSpec
from monasca_transform.offset_specs import OffsetSpecs

Base = automap_base()


class MySQLOffsetSpec(Base, OffsetSpec):

    __tablename__ = 'kafka_offsets'


class MySQLOffsetSpecs(OffsetSpecs):

    def __init__(self):
        database_name = cfg.CONF.database.database_name
        database_server = cfg.CONF.database.host
        database_uid = cfg.CONF.database.username
        database_pwd = cfg.CONF.database.password

        db = create_engine('mysql+pymysql://%s:%s@%s/%s' % (
            database_uid,
            database_pwd,
            database_server,
            database_name
        ), isolation_level="READ UNCOMMITTED")

        db.echo = True
        # reflect the tables
        Base.prepare(db, reflect=True)

        Session = sessionmaker(bind=db)
        self.session = Session()

    def add(self, app_name, topic, partition,
            from_offset, until_offset):
        try:
            offset_spec = self.session.query(MySQLOffsetSpec).filter_by(
                app_name=app_name, topic=topic,
                partition=partition).one()
            offset_spec.from_offset = from_offset
            offset_spec.until_offset = until_offset
            self.session.commit()

        except NoResultFound:
            offset_spec = MySQLOffsetSpec(
                topic=topic,
                app_name=app_name,
                partition=partition,
                from_offset=from_offset,
                until_offset=until_offset)
            self.session.add(offset_spec)
            self.session.commit()

    def get_kafka_offsets(self):
        return {'%s_%s_%s' % (
            offset.get_app_name(), offset.get_topic(), offset.get_partition()
        ): offset for offset in self.session.query(MySQLOffsetSpec).all()}

    def delete_all_kafka_offsets(self):
        try:
            self.session.query(MySQLOffsetSpec).delete()
            self.session.commit()

        except Exception:
            # Seems like there isn't much that can be done in this situation
            pass
