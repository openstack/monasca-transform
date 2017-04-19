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
import datetime
from oslo_config import cfg
from sqlalchemy import create_engine
from sqlalchemy import desc
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker

from monasca_transform.db.db_utils import DbUtil
from monasca_transform.offset_specs import OffsetSpec
from monasca_transform.offset_specs import OffsetSpecs

Base = automap_base()


class MySQLOffsetSpec(Base, OffsetSpec):
    __tablename__ = 'kafka_offsets'

    def __str__(self):
        return "%s,%s,%s,%s,%s,%s,%s,%s" % (str(self.id),
                                            str(self.topic),
                                            str(self.partition),
                                            str(self.until_offset),
                                            str(self.from_offset),
                                            str(self.batch_time),
                                            str(self.last_updated),
                                            str(self.revision))


class MySQLOffsetSpecs(OffsetSpecs):

    def __init__(self):

        db = create_engine(DbUtil.get_python_db_connection_string(),
                           isolation_level="READ UNCOMMITTED")

        if cfg.CONF.service.enable_debug_log_entries:
            db.echo = True

        # reflect the tables
        Base.prepare(db, reflect=True)

        Session = sessionmaker(bind=db)
        self.session = Session()

        # keep these many offset versions around
        self.MAX_REVISIONS = cfg.CONF.repositories.offsets_max_revisions

    def _manage_offset_revisions(self):
        """manage offset versions"""
        distinct_offset_specs = self.session.query(
            MySQLOffsetSpec).group_by(MySQLOffsetSpec.app_name,
                                      MySQLOffsetSpec.topic,
                                      MySQLOffsetSpec.partition
                                      ).all()

        for distinct_offset_spec in distinct_offset_specs:
            ordered_versions = self.session.query(
                MySQLOffsetSpec).filter_by(
                    app_name=distinct_offset_spec.app_name,
                    topic=distinct_offset_spec.topic,
                    partition=distinct_offset_spec.partition).order_by(
                        desc(MySQLOffsetSpec.id)).all()

            revision = 1
            for version_spec in ordered_versions:
                version_spec.revision = revision
                revision = revision + 1

        # delete any revisions excess than required
        self.session.query(MySQLOffsetSpec).filter(
            MySQLOffsetSpec.revision > self.MAX_REVISIONS).delete(
                synchronize_session="fetch")

    def get_kafka_offsets(self, app_name):
        return {'%s_%s_%s' % (
            offset.get_app_name(), offset.get_topic(), offset.get_partition()
        ): offset for offset in self.session.query(MySQLOffsetSpec).filter(
            MySQLOffsetSpec.app_name == app_name,
            MySQLOffsetSpec.revision == 1).all()}

    def get_kafka_offsets_by_revision(self, app_name, revision):
        return {'%s_%s_%s' % (
            offset.get_app_name(), offset.get_topic(), offset.get_partition()
        ): offset for offset in self.session.query(MySQLOffsetSpec).filter(
            MySQLOffsetSpec.app_name == app_name,
            MySQLOffsetSpec.revision == revision).all()}

    def get_most_recent_batch_time_from_offsets(self, app_name, topic):
        try:
            # get partition 0 as a representative of all others
            offset = self.session.query(MySQLOffsetSpec).filter(
                MySQLOffsetSpec.app_name == app_name,
                MySQLOffsetSpec.topic == topic,
                MySQLOffsetSpec.partition == 0,
                MySQLOffsetSpec.revision == 1).one()
            most_recent_batch_time = datetime.datetime.strptime(
                offset.get_batch_time(),
                '%Y-%m-%d %H:%M:%S')
        except Exception:
            most_recent_batch_time = None

        return most_recent_batch_time

    def delete_all_kafka_offsets(self, app_name):
        try:
            self.session.query(MySQLOffsetSpec).filter(
                MySQLOffsetSpec.app_name == app_name).delete()
            self.session.commit()
        except Exception:
            # Seems like there isn't much that can be done in this situation
            pass

    def add_all_offsets(self, app_name, offsets,
                        batch_time_info):
        """add offsets. """
        try:

            # batch time
            batch_time = \
                batch_time_info.strftime(
                    '%Y-%m-%d %H:%M:%S')

            # last updated
            last_updated = \
                datetime.datetime.now().strftime(
                    '%Y-%m-%d %H:%M:%S')

            NEW_REVISION_NO = -1

            for o in offsets:
                offset_spec = MySQLOffsetSpec(
                    topic=o.topic,
                    app_name=app_name,
                    partition=o.partition,
                    from_offset=o.fromOffset,
                    until_offset=o.untilOffset,
                    batch_time=batch_time,
                    last_updated=last_updated,
                    revision=NEW_REVISION_NO)
                self.session.add(offset_spec)

            # manage versions
            self._manage_offset_revisions()

            self.session.commit()
        except Exception:
            self.session.rollback()
            raise

    def add(self, app_name, topic, partition,
            from_offset, until_offset, batch_time_info):
        """add offset info. """
        try:
            # batch time
            batch_time = \
                batch_time_info.strftime(
                    '%Y-%m-%d %H:%M:%S')

            # last updated
            last_updated = \
                datetime.datetime.now().strftime(
                    '%Y-%m-%d %H:%M:%S')

            NEW_REVISION_NO = -1

            offset_spec = MySQLOffsetSpec(
                topic=topic,
                app_name=app_name,
                partition=partition,
                from_offset=from_offset,
                until_offset=until_offset,
                batch_time=batch_time,
                last_updated=last_updated,
                revision=NEW_REVISION_NO)

            self.session.add(offset_spec)

            # manage versions
            self._manage_offset_revisions()

            self.session.commit()
        except Exception:
            self.session.rollback()
            raise
