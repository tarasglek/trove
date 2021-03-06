# Copyright 2015 IBM Corp.
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

import os

from oslo_log import log as logging
from oslo_utils import encodeutils

from trove.common import cfg
from trove.common.db import models
from trove.common import exception
from trove.common.i18n import _
from trove.common import instance as rd_instance
from trove.common.stream_codecs import PropertiesCodec
from trove.common import utils as utils
from trove.guestagent.common.configuration import ConfigurationManager
from trove.guestagent.common.configuration import ImportOverrideStrategy
from trove.guestagent.common import guestagent_utils
from trove.guestagent.common import operating_system
from trove.guestagent.datastore.experimental.db2 import system
from trove.guestagent.datastore import service

CONF = cfg.CONF
LOG = logging.getLogger(__name__)
MOUNT_POINT = CONF.db2.mount_point
FAKE_CFG = os.path.join(MOUNT_POINT, "db2.cfg.fake")
DB2_DEFAULT_CFG = os.path.join(MOUNT_POINT, "db2_default_dbm.cfg")


class DB2App(object):
    """
    Handles installation and configuration of DB2
    on a Trove instance.
    """
    def __init__(self, status, state_change_wait_time=None):
        LOG.debug("Initialize DB2App.")
        self.state_change_wait_time = (
            state_change_wait_time if state_change_wait_time else
            CONF.state_change_wait_time
        )
        LOG.debug("state_change_wait_time = %s.", self.state_change_wait_time)
        self.status = status
        self.dbm_default_config = {}
        self.init_config()
        '''
        If DB2 guest agent has been configured for online backups,
        every database that is created will be configured for online
        backups. Since online backups are done using archive logging,
        we need to create a directory to store the archived logs.
        '''
        if CONF.db2.backup_strategy == 'DB2OnlineBackup':
            create_db2_dir(system.DB2_ARCHIVE_LOGS_DIR)

    def init_config(self):
        if not operating_system.exists(MOUNT_POINT, True):
            operating_system.create_directory(MOUNT_POINT,
                                              system.DB2_INSTANCE_OWNER,
                                              system.DB2_INSTANCE_OWNER,
                                              as_root=True)
        """
        The database manager configuration file - db2systm is stored  under the
        /home/db2inst1/sqllib directory. To update the configuration
        parameters, DB2 recommends using the command - UPDATE DBM CONFIGURATION
        commands instead of directly updating the config file.

        The existing PropertiesCodec implementation has been reused to handle
        text-file operations. Configuration overrides are implemented using
        the ImportOverrideStrategy of the guestagent configuration manager.
        """
        LOG.debug("Initialize DB2 configuration")
        revision_dir = (
            guestagent_utils.build_file_path(
                os.path.join(MOUNT_POINT,
                             os.path.dirname(system.DB2_INSTANCE_OWNER)),
                ConfigurationManager.DEFAULT_STRATEGY_OVERRIDES_SUB_DIR)
        )
        if not operating_system.exists(FAKE_CFG):
            operating_system.write_file(FAKE_CFG, '', as_root=True)
            operating_system.chown(FAKE_CFG, system.DB2_INSTANCE_OWNER,
                                   system.DB2_INSTANCE_OWNER, as_root=True)
        self.configuration_manager = (
            ConfigurationManager(FAKE_CFG, system.DB2_INSTANCE_OWNER,
                                 system.DB2_INSTANCE_OWNER,
                                 PropertiesCodec(delimiter='='),
                                 requires_root=True,
                                 override_strategy=ImportOverrideStrategy(
                                     revision_dir, "cnf"))
        )
        '''
        Below we are getting the database manager default configuration and
        saving it to the DB2_DEFAULT_CFG file. This is done to help with
        correctly resetting the configurations to the original values when
        user wants to detach a user-defined configuration group from an
        instance. DB2 provides a command to reset the database manager
        configuration parameters (RESET DBM CONFIGURATION) but this command
        resets all the configuration parameters to the system defaults. When
        we build a DB2 guest image there are certain configurations
        parameters like SVCENAME which we set so that the instance can start
        correctly. Hence resetting this value to the system default will
        render the instance in an unstable state. Instead, the recommended
        way for resetting a subset of configuration parameters is to save
        the output of GET DBM CONFIGURATION of the original configuration
        and then call UPDATE DBM CONFIGURATION to reset the value.
          http://www.ibm.com/support/knowledgecenter/SSEPGG_10.5.0/
        com.ibm.db2.luw.admin.cmd.doc/doc/r0001970.html
        '''
        if not operating_system.exists(DB2_DEFAULT_CFG):
            run_command(system.GET_DBM_CONFIGURATION % {
                "dbm_config": DB2_DEFAULT_CFG})
        self.process_default_dbm_config()

    def process_default_dbm_config(self):
        """
        Once the default database manager configuration is saved to
        DB2_DEFAULT_CFG, we try to store the configuration parameters
        and values into a dictionary object, dbm_default_config. For
        example, a sample content of the database manager configuration
        file looks like this:
         Buffer pool                         (DFT_MON_BUFPOOL) = OFF
        We need to process this so that we key it on the configuration
        parameter DFT_MON_BUFPOOL.
        """
        with open(DB2_DEFAULT_CFG) as cfg_file:
            for line in cfg_file:
                if '=' in line:
                    item = line.rstrip('\n').split(' = ')
                    fIndex = item[0].rfind('(')
                    lIndex = item[0].rfind(')')
                    if fIndex > -1:
                        param = item[0][fIndex + 1: lIndex]
                        value = item[1]
                        '''
                        Some of the configuration parameters have the keyword
                        AUTOMATIC to indicate that DB2 will automatically
                        adjust the setting depending on system resources.
                        For some configuration parameters, DB2 also allows
                        setting a starting value along with the AUTOMATIC
                        setting. In the configuration parameter listing,
                        this is displayed as:
                        MON_HEAP_SZ = AUTOMATIC(90)
                        This can be set using the following command:
                        db2 update dbm cfg using mon_heap_sz 90 automatic
                        '''
                        if not value:
                            value = 'NULL'
                        elif 'AUTOMATIC' in value:
                            fIndex = item[1].rfind('(')
                            lIndex = item[1].rfind(')')
                            if fIndex > -1:
                                default_value = item[1][fIndex + 1: lIndex]
                                value = default_value + " AUTOMATIC"
                        self.dbm_default_config.update({param: value})

    def update_hostname(self):
        """
        When DB2 server is installed, it uses the hostname of the
        instance were the image was built. This needs to be updated
        to reflect the guest instance.
        """
        LOG.debug("Update the hostname of the DB2 instance.")
        try:
            run_command(system.UPDATE_HOSTNAME,
                        superuser='root')
        except exception.ProcessExecutionError:
            raise RuntimeError(_("Command to update the hostname failed."))

    def change_ownership(self, mount_point):
        """
        When DB2 server instance is installed, it does not have the
        DB2 local database directory created (/home/db2inst1/db2inst1).
        This gets created when we mount the cinder volume. So we need
        to change ownership of this directory to the DB2 instance user
        - db2inst1.
        """
        LOG.debug("Changing ownership of the DB2 data directory.")
        try:
            operating_system.chown(mount_point,
                                   system.DB2_INSTANCE_OWNER,
                                   system.DB2_INSTANCE_OWNER,
                                   recursive=False, as_root=True)
        except exception.ProcessExecutionError:
            raise RuntimeError(_(
                "Command to change ownership of  DB2 data directory failed."))

    def _enable_db_on_boot(self):
        LOG.debug("Enable DB on boot.")
        try:
            run_command(system.ENABLE_AUTOSTART)
        except exception.ProcessExecutionError:
            raise RuntimeError(_(
                "Command to enable DB2 server on boot failed."))

    def _disable_db_on_boot(self):
        LOG.debug("Disable DB2 on boot.")
        try:
            run_command(system.DISABLE_AUTOSTART)
        except exception.ProcessExecutionError:
            raise RuntimeError(_(
                "Command to disable DB2 server on boot failed."))

    def start_db_with_conf_changes(self, config_contents):
        LOG.info(_("Starting DB2 with configuration changes."))
        self.configuration_manager.save_configuration(config_contents)
        self.start_db(True)

    def start_db(self, update_db=False):
        LOG.debug("Start the DB2 server instance.")
        self._enable_db_on_boot()
        try:
            run_command(system.START_DB2)
        except exception.ProcessExecutionError:
            pass

        if not self.status.wait_for_real_status_to_change_to(
                rd_instance.ServiceStatuses.RUNNING,
                self.state_change_wait_time, update_db):
            LOG.error(_("Start of DB2 server instance failed."))
            self.status.end_restart()
            raise RuntimeError(_("Could not start DB2."))

    def stop_db(self, update_db=False, do_not_start_on_reboot=False):
        LOG.debug("Stop the DB2 server instance.")
        if do_not_start_on_reboot:
            self._disable_db_on_boot()
        try:
            run_command(system.STOP_DB2)
        except exception.ProcessExecutionError:
            pass

        if not (self.status.wait_for_real_status_to_change_to(
                rd_instance.ServiceStatuses.SHUTDOWN,
                self.state_change_wait_time, update_db)):
            LOG.error(_("Could not stop DB2."))
            self.status.end_restart()
            raise RuntimeError(_("Could not stop DB2."))

    def restart(self):
        LOG.debug("Restarting DB2 server instance.")
        try:
            self.status.begin_restart()
            self.stop_db()
            self.start_db()
        finally:
            self.status.end_restart()

    def update_overrides(self, context, overrides, remove=False):
        if overrides:
            self.apply_overrides(overrides)

    def remove_overrides(self):
        config = self.configuration_manager.get_user_override()
        self._reset_config(config)
        self.configuration_manager.remove_user_override()

    def apply_overrides(self, overrides):
        self._apply_config(overrides)
        self.configuration_manager.apply_user_override(overrides)

    def _update_dbm_config(self, param, value):
        try:
            run_command(
                system.UPDATE_DBM_CONFIGURATION % {
                    "parameter": param,
                    "value": value})
        except exception.ProcessExecutionError:
            LOG.exception(_("Failed to update config %s"), param)
            raise

    def _reset_config(self, config):
        try:
            for k, v in config.iteritems():
                default_cfg_value = self.dbm_default_config[k]
                self._update_dbm_config(k, default_cfg_value)
        except Exception:
            LOG.exception(_("DB2 configuration reset failed."))
            raise RuntimeError(_("DB2 configuration reset failed."))
        LOG.info(_("DB2 configuration reset completed."))

    def _apply_config(self, config):
        try:
            for k, v in config.items():
                self._update_dbm_config(k, v)
        except Exception:
            LOG.exception(_("DB2 configuration apply failed"))
            raise RuntimeError(_("DB2 configuration apply failed"))
        LOG.info(_("DB2 config apply completed."))


class DB2AppStatus(service.BaseDbStatus):
    """
    Handles all of the status updating for the DB2 guest agent.
    """
    def _get_actual_db_status(self):
        LOG.debug("Getting the status of the DB2 server instance.")
        try:
            out, err = utils.execute_with_timeout(
                system.DB2_STATUS, shell=True)
            if "0" not in out:
                return rd_instance.ServiceStatuses.RUNNING
            else:
                return rd_instance.ServiceStatuses.SHUTDOWN
        except exception.ProcessExecutionError:
            LOG.exception(_("Error getting the DB2 server status."))
            return rd_instance.ServiceStatuses.CRASHED


def run_command(command, superuser=system.DB2_INSTANCE_OWNER,
                timeout=system.TIMEOUT):
    return utils.execute_with_timeout("sudo", "su", "-", superuser, "-c",
                                      command, timeout=timeout)


def create_db2_dir(dir_name):
    if not operating_system.exists(dir_name, True):
        operating_system.create_directory(dir_name,
                                          system.DB2_INSTANCE_OWNER,
                                          system.DB2_INSTANCE_OWNER,
                                          as_root=True)


def remove_db2_dir(dir_name):
    operating_system.remove(dir_name,
                            force=True,
                            as_root=True)


class DB2Admin(object):
    """
    Handles administrative tasks on the DB2 instance.
    """
    def create_database(self, databases):
        """Create the given database(s)."""
        dbName = None
        db_create_failed = []
        LOG.debug("Creating DB2 databases.")
        for item in databases:
            mydb = models.DatastoreSchema.deserialize(item)
            mydb.check_create()
            dbName = mydb.name
            LOG.debug("Creating DB2 database: %s.", dbName)
            try:
                run_command(system.CREATE_DB_COMMAND % {'dbname': dbName})
            except exception.ProcessExecutionError:
                LOG.exception(_(
                    "There was an error creating database: %s."), dbName)
                db_create_failed.append(dbName)
                pass

            '''
            Configure each database to do archive logging for online
            backups. Once the database is configured, it will go in to a
            BACKUP PENDING state. In this state, the database will not
            be accessible for any operations. To get the database back to
            normal mode, we have to do a full offline backup as soon as we
            configure it for archive logging.
            '''
            try:
                if CONF.db2.backup_strategy == 'DB2OnlineBackup':
                    run_command(system.UPDATE_DB_LOG_CONFIGURATION % {
                        'dbname': dbName})
                    run_command(system.RECOVER_FROM_BACKUP_PENDING_MODE % {
                        'dbname': dbName})
            except exception.ProcessExecutionError:
                LOG.exception(_(
                    "There was an error while configuring the database for "
                    "online backup: %s."), dbName)

        if len(db_create_failed) > 0:
            LOG.exception(_("Creating the following databases failed: %s."),
                          db_create_failed)

    def delete_database(self, database):
        """Delete the specified database."""
        dbName = None
        try:
            mydb = models.DatastoreSchema.deserialize(database)
            mydb.check_delete()
            dbName = mydb.name
            LOG.debug("Deleting DB2 database: %s.", dbName)
            run_command(system.DELETE_DB_COMMAND % {'dbname': dbName})
        except exception.ProcessExecutionError:
            LOG.exception(_(
                "There was an error while deleting database:%s."), dbName)
            raise exception.GuestError(original_message=_(
                "Unable to delete database: %s.") % dbName)

    def list_databases(self, limit=None, marker=None, include_marker=False):
        LOG.debug("Listing all the DB2 databases.")
        databases = []
        next_marker = None

        try:
            out, err = run_command(system.LIST_DB_COMMAND)
            dblist = out.split()
            result = iter(dblist)
            count = 0

            if marker is not None:
                try:
                    item = next(result)
                    while item != marker:
                        item = next(result)

                    if item == marker:
                        marker = None
                except StopIteration:
                    pass

            try:
                item = next(result)
                while item:
                    count = count + 1
                    if (limit and count <= limit) or limit is None:
                        db2_db = models.DatastoreSchema(name=item)
                        LOG.debug("database = %s .", item)
                        next_marker = db2_db.name
                        databases.append(db2_db.serialize())
                        item = next(result)
                    else:
                        next_marker = None
                        break
            except StopIteration:
                next_marker = None
            LOG.debug("databases = %s.", str(databases))
        except exception.ProcessExecutionError as pe:
            err_msg = encodeutils.exception_to_unicode(pe)
            LOG.exception(_("An error occurred listing databases: %s."),
                          err_msg)
            pass
        return databases, next_marker

    def create_user(self, users):
        LOG.debug("Creating user(s) for accessing DB2 database(s).")
        try:
            for item in users:
                user = models.DatastoreUser.deserialize(item)
                user.check_create()
                try:
                    LOG.debug("Creating OS user: %s.", user.name)
                    utils.execute_with_timeout(
                        system.CREATE_USER_COMMAND % {
                            'login': user.name, 'login': user.name,
                            'passwd': user.password}, shell=True)
                except exception.ProcessExecutionError as pe:
                    LOG.exception(_("Error creating user: %s."), user.name)
                    continue

                for database in user.databases:
                    mydb = models.DatastoreSchema.deserialize(database)
                    try:
                        LOG.debug("Granting user: %(user)s access to "
                                  "database: %(db)s.",
                                  {'user': user.name, 'db': mydb.name})
                        run_command(system.GRANT_USER_ACCESS % {
                            'dbname': mydb.name, 'login': user.name})
                    except exception.ProcessExecutionError as pe:
                        LOG.debug("Error granting user: %(user)s access to "
                                  "database: %(db)s.",
                                  {'user': user.name, 'db': mydb.name})
                        LOG.debug(pe)
                        pass
        except exception.ProcessExecutionError as pe:
            LOG.exception(_("An error occurred creating users: %s."),
                          pe.message)
            pass

    def delete_user(self, user):
        LOG.debug("Delete a given user.")
        db2_user = models.DatastoreUser.deserialize(user)
        db2_user.check_delete()
        userName = db2_user.name
        user_dbs = db2_user.databases
        LOG.debug("For user %(user)s, databases to be deleted = %(dbs)r.",
                  {'user': userName, 'dbs': user_dbs})

        if len(user_dbs) == 0:
            databases = self.list_access(db2_user.name, None)
        else:
            databases = user_dbs

        LOG.debug("databases for user = %r.", databases)
        for database in databases:
            mydb = models.DatastoreSchema.deserialize(database)
            try:
                run_command(system.REVOKE_USER_ACCESS % {
                    'dbname': mydb.name,
                    'login': userName})
                LOG.debug("Revoked access for user:%(user)s on "
                          "database:%(db)s.",
                          {'user': userName, 'db': mydb.name})
            except exception.ProcessExecutionError as pe:
                LOG.debug("Error occurred while revoking access to %s.",
                          mydb.name)
                pass
            try:
                utils.execute_with_timeout(system.DELETE_USER_COMMAND % {
                    'login': db2_user.name.lower()}, shell=True)
            except exception.ProcessExecutionError as pe:
                LOG.exception(_(
                    "There was an error while deleting user: %s."), pe)
                raise exception.GuestError(original_message=_(
                    "Unable to delete user: %s.") % userName)

    def list_users(self, limit=None, marker=None, include_marker=False):
        LOG.debug(
            "List all users for all the databases in a DB2 server instance.")
        users = []
        user_map = {}
        next_marker = None
        count = 0

        databases, marker = self.list_databases()
        for database in databases:
            db2_db = models.DatastoreSchema.deserialize(database)
            out = None
            try:
                out, err = run_command(
                    system.LIST_DB_USERS % {'dbname': db2_db.name})
            except exception.ProcessExecutionError:
                LOG.debug(
                    "There was an error while listing users for database: %s.",
                    db2_db.name)
                continue

            userlist = []
            for item in out.split('\n'):
                LOG.debug("item = %r", item)
                user = item.split() if item != "" else None
                LOG.debug("user = %r", user)
                if (user is not None
                    and (user[0] not in cfg.get_ignored_users()
                         and user[1] == 'Y')):
                    userlist.append(user[0])
            result = iter(userlist)

            if marker is not None:
                try:
                    item = next(result)
                    while item != marker:
                        item = next(result)

                    if item == marker:
                        marker = None
                except StopIteration:
                    pass

            try:
                item = next(result)

                while item:
                    '''
                    Check if the user has already been discovered. If so,
                    add this database to the database list for this user.
                    '''
                    if item in user_map:
                        db2user = user_map.get(item)
                        db2user.databases = db2_db.name
                        item = next(result)
                        continue
                    '''
                     If this user was not previously discovered, then add
                     this to the user's list.
                    '''
                    count = count + 1
                    if (limit and count <= limit) or limit is None:
                        db2_user = models.DatastoreUser(name=item,
                                                        databases=db2_db.name)
                        users.append(db2_user.serialize())
                        user_map.update({item: db2_user})
                        item = next(result)
                    else:
                        next_marker = None
                        break
            except StopIteration:
                next_marker = None

            if count == limit:
                break
        return users, next_marker

    def get_user(self, username, hostname):
        LOG.debug("Get details of a given database user.")
        user = self._get_user(username, hostname)
        if not user:
            return None
        return user.serialize()

    def _get_user(self, username, hostname):
        LOG.debug("Get details of a given database user %s.", username)
        user = models.DatastoreUser(name=username)
        databases, marker = self.list_databases()
        out = None
        for database in databases:
            db2_db = models.DatastoreSchema.deserialize(database)
            try:
                out, err = run_command(
                    system.LIST_DB_USERS % {'dbname': db2_db.name})
            except exception.ProcessExecutionError:
                LOG.debug(
                    "Error while trying to get the users for database: %s.",
                    db2_db.name)
                continue

            for item in out.split('\n'):
                user_access = item.split() if item != "" else None
                if (user_access is not None and
                        user_access[0].lower() == username.lower() and
                        user_access[1] == 'Y'):
                    user.databases = db2_db.name
                    break
        return user

    def list_access(self, username, hostname):
        """
           Show all the databases to which the user has more than
           USAGE granted.
        """
        LOG.debug("Listing databases that user: %s has access to.", username)
        user = self._get_user(username, hostname)
        return user.databases
