from oslo_config import cfg


class DbUtil(object):

    @staticmethod
    def get_python_db_connection_string(config=cfg.CONF):
        database_name = config.database.database_name
        database_server = config.database.host
        database_uid = config.database.username
        database_pwd = config.database.password

        if config.database.use_ssl:
            db_ssl = "?ssl_ca=%s" % config.database.ca_file
        else:
            db_ssl = ''

        return 'mysql+pymysql://%s:%s@%s/%s%s' % (
            database_uid,
            database_pwd,
            database_server,
            database_name,
            db_ssl)

    @staticmethod
    def get_java_db_connection_string(config=cfg.CONF):

        ssl_params = ''
        if config.database.use_ssl:
            ssl_params = "&useSSL=%s&requireSSL=%s" % (
                config.database.use_ssl, config.database.use_ssl
            )
        # FIXME I don't like this, find a better way of managing the conn
        return 'jdbc:%s://%s/%s?user=%s&password=%s%s' % (
            config.database.server_type,
            config.database.host,
            config.database.database_name,
            config.database.username,
            config.database.password,
            ssl_params,
        )
