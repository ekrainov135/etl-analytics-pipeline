from settings import CLICKHOUSE_HOST, DATABASE


def get_ch_config(host=None, db=None):
	host = host or CLICKHOUSE_HOST
	database = db or DATABASE

	config = {'host': host, 'database': database}

	return config
