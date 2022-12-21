from settings import CTL_SCHEMA, CTL_TABLE_LOAD_INFO


# Temp view queries
TEMP_VIEW_DROP = "DROP VIEW IF EXISTS {view};"
TEMP_VIEW_CREATE = """
	CREATE VIEW IF NOT EXISTS {view} AS 
		SELECT *, rowNumberInAllBlocks() AS row_num 
		FROM {table} 
		ORDER BY row_num;
"""
TEMP_VIEW_SELECT = """
	SELECT {columns} 
	FROM {table} 
	WHERE {cond} 
	LIMIT {limit} 
	OFFSET {offset}
"""    	

# Common table queries
DROP_TABLE = """
	DROP TABLE IF EXISTS {table};
"""


METADATA_DELETE = "ALTER TABLE "+'{}.{}'.format(CTL_SCHEMA, CTL_TABLE_LOAD_INFO)+" DELETE WHERE {cond};"
METADATA_INSERT = "INSERT INTO "+'{}.{}'.format(CTL_SCHEMA, CTL_TABLE_LOAD_INFO)+" VALUES ({values});"
METADATA_CREATE = """
    CREATE TABLE IF NOT EXISTS {}.{}
    (
        table String,
        param String,
        value String,
        timestamp DateTime
    ) ENGINE=MergeTree() ORDER BY table;
""".format(CTL_SCHEMA, CTL_TABLE_LOAD_INFO)

MESSAGE_ACTIONS_CREATE = """
	CREATE TABLE IF NOT EXISTS {db}.message_actions
	(
		user_id UInt32,
		reciever_id UInt32,
		time DateTime,
		source String,
		exp_group Int8,
		gender Int8,
		age Int16,
		country String,
		city String,
		os String
	) ENGINE=MergeTree() ORDER BY time;
"""

FEED_ACTIONS_CREATE = """
	CREATE TABLE IF NOT EXISTS {db}.feed_actions
	(
		user_id UInt32,
		post_id UInt32,
		action String,
		time DateTime,
		gender Int8,
		age Int16,
		country String,
		city String,
		os String,
		source String,
		exp_group Int8
	) ENGINE=MergeTree() ORDER BY time;
"""
