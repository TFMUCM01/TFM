import snowflake.connector
from config import SNOWFLAKE_CONFIG

try:
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_CONFIG['user'],
        password=SNOWFLAKE_CONFIG['password'],
        account=SNOWFLAKE_CONFIG['account'],
        warehouse=SNOWFLAKE_CONFIG['warehouse'],
        database=SNOWFLAKE_CONFIG['database'],
        schema=SNOWFLAKE_CONFIG['schema']
    )
    print("✅ Conexión a Snowflake exitosa.")
    conn.close()
except Exception as e:
    print("❌ Error de conexión:", e)
