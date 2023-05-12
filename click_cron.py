from clickhouse_py  import clickhouse_pandas, clickhouse_logger
import sys

clk = clickhouse_pandas('ga')
q = f"""SELECT * FROM schedules.view_refresh
order by order"""
tables = clk.get_query_results(q)
for scripts in tables.iterrows():
    script_str = scripts[1].script.split(';')
    for line in script_str:
        line = line.strip()
        if line != '':
            print(line)
            try:
                clk.get_query_results(line)
            except:
                print(str(sys.exc_info()[1]))