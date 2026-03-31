import psycopg2
conn=psycopg2.connect(dbname='ru_db', user='sindri', password='Ebba1505', host='localhost', port=5432)
cur=conn.cursor()
cur.execute("SELECT id, type FROM public.pwr_plant ORDER BY id")
print(cur.fetchall())
cur.close(); conn.close()
