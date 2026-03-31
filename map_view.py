import psycopg2
conn=psycopg2.connect(dbname='ru_db', user='sindri', password='Ebba1505', host='localhost', port=5432)
cur=conn.cursor()
cur.execute("SELECT * FROM public.substation_plant_map ORDER BY substation_id, plant_id")
print(cur.fetchall())
cur.close(); conn.close()
