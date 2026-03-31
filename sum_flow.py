import psycopg2
conn=psycopg2.connect(dbname='ru_db', user='sindri', password='Ebba1505', host='localhost', port=5432)
cur=conn.cursor()
cur.execute("SELECT name, SUM(total_production_kwh), SUM(delivered_pwr) FROM public.energy_flow WHERE year=2025 GROUP BY name")
for row in cur.fetchall():
    print(row)
cur.close(); conn.close()
