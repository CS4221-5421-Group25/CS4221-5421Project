#!/usr/bin/python
import psycopg2
from numpy import random

def connect():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:

        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(
            host="localhost",
            database="postgres",
            user="postgres",
            password="postgres")
		
        # create a cursor
        cur = conn.cursor()
        print('PostgreSQL database connected.\n')
        
        # Four sections:
        # Section 1: 6 Index Types
        # Section 2: Multicolumn Index
        # Section 3: Partial Index
        # Section 4: Unique Index

        # Section 1: Index Types
        print('Section 1: Index Types\n')

	    # 1.1 B-Tree
        print('======================🔥 B-Tree Experiment======================\n')
        print('1️⃣  Query based on order_id without indexing\n')
        print('✅ SQL Statement: \nEXPLAIN ANALYZE SELECT * FROM sales_data WHERE order_id in (198441684, 562844884, 838541638);')
        cur.execute("EXPLAIN ANALYZE SELECT * FROM sales_data WHERE order_id in (198441684, 562844884, 838541638);")
        data = cur.fetchall()
        print('\n✅ Execution result:')
        print(data)

        print('\n2️⃣  Query based on order_id with B-Tree indexing on order_id\n')

        print('✅ Create B-tree index on order_id column:')
        print('CREATE INDEX index_order_id on sales_data(order_id);\n')
        cur.execute("CREATE INDEX index_order_id on sales_data(order_id);")

        print('✅ Rerun the SELECT query\n')
        cur.execute("EXPLAIN ANALYZE SELECT * FROM sales_data WHERE order_id in (198441684, 562844884, 838541638);")
        data = cur.fetchall()
        print('✅ Execution result:')
        print(data)

        print('\n3️⃣  Clear existing index')
        cur.execute("DROP INDEX if exists index_order_id;")

        print('\n======================End of B-Tree Experiement======================\n')

        # Hash
        print('======================🔥 Hash Experiment======================\n')
        print('1️⃣  Query based on order_id without indexing\n')

        print('✅ SQL Statement: \nEXPLAIN ANALYZE SELECT * FROM sales_data WHERE order_id > 800000000;')
        cur.execute("EXPLAIN ANALYZE SELECT * FROM sales_data WHERE order_id > 800000000;")
        data = cur.fetchall()
        
        print('\n✅ Execution result:')
        print(data)

        print('\n2️⃣  Query based on order_id with Hash indexing on order_id\n')

        print('✅ Create Hash index on order_id column:')
        print('CREATE INDEX hash_index_order_id on sales_data using HASH(order_id);\n')
        cur.execute("CREATE INDEX hash_order_id on sales_data using HASH(\"order_id\");")

        print('✅ Rerun the SELECT query\n')
        cur.execute("EXPLAIN ANALYZE SELECT * FROM sales_data WHERE order_id > 800000000;")
        data = cur.fetchall()
        print('✅ Execution result:')
        print(data)

        print('\n3️⃣  Clear existing index')
        cur.execute("DROP INDEX if exists hash_index_order_id;")

        print('\n======================End of Hash Experiement======================\n')
       
        # Gin
        print('======================🔥 Gin Experiment======================\n')
        print('1️⃣  Query based on region without indexing\n')

        print('✅ SQL Statement: \nEXPLAIN ANALYZE SELECT count(*) FROM sales_data WHERE region ilike %Nor%')
        cur.execute("EXPLAIN ANALYZE SELECT count(*) FROM sales_data WHERE region ilike \'%Nor%\';")
        data = cur.fetchall()

        print('\n✅ Execution result:')
        print(data)

        print('\n2️⃣  Query based on region with Gin indexing on region\n')

        print('✅ Create Gin index on region column:')
        print('CREATE EXTENSION pg_trgm;')
        print('CREATE INDEX gin_index_region on sales_data using gin(region gin_trgm_ops);\n')
        cur.execute("CREATE EXTENSION pg_trgm;")
        cur.execute("CREATE INDEX gin_region on sales_data using gin(region gin_trgm_ops);")
        
        print('✅ Rerun the SELECT query\n')
        cur.execute("EXPLAIN ANALYZE SELECT count(*) FROM sales_data WHERE region ilike \'%Nor%\';")
        data = cur.fetchall()
        
        print('✅ Execution result:')
        print(data)

        print('\n3️⃣  Clear existing index')
        cur.execute("DROP INDEX if exists gin_index_region;")

        print('\n======================End of Gin Experiement======================\n')

        # BRIN
        print('======================🔥 BRIN Experiment======================\n')
        print('1️⃣  Query based on ship_date without indexing\n')

        print('✅ SQL Statement: \nEXPLAIN ANALYZE SELECT count(*) FROM sorted_sales_data where ship_date >= \'2010-01-01\' and ship_date <= \'2010-01-08\';')
        cur.execute("EXPLAIN ANALYZE SELECT count(*) FROM sorted_sales_data where ship_date >= '2010-01-01' and ship_date <= '2010-01-08';")
        data = cur.fetchall()
        
        print('\n✅ Execution result:')
        print(data)

        print('\n2️⃣  Query based on ship_date with Brin indexing on order_id\n')
        
        print('✅ Create Gin index on ship_date column:')
        print('CREATE INDEX brin_index_ship_date on sorted_sales_data using brin(ship_date);\n')
        cur.execute("CREATE INDEX brin_index_ship_date on sorted_sales_data using brin(ship_date);")
        
        print('✅ Rerun the SELECT query\n')
        cur.execute("EXPLAIN ANALYZE SELECT count(*) FROM sorted_sales_data where ship_date = '2010-01-01' and ship_date <= '2010-01-08';")
        data = cur.fetchall()
        
        print('✅ Execution result:')
        print(data)

        print('\n3️⃣  Clear existing index')
        cur.execute("DROP INDEX if exists brin_index_ship_date;")

        print('\n======================End of BRIN Experiement======================\n')

        # GiST
        print('======================🔥 GiST Experiment======================\n')
        print('1️⃣  Query based on p without indexing\n')
        
        print('✅ SQL Statement: \nEXPLAIN ANALYZE SELECT * FROM geom_data ORDER BY p <-> point \'(120,450)\' LIMIT 15;')
        cur.execute("EXPLAIN ANALYZE SELECT * FROM geom_data ORDER BY p <-> point '(120,450)' LIMIT 15;")
        data = cur.fetchall()
        
        print('\n✅ Execution result:')
        print(data)

        print('\n2️⃣  Query based on p with GiST indexing on p\n')

        print('✅ Create Gin index on p column:')
        print('CREATE INDEX gist_index_p on geom_data using gist(p);')
        cur.execute("CREATE INDEX gist_index_p on geom_data using gist(p);")

        print('\n✅ Rerun the SELECT query\n')
        cur.execute("EXPLAIN ANALYZE SELECT * FROM geom_data ORDER BY p <-> point '(120,450)' LIMIT 15;")
        data = cur.fetchall()

        print('✅ Execution result:')
        print(data)

        print('\n3️⃣  Clear existing index')
        cur.execute("DROP INDEX if exists gist_index_p;")

        print('\n======================End of GiST Experiement======================\n')

        # SP-GiST
        print('======================🔥 SP-GiST Experiment======================\n')
        print('1️⃣  Query based on p without indexing\n')

        print('✅ SQL Statement: \nEXPLAIN ANALYZE SELECT * FROM geom_data ORDER BY p <-> point \'(120,450)\' LIMIT 15;')
        cur.execute("EXPLAIN ANALYZE SELECT * FROM geom_data ORDER BY p <-> point '(180,320)' LIMIT 8;")
        data = cur.fetchall()
        
        print('\n✅ Execution result:')
        print(data)

        print('\n2️⃣  Query based on p with SP-GiST indexing on p\n')

        print('✅ Create SP-GiST index on p column:')
        print('CREATE INDEX spgist_index_p on geom_data using spgist(p);')
        cur.execute("CREATE INDEX spgist_index_p on geom_data using spgist(p);")

        print('\n✅ Rerun the SELECT query\n')
        cur.execute("EXPLAIN ANALYZE SELECT * FROM geom_data ORDER BY p <-> point '(180,320)' LIMIT 8;")
        data = cur.fetchall()
        
        print('✅ Execution result:')
        print(data)

        print('\n3️⃣  Clear existing index')
        cur.execute("DROP INDEX if exists spgist_index_p;")

        print('\n======================End of SP-GiST Experiement======================\n')

        # B-Tree MultiColumns
        print('Section 2: MultiColumn Index\n')
        print('======================🔥 B-Tree MultiColumns Experiment======================\n')
        
        print('🟡 Part 1: Queries without any indexing\n')

        print('1️⃣  Query based on units_sold and ship_date without indexing\n')

        print('✅ SQL Statement: EXPLAIN ANALYZE SELECT * FROM sales_data WHERE ship_date = \'2020-05-24\' and units_sold >= 1000;\n')
        cur.execute("EXPLAIN ANALYZE SELECT * FROM sales_data WHERE ship_date = '2020-05-24' and units_sold >= 1000;")
        result = cur.fetchall()

        print('\n✅ Execution result:')
        print(result)

        print('\n2️⃣  Query based on ship_date without indexing\n')

        print('✅ SQL Statement: EXPLAIN ANALYZE SELECT * FROM sales_data WHERE ship_date = \'2019-08-30\'\n')
        cur.execute("EXPLAIN ANALYZE SELECT count(*) FROM sales_data WHERE ship_date = '2019-08-30';")
        result = cur.fetchall()

        print('\n✅ Execution result:')
        print(result)

        print('\n3️⃣  Query based on units_sold without indexing\n')
        print('✅ SQL Statement: EXPLAIN ANALYZE SELECT count(*) FROM sales_data WHERE units_sold > 5000;')
        cur.execute("EXPLAIN ANALYZE SELECT count(*) FROM sales_data WHERE units_sold > 5000;")
        result = cur.fetchall()

        print('\n✅ Execution result:')
        print(result)

        print('\n🟡 Part 2: Queries with multicolumn indexing\n')

        print('✅ Create multicolumn index on ship_date and units_sold column with ship_date leading:')
        print('CREATE INDEX index_multi_btree_index on sales_data(ship_date, units_sold);')
        cur.execute("CREATE INDEX index_multi_btree_index on sales_data(ship_date, units_sold);")

        print('\n1️⃣  Rerun Part 1.1 query \n')
        cur.execute("EXPLAIN ANALYZE SELECT * FROM sales_data WHERE ship_date = '2020-05-24' and units_sold >= 1000;")
        result = cur.fetchall()

        print('✅ Execution result:')
        print(result)

        print('\n2️⃣  Rerun Part 1.2 query \n')
        cur.execute("EXPLAIN ANALYZE SELECT count(*) FROM sales_data WHERE ship_date = '2019-08-30';")
        result = cur.fetchall()
        
        print('✅ Execution result:')
        print(result)

        print('\n3️⃣  Rerun Part 1.3 query\n')
        cur.execute("EXPLAIN ANALYZE SELECT count(*) FROM sales_data WHERE units_sold > 5000;")
        result = cur.fetchall()

        print('✅ Execution result:')
        print(result)

        print('\n🟡 Part 3: Clear existing index')
        cur.execute("DROP INDEX if exists index_multi_btree_index;")

        print('\n======================End of B-Tree MultiColumns Experiement======================\n')


        # B-Tree Unique
        print('======================🔥 B-Tree Unique Index Experiment======================\n')

        print('1️⃣  Query based on order_id without indexing')
        print('✅ SQL Statement: \nEXPLAIN ANALYZE SELECT * FROM sales_data WHERE order_id in (198441684, 562844884, 838541638);')
        cur.execute("EXPLAIN ANALYZE SELECT * FROM sales_data WHERE order_id in (198441684, 562844884, 838541638);")
        result = cur.fetchall()
        
        print('\n✅ Execution result:')
        print(result)

        print('\n2️⃣  Query based on order_id with B-Tree indexing on order_id\n')

        print('✅ Create B-Tree index on order_id column:')
        print('CREATE UNIQUE INDEX unique_index_order_id on sales_data(order_id);')
        cur.execute("CREATE UNIQUE INDEX unique_index_order_id on sales_data(order_id);")

        print('\n✅ Rerun the SELECT query\n')
        cur.execute("EXPLAIN ANALYZE SELECT * FROM sales_data WHERE order_id in (198441684, 562844884, 838541638);")
        result = cur.fetchall()

        print('✅ Execution result:')
        print(result)

        print('\n3️⃣  Insert a new row into sales_data with existing order_id\n')

        print('✅ Insertion statement:')
        print('INSERT INTO sales_data(region, country, order_date, order_id) values(\'Europe\', \'Luxembourg\', \'2019-08-30\', 621165549);')
        cur.execute("INSERT INTO sales_data(region, country, order_date, order_id) values('Europe', 'Luxembourg', '2019-08-30', 621165549);")
        result = cur.fetchall()

        print('')
        print(result)

        print('4️⃣ Clear existing index')
        cur.execute("DROP INDEX if exists unique_index_order_id;")

        print('\n======================End of B-Tree Unique Index Experiement======================\n')

        # Partial index
        print('======================🔥 Partial Index Experiment======================\n')
        print('1️⃣  Query based on order_id without indexing\n')

        print('✅ SQL Statement: \nEXPLAIN ANALYZE SELECT * FROM sales_data WHERE region = \'North America\' and order_id = 160068500;')
        cur.execute("EXPLAIN ANALYZE SELECT * FROM sales_data WHERE region = 'North America' and order_id = 160068500;")
        result = cur.fetchall()

        print('\n✅ Execution result:')
        print(result)

        print('\n2️⃣  Query based on region with partial indexing on order_id\n')

        print('\n✅ Create partial index on order_id column of rows from North America\n')
        print('CREATE INDEX patrial_index_order_id on sales_data(order_id) WHERE region = \'North America\';')
        cur.execute("CREATE INDEX patrial_index_order_id on sales_data(order_id) WHERE region = 'North America';")
        
        print('\n✅ Rerun the SELECT query\n')
        cur.execute("EXPLAIN ANALYZE SELECT * FROM sales_data WHERE region = 'North America' and order_id = 160068500;")
        result = cur.fetchall()
        
        print('✅ Execution result:')
        print(result)

        print('\n3️⃣  Clear existing index')
        cur.execute("DROP INDEX if exists patrial_index_order_id;")

        print('\n======================End of Partial Index Experiement======================\n')


	# close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')


if __name__ == '__main__':
    connect()

