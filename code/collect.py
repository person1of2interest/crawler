import sys
import sqlite3
import csv


def create_links_db(db_name: str = 'crawler.sqlite',
                    file_name: str = 'links.txt',
                    path_to_folder: str = 'logs'):
    """
    Создаем БД, состоящую из посещенных страниц

    Параметры:
    ----------
       db_name (str): название будущей БД
       file_name (str): файл с посещенными страницами
       path_to_folder (str): путь к папке с файлом
       			     file_name
    """
    conn = sqlite3.connect(path_to_folder + '/' + db_name)
    cursor = conn.cursor()
    cursor.execute('CREATE TABLE links (name VARCHAR NOT NULL)')

    # загружаем файл в БД
    with open(path_to_folder + '/' + file_name, 'r') as file:
        for line in file:
            link = line.strip()
            if link and 'mailto' not in link:
                cursor.execute(
                    'INSERT INTO links (name) VALUES (?)', (link,)
                )

    # сохраняем изменения
    conn.commit()
    conn.close()

def get_stats_from_db(cursor: sqlite3.Cursor,
		      home_domain: str = 'spbu.ru') -> (int, int):
    """
    Собираем статистику по числу
    поддоменов и внутренних страниц
    
    Параметры:
    ----------
    	home_domain (str): домен
    
    Возвращает:
    -----------
    	subdomains_count (int): число поддоменов
    	internal_pages_count (int): число внутр-х страниц
    """
    query_1 = '''
        WITH t AS (
            SELECT 
                name,
                SUBSTR(name, 1, INSTR(name, ?) - 1) AS name_before
            FROM links
            WHERE INSTR(name, ?) > 0
        ),

        t1 AS (
            SELECT
                name,
                SUBSTR(
			name_before,
			INSTR(name_before, "https://") + 8
                ) AS subdomain
            FROM t
            WHERE LENGTH(name_before) > 8
        )

        SELECT COUNT(DISTINCT subdomain)
        FROM t1
    '''
    cursor.execute(query_1, (home_domain, home_domain))
    subdomains_count = cursor.fetchall()[0][0]
    
    query_2 = '''
        SELECT COUNT(name) AS internal_pages
        FROM links
        WHERE name LIKE ? or name LIKE ?
    '''
    cursor.execute(
        query_2,
        ("https://" + home_domain + "/%", "http://" + home_domain + "/%")
    )
    internal_pages_count = cursor.fetchall()[0][0]

    return subdomains_count, internal_pages_count


if __name__ == "__main__":
    home_domain = sys.argv[1]
    path_to_folder = 'logs'
    db_name = 'crawler.sqlite'
    create_links_db(
    	db_name=db_name,
    	file_name='links.txt',
    	path_to_folder=path_to_folder
    )

    conn = sqlite3.connect(path_to_folder + '/' + db_name)
    cursor = conn.cursor()

    subdomains_count, internal_pages_count = get_stats_from_db(
        cursor,
        home_domain=home_domain
    )
    conn.close()

    with open(path_to_folder + '/' + 'stats.csv', 'r') as stats_file:
        reader = csv.reader(stats_file, delimiter=',')
        header = next(reader)
        values = next(reader)
        for i in range(len(header)):
            print(f'{header[i]}: {values[i]}')
    
    print(f'Subdomains: {subdomains_count}')
    print(f'Internal pages: {internal_pages_count}')




