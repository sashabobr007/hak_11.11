import pandas as pd
from requests import get
from geopy.distance import geodesic
import psycopg2
from scipy.optimize import linear_sum_assignment


def get_kords(address='Москва, Ленинские горы, 1'):

    response = get('https://geocode-maps.yandex.ru/1.x/',
                       params={'apikey': 'c320b927-84f6-44f7-80d6-3283ede7cbe4',
                               'format': 'json',
                               'geocode': address}).json()

    cord = ','.join(
            response['response']['GeoObjectCollection']['featureMember'][0]['GeoObject']['Point']['pos'].split()[
            ::-1])

    return cord

def to_time(time):
    time = str(time)
    hours = time.split('.')[0]
    minutes = round(float('0.' + time.split('.')[1]) * 60)
    if minutes < 10:
        minutes = '0' + str(minutes)
    return '.'.join([hours, str(minutes)])


def to_bd_analitika():
    df = pd.read_csv('worker_analitika.csv')
    conn = psycopg2.connect(database='hr', user='aleksandralekseev', host='localhost', password='')
    cur = conn.cursor()


    cur.execute("delete from worker_analitika;")
#
    # Вставляем данные из датафрейма в таблицу базы данных
    for index, row in df.iterrows():
        #print(row['Адрес'])
        cur.execute(f"INSERT INTO worker_analitika (fio, quantity_tasks, total_time_way, total_distance, total_time_tasks, mean_time_way)"
                    f" VALUES ('{row['Работник']}', '{row['колличество задач']}', '{row['общее время в пути']}', '{round(row['общий километраж'], 2)}', '{round(row['тотал время на выполнение задач'], 2)}', '{row['среднее время в пути']}'"
                    f" );")

    conn.commit()
    cur.close()
    conn.close()


def to_bd_timesheet():
    df = pd.read_csv('timesheet.csv')
    conn = psycopg2.connect(database='hr', user='aleksandralekseev', host='localhost', password='')
    cur = conn.cursor()

    cur.execute("TRUNCATE TABLE timesheet_task;")
#
    # Вставляем данные из датафрейма в таблицу базы данных
    for index, row in df.iterrows():
        #print(row['Адрес'])
        cur.execute(f"INSERT INTO timesheet_task (fio, name, address, point, coordinates_start, coordinates_finish, route_time, distance, time_start, time_finish, priority, status)"
                    f" VALUES ('{row['Работник']}', '{row['Задача']}', '{row['Адрес']}', '{row['Номер точки']}', '{row['Координаты начальной точки']}', '{row['Координаты конечной точки']}', '{row['Примерное время в пути']}', '{row['Расстояние']}',  '{to_time(row['Время начала'])}',"
                    f" '{to_time(row['Время окончания'])}', '{row['Приоритет']}', 0);")

    conn.commit()
    cur.close()
    conn.close()


def to_bd_workers():
    df = pd.read_csv('workers.csv')
    conn = psycopg2.connect(database='hr', user='aleksandralekseev', host='localhost', password='')
    cur = conn.cursor()

    # Очищаем таблицу перед вставкой данных
    cur.execute("TRUNCATE TABLE workers_task;")

    # Вставляем данные из датафрейма в таблицу базы данных
    for index, row in df.iterrows():


        cur.execute("INSERT INTO workers_task (fio, graid, address, current_address, busy_until"
                    ") VALUES (%s, %s, %s, %s, %s);",
                    (row['ФИО'], row['Грейд'], row['Адрес локации'],
                       row['Текущая локация'],
                     to_time(row['Занят до'])))

    conn.commit()
    cur.close()
    conn.close()


def to_bd_day_tasks():
    df = pd.read_csv('day_tasks.csv')
    conn = psycopg2.connect(database='hr', user='aleksandralekseev', host='localhost', password='')
    cur = conn.cursor()

    # Очищаем таблицу перед вставкой данных
    cur.execute("TRUNCATE TABLE day_tasks;")

    # Вставляем данные из датафрейма в таблицу базы данных
    for index, row in df.iterrows():
        if row['Карты и материалы доставлены?'] == 'да':
            all_received = True
        else:
            all_received = False
        cords = get_kords(row['Адрес точки, г. Краснодар'])
        cur.execute("INSERT INTO day_tasks (id, address, date_connected, all_received, days_from_last_card,"
                    " approved_cards, priority, quantity_cards, coordinares) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);",
                    (row['№ точки'], row['Адрес точки, г. Краснодар'], row['Когда подключена точка?'],
                     all_received , row['Кол-во дней после выдачи последней карты'],
                     row['Кол-во выданных карт'], row['Приоритет'], row['Кол-во выданных карт'], cords))

    conn.commit()
    cur.close()
    conn.close()

def algoritm():
    dots = pd.read_csv('dots.csv', sep=';', index_col=0)

    workers = pd.read_csv('workers_algos.csv', sep=';', index_col=0)
    workers['Грейд'] = workers['Грейд'].map({'Синьор': 3, 'Мидл': 2, 'Джун': 1})
    workers['Текущая локация'] = workers['Адрес локации'].copy()
    workers['Занят до'] = 9.

    tasks_description = {1: {'name': 'Доставка карт и материалов', 'time': 1.5},
                         2: {'name': 'Обучение агента', 'time': 2.},
                         3: {'name': 'Выезд на точку для стимулирования выдач', 'time': 4.}}

    def get_task_for_dot(dot):
        if dot['Кол-во дней после выдачи последней карты'] >= 7 and dot['Кол-во одобренных заявок'] or dot[
            'Кол-во дней после выдачи последней карты'] >= 14:
            return 3
        elif 0 < 2 * dot['Кол-во выданных карт'] < dot['Кол-во одобренных заявок']:
            return 2
        elif dot['Когда подключена точка?'] == 'вчера' or dot['Карты и материалы доставлены?'] == 'нет':
            return 1

    day_tasks = dots.copy()
    day_tasks['Задача'] = dots.apply(get_task_for_dot, axis=1)
    day_tasks.dropna(inplace=True)
    day_tasks['Задача'] = day_tasks['Задача'].astype(int)
    day_tasks['Приоритет'] = day_tasks['Задача'].copy()

    yesterday_tasks = pd.read_csv('yesterday_tasks.csv', sep=';', index_col=0)

    for dot, task in yesterday_tasks.iterrows():
        day_tasks.loc[dot] = task

    day_tasks.sort_values(['Приоритет', 'Задача'], inplace=True, ascending=False)
    day_tasks['Прогресс'] = 'Задание переносится на следующий день'

    geocodes = {}

    def get_geocode(address='Москва, Ленинские горы, 1'):
        if address not in geocodes:
            response = get('https://geocode-maps.yandex.ru/1.x/',
                           params={'apikey': 'c320b927-84f6-44f7-80d6-3283ede7cbe4',
                                   'format': 'json',
                                   'geocode': address}).json()

            geocodes[address] = ','.join(
                response['response']['GeoObjectCollection']['featureMember'][0]['GeoObject']['Point']['pos'].split()[
                ::-1])

        return geocodes[address]

    def get_way_times(starts, ends):
        response = get('https://api.routing.yandex.net/v2/distancematrix',
                       params={'apikey': 'ccd24ae0-58fc-4a41-bfde-b65930caab3a',
                               'origins': '|'.join(get_geocode(start) for start in starts),
                               'destinations': '|'.join(get_geocode(end) for end in ends)}).json()

        way_times = pd.DataFrame(index=ends, columns=starts)
        distances = pd.DataFrame(index=ends, columns=starts)

        for column_idx, row in enumerate(response['rows']):
            for row_idx, data in enumerate(row['elements']):
                way_times.iloc[row_idx, column_idx] = data['duration']['value']
                distances.iloc[row_idx, column_idx] = data['distance']['value'] / 1000
        return way_times.astype(int), distances

    timesheet = pd.DataFrame(columns=['Работник',
                                      'Задача',
                                      'Приоритет',
                                      'Адрес',
                                      'Номер точки',
                                      'Примерное время в пути',
                                      'Расстояние',
                                      'Время начала',
                                      'Время окончания',
                                      'Координаты начальной точки',
                                      'Координаты конечной точки'])

    yesterday_tasks = pd.DataFrame(columns=yesterday_tasks.columns)

    def get_tasks(priority=3, grade=3):
        tasks = day_tasks[(day_tasks['Приоритет'] == priority) & \
                          (day_tasks['Прогресс'] != 'Задание распределено на текущий день') & \
                          (day_tasks['Задача'] == grade)]

        if len(tasks) == 0:
            return 'Таких задач нет, милорд'

        possible_workers = workers[(workers['Грейд'] >= grade) & \
                                   (workers['Занят до'] + tasks_description[priority]['time'] <= 17.)]

        if len(possible_workers) == 0:
            return 'Работников не хватило, милорд'

        way_times, distances = get_way_times(starts=possible_workers['Текущая локация'],
                                             ends=tasks['Адрес точки, г. Краснодар'])

        distances.index = way_times.index = tasks['Адрес точки, г. Краснодар'].index
        distances.columns = way_times.columns = possible_workers['Текущая локация'].index

        dot_idxs, worker_idxs = linear_sum_assignment(way_times)

        for dot, fio in zip(way_times.index[dot_idxs], way_times.columns[worker_idxs]):
            way_time = way_times.loc[dot, fio] // 60

            timesheet.loc[len(timesheet)] = [fio,
                                             tasks_description[grade]['name'],
                                             priority,
                                             tasks.loc[dot, 'Адрес точки, г. Краснодар'],
                                             dot,
                                             way_time,
                                             distances.loc[dot, fio],
                                             workers.loc[fio, 'Занят до'],
                                             workers.loc[fio, 'Занят до'] + tasks_description[grade][
                                                 'time'] + way_time / 60,
                                             get_geocode(workers.loc[fio, 'Текущая локация']),
                                             get_geocode(tasks.loc[dot, 'Адрес точки, г. Краснодар'])]

            workers.loc[fio, 'Текущая локация'] = tasks.loc[dot, 'Адрес точки, г. Краснодар']
            workers.loc[fio, 'Занят до'] += tasks_description[priority]['time'] + way_time / 60

            day_tasks.loc[dot, 'Прогресс'] = 'Задание распределено на текущий день'

        if len(possible_workers) >= len(tasks):
            return 'Все задачи распределены, милорд'
        else:
            return get_tasks(priority=priority, grade=grade)

    for priority in (3, 2, 1):
        for grade in (3, 2, 1):
            print(f"Задачи приоритета = {priority}, грейда = {grade} распределяются", end=': ')
            flag = get_tasks(priority=priority, grade=grade)
            print(flag)

    tomorrow_tasks = day_tasks[day_tasks['Прогресс'] == 'Задание переносится на следующий день']
    tomorrow_tasks['Приоритет'] = 3
    tomorrow_tasks.to_csv('yesterday_tasks.csv', sep=';')


    day_tasks.to_csv("day_tasks.csv")
    timesheet.to_csv("timesheet.csv")
    workers.to_csv("workers.csv")

def append_from_exel(filename):
    df = pd.read_excel(filename, index_col=0)
    df_dots = pd.read_csv('dots.csv', sep=';', index_col=0)
    df_dots = pd.concat([df_dots, df])
    df_dots.to_csv("dots.csv", sep=';')
    return 'success'

def analitica():
    day_tasks = pd.read_csv('day_tasks.csv')
    sum_all_today = day_tasks.query('Прогресс == "Задание распределено на текущий день"').shape[0]
    sum_all = day_tasks.shape[0]
    sum_high_priority = day_tasks.query('Приоритет == 3').shape[0]
    sum_medium_priority = day_tasks.query('Приоритет == 2').shape[0]
    sum_low_priority = day_tasks.query('Приоритет == 1').shape[0]
    sum_high_priority_today = \
    day_tasks.query('Прогресс == "Задание распределено на текущий день" and Приоритет == 3').shape[0]
    sum_medium_priority_today = \
    day_tasks.query('Прогресс == "Задание распределено на текущий день" and Приоритет == 2').shape[0]
    sum_low_priority_today = \
    day_tasks.query('Прогресс == "Задание распределено на текущий день" and Приоритет == 1').shape[0]
    timesheet = pd.read_csv('timesheet.csv')
    mean_time = timesheet['Примерное время в пути'].mean().round(2)
    workers = pd.read_csv('workers_algos.csv', sep=';')
    workers = workers.rename(columns={'ФИО': 'Работник'})
    timesheet = timesheet.merge(workers, on='Работник')
    timesheet['diff'] = timesheet['Время окончания'] - timesheet['Время начала']
    mean_time_jun = timesheet.query('Грейд == "Джун"').agg({'diff': 'mean'}).iloc[0].round(2)
    mean_time_midle = timesheet.query('Грейд == "Мидл"').agg({'diff': 'mean'}).iloc[0].round(2)
    mean_time_senior = timesheet.query('Грейд == "Синьор"').agg({'diff': 'mean'}).iloc[0].round(2)
    worker_analitika = timesheet.groupby('Работник', as_index=False).agg(
        {'Адрес': 'count', 'Примерное время в пути': 'sum', 'Расстояние': 'sum', 'diff': 'sum'}) \
        .rename(columns={'Адрес': 'колличество задач', 'Примерное время в пути': 'общее время в пути',
                         'Расстояние': 'общий километраж', 'diff': 'тотал время на выполнение задач'})
    worker_analitika = worker_analitika.merge(
        timesheet.groupby('Работник', as_index=False).agg({'Примерное время в пути': 'mean'}).rename(
            columns={'Примерное время в пути': 'среднее время в пути'}), on='Работник')
    worker_analitika.to_csv('worker_analitika.csv')

    result ={
        "sum_all_today": sum_all_today,
        "sum_all" : sum_all,
        "sum_high_priority" : sum_high_priority,
        "sum_medium_priority":sum_medium_priority,
        "sum_low_priority":sum_low_priority,
        "sum_high_priority_today":sum_high_priority_today,
        "sum_medium_priority_today":sum_medium_priority_today,
        "sum_low_priority_today":sum_low_priority_today,
        "mean_time":mean_time,
        "mean_time_jun":mean_time_jun,
        "mean_time_midle":mean_time_midle,
        "mean_time_senior":mean_time_senior
    }
    return result


#
#if __name__ == "__main__":
    #print(get_kords())
    #to_bd_analitika()
    #algoritm()
    #to_bd_day_tasks()
#     to_bd_workers()
#     to_bd_timesheet()
