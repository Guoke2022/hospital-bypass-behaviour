import pandas as pd
import numpy as np
from haversine import haversine, Unit
from pathlib2 import Path
from pypinyin import lazy_pinyin
from Driving_distance import get_road_distances
import osmnx as ox
import time
from functools import partial
from multiprocessing import Pool
import warnings
warnings.filterwarnings('ignore')
pd.set_option('display.max_columns', None)

def f_haversine(lat1, lon1, lat2, lon2):
    point1 = (lat1, lon1)
    point2 = (lat2, lon2)
    distance = haversine(point1, point2, unit=Unit.METERS)

    return distance

def find_closest_hospital_info(row):
    target_hospital_name = row['name']
    distances = df_hospital.apply(lambda x: f_haversine(row['grid_lat_hou'], row['grid_lon_hou'], x['POINT_Y'], x['POINT_X']), axis=1)
    closest_hospitals = distances.nsmallest(len(df_hospital)).index
    hospital_names = df_hospital.loc[closest_hospitals, 'hospital_name'].values

    index = -1
    for i, name in enumerate(hospital_names):
        if name == target_hospital_name:
            index = i+1
            break

    closest_name = df_hospital.loc[closest_hospitals[0], 'hospital_name']
    closest_lat = df_hospital.loc[closest_hospitals[0], 'POINT_Y']
    closest_lon = df_hospital.loc[closest_hospitals[0], 'POINT_X']
    closest_distance = distances[closest_hospitals[0]]/1000

    second_name = df_hospital.loc[closest_hospitals[1], 'hospital_name']
    second_lat = df_hospital.loc[closest_hospitals[1], 'POINT_Y']
    second_lon = df_hospital.loc[closest_hospitals[1], 'POINT_X']
    second_distance = distances[closest_hospitals[1]]/1000

    third_name = df_hospital.loc[closest_hospitals[2], 'hospital_name']
    third_lat = df_hospital.loc[closest_hospitals[2], 'POINT_Y']
    third_lon = df_hospital.loc[closest_hospitals[2], 'POINT_X']
    third_distance = distances[closest_hospitals[2]]/1000

    return index, closest_name, closest_lat, closest_lon, closest_distance, \
           second_name, second_lat, second_lon, second_distance,  \
           third_name, third_lat, third_lon, third_distance


# TODO:Calculating the distance from home to hospital based on road network distance
def home_process_row_closest(index, row, graph, CITY):
    lat_home = row['grid_lat_hou']
    lon_home = row['grid_lon_hou']
    lat_hosp = row['closest_lat']
    lon_hosp = row['closest_lon']
    road_distance = get_road_distances((lon_home, lat_home), (lon_hosp, lat_hosp), graph, cpus=15, CITY=CITY)
    print(CITY, 'closest', index, road_distance)
    return road_distance

def home_process_row_second(index, row, graph, CITY):
    lat_home = row['grid_lat_hou']
    lon_home = row['grid_lon_hou']
    lat_hosp = row['second_lat']
    lon_hosp = row['second_lon']
    road_distance = get_road_distances((lon_home, lat_home), (lon_hosp, lat_hosp), graph, cpus=15, CITY=CITY)

    print(CITY, 'second', index, road_distance)
    return road_distance

def home_process_row_third(index, row, graph, CITY):
    lat_home = row['grid_lat_hou']
    lon_home = row['grid_lon_hou']
    lat_hosp = row['third_lat']
    lon_hosp = row['third_lon']
    road_distance = get_road_distances((lon_home, lat_home), (lon_hosp, lat_hosp), graph, cpus=15, CITY=CITY)
    print(CITY, 'third', index, road_distance)
    return road_distance

def main(CITY):
    graph = ox.load_graphml(filepath=f".../{CITY}.graphml")
    data = pd.read_csv(PATH_RESULT2.joinpath(f'{CITY}NNHI中间文件.csv'), encoding='utf-8')
    df = data.drop_duplicates(subset=['grid_lon_hou', 'grid_lat_hou'])
    df = df.dropna().reset_index(drop=True)

    process_partial_closest = partial(home_process_row_closest, graph=graph, CITY=CITY)
    process_partial_second = partial(home_process_row_second, graph=graph, CITY=CITY)
    process_partial_third = partial(home_process_row_third, graph=graph, CITY=CITY)

    with Pool(processes=processes_num) as pool:
        distances_closest = pool.starmap(process_partial_closest, df.iterrows())
        distances_second = pool.starmap(process_partial_second, df.iterrows())
        distances_third = pool.starmap(process_partial_third, df.iterrows())

    distances_sorted_closest = [distances_closest[i] for i in range(len(distances_closest))]
    df['road_closest'] = distances_sorted_closest
    df['road_closest'] = (df['road_closest'] / 1000).round(2)

    distances_sorted_second = [distances_second[i] for i in range(len(distances_second))]
    df['road_second'] = distances_sorted_second
    df['road_second'] = (df['road_second'] / 1000).round(2)

    distances_sorted_third = [distances_third[i] for i in range(len(distances_third))]
    df['road_third'] = distances_sorted_third
    df['road_third'] = (df['road_third'] / 1000).round(2)

    data = data[['grid_lon_hou', 'grid_lat_hou', 'NNHI', 'name']]
    df = df[['grid_lon_hou', 'grid_lat_hou', 'road_closest', 'road_second', 'road_third']]
    merged_data = pd.merge(data, df, on=['grid_lon_hou', 'grid_lat_hou'], how='inner')
    merged_data.to_csv(PATH_RESULT2.joinpath(CITY + '路网NNHI.csv'), index=False, encoding='utf-8-sig')

processes_num = 10

if __name__ == '__main__':
    starttime = time.time()

    CITY_LIST = ['齐齐哈尔市', '海东市', '普洱市', '日喀则市', '吐鲁番市', '成都市', '武汉市', '深圳市', '上海市', '广州市', '北京市']
    for CITY in CITY_LIST:
        PATH_RESULT2 = Path(f"...")
        PATH_SHP = Path(f"...")
        names_to_exclude = pd.read_csv(PATH_RESULT2.joinpath(f'{CITY}剔除人数异常少的医院列表.csv'), encoding='utf-8')

        df_hospital = pd.read_csv(PATH_SHP.joinpath(f'{CITY}_医疗机构_gcj02新.csv'), encoding='utf-8')
        df_hospital = df_hospital[~df_hospital['name'].isin(names_to_exclude['excluded_hospitals'])]
        df_hospital.rename(columns={'name': 'hospital_name'}, inplace=True)
        df_hospital = df_hospital.reset_index(drop=True)

        main(CITY)

    endtime = time.time()
    print("time consuming:", (endtime - starttime) / 60, "minutes")
