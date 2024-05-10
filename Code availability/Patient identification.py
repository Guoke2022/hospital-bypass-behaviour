import pandas as pd
import time
from pathlib2 import Path
import geopandas as gpd
from shapely.geometry import Polygon, Point
from multiprocessing import Pool, cpu_count
from functools import partial
from multiprocessing import current_process
import subprocess
import warnings
warnings.filterwarnings('ignore') 
pd.set_option('display.max_columns', None)

starttime_all = time.time()

# TODO:mem_usage
def mem_usage(pandas_obj):
    if isinstance(pandas_obj, pd.DataFrame):
        usage_b = pandas_obj.memory_usage(deep=True).sum()
    else:  # we assume if not a df it's a series
        usage_b = pandas_obj.memory_usage(deep=True)
    usage_mb = usage_b / 1024 ** 2  # convert bytes to megabytes
    return "{:03.2f} MB".format(usage_mb)

# TODO:Filter_lonlat (Relax standards appropriately)
def Filter_lonlat(CITY):
    if CITY == '北京市':
        LON_MIN, LON_MAX, LAT_MIN, LAT_MAX = 115.5, 117.6, 39.2, 41.8
    elif CITY == '上海市':
        LON_MIN, LON_MAX, LAT_MIN, LAT_MAX = 120.6, 122.5, 30.4, 32.1
    elif CITY == '广州市':
        LON_MIN, LON_MAX, LAT_MIN, LAT_MAX = 112.7, 114.2, 22.2, 24
    elif CITY == '深圳市':
        LON_MIN, LON_MAX, LAT_MIN, LAT_MAX = 113.5, 114.8, 22.2, 23
    elif CITY == '成都市':
        LON_MIN, LON_MAX, LAT_MIN, LAT_MAX = 102.6, 105, 29.9, 31.6
    elif CITY == '武汉市':
        LON_MIN, LON_MAX, LAT_MIN, LAT_MAX = 113.5, 114.9, 29.8, 31.2
    elif CITY == '齐齐哈尔市':
        LON_MIN, LON_MAX, LAT_MIN, LAT_MAX = 122, 127, 45, 49
    elif CITY == '海东市':
        LON_MIN, LON_MAX, LAT_MIN, LAT_MAX = 100.5, 103.3, 35.2, 37.3
    elif CITY == '普洱市':
        LON_MIN, LON_MAX, LAT_MIN, LAT_MAX = 99, 102.5, 21.9, 25
    elif CITY == '日喀则市':
        LON_MIN, LON_MAX, LAT_MIN, LAT_MAX = 81, 91, 26, 33
    elif CITY == '吐鲁番市':
        LON_MIN, LON_MAX, LAT_MIN, LAT_MAX = 87, 92, 41, 44

    return LAT_MIN, LAT_MAX, LON_MIN, LON_MAX


def data_cleaning(chunk):
    # TODO: data_cleaning
    chunk.columns = ['id', 'lon', 'lat', 'datetime']
    chunk = chunk[chunk['lon'].str.contains('[a-zA-Z]|[\u4e00-\u9fff]', na=False) == False]
    chunk = chunk[chunk['lat'].str.contains('[a-zA-Z]|[\u4e00-\u9fff]', na=False) == False]
    chunk['id'] = chunk['id'].astype('category')
    chunk['lon'] = chunk['lon'].astype('float32')
    chunk['lat'] = chunk['lat'].astype('float32')

    LAT_MIN, LAT_MAX, LON_MIN, LON_MAX = Filter_lonlat(CITY)
    chunk = chunk[
        (chunk['lon'] < LON_MAX) & (chunk['lon'] > LON_MIN) & (chunk['lat'] < LAT_MAX) & (chunk['lat'] > LAT_MIN)]
    print('mem_usage：', mem_usage(chunk))
    return chunk

def chunk_GeoData(chunk, polygonShp):
    chunk = data_cleaning(chunk)

    geometry = [Point(lon, lat) for lon, lat in zip(chunk['lon'], chunk['lat'])]
    gdf_point = gpd.GeoDataFrame(chunk, geometry=geometry)

    gdf_points = gpd.sjoin(gdf_point, polygonShp, how="inner", op="within")[['id', 'lon', 'lat', 'datetime',
                                                                             'name']]
    if gdf_points.empty:
        print("gdf_points is empty")

    return gdf_points

def preprocess_chunk(file, polygonShp):

    # Use geopandas to filter out users passing through hospitals

    filtered_data_tatal = []
    N = 1

    data = pd.read_csv(file, usecols=['脱敏ID', '开始时间', '经度', '纬度'],
                       dtype={'脱敏ID': str, '经度': str, '纬度': str},
                       chunksize=CHUNKSIZE, error_bad_lines=False, encoding='utf-8')

    for chunk in data:
        starttime1 = time.time()
        filtered_data_tatal.append(chunk_GeoData(chunk, polygonShp))
        endtime1 = time.time()
        print(CITY, file.stem, "Number of cycles:", N, "time-consuming", (endtime1 - starttime1) / 60, "minutes")
        N += 1

    filtered_data_total_result = pd.concat(filtered_data_tatal, axis=0)

    return filtered_data_total_result

def process_time(file, polygonShp):
    starttime_one_day = time.time()

    filtered_data_tatal_result = preprocess_chunk(file, polygonShp)

    OUTPUT_PATH1 = Path(PATH_RESULT1).joinpath(CITY + file.stem + '途经医院用户（仅途经点）' + '.csv')
    filtered_data_tatal_result.to_csv(OUTPUT_PATH1, index=False, encoding='utf-8-sig')
    print(CITY + file.stem + '已输出 途经医院用户（仅途经点）.csv')

    # TODO: Stay time screening
    id_unique_list = filtered_data_tatal_result["id"].unique()

    df_tatal = []
    i = 1

    for id in id_unique_list:
        df = filtered_data_tatal_result[filtered_data_tatal_result["id"] == id]

        min_time_cut = 30 * 60
        max_time_cut = 10 * 60 * 60

        df['datetime'] = pd.to_datetime(df['datetime'])
        df.sort_values('datetime', inplace=True)

        start_time = df['datetime'].min()
        end_time = df['datetime'].max()
        duration_time = (end_time - start_time).total_seconds()

        if (start_time.hour >= 7) & (end_time.hour < 19):
            if (duration_time >= min_time_cut) & (duration_time <= max_time_cut):
                df['start_time'] = start_time
                df['end_time'] = end_time
                df['duration(min)'] = duration_time / 60
                df = df.drop('datetime', axis=1)
                df = df.drop_duplicates(subset=['id'])
                df_tatal.append(df)

                print(file.stem, i)
                i += 1

    df_tatal_result = pd.concat(df_tatal, axis=0)
    OUTPUT_PATH2 = Path(PATH_RESULT1).joinpath(CITY + file.stem + '潜在单日就医用户（已计算停留时间）' + '.csv')
    df_tatal_result.to_csv(OUTPUT_PATH2, index=False, encoding='utf-8-sig')
    print(CITY + file.stem + '已输出 潜在单日就医用户（已计算停留时间）.csv')

    endtime_one_day = time.time()
    print("Processing data for one day takes:", (endtime_one_day - starttime_one_day) / 60, "minutes")

    # Delete files after processing
    file.unlink()  
    print(f"Process {current_process().name}: Deleted {file}")

def unzip_file(zip_file, target_dir):
    command = f'"C:\\Program Files\\7-Zip\\7z.exe" x "{zip_file}" -o"{target_dir}" -p{PASSWORD}'
    subprocess.run(command, shell=True)

def unzip_files(zip_files, target_dir, max_files):
    count = 0
    for zip_file in zip_files:
        if count >= max_files:
            break
        unzip_file(zip_file, target_dir)
        count += 1

def process_main(zip_files, target_dir):
    processed_files = []

    while len(zip_files) > 0 or len(list(Path(target_dir).glob('*.csv'))) > 0:
        pool_process = Pool(processes=NUM_PROCESSES)
        # Decompress the file until the number of files in PATH-DATA reaches NUM-PROCESSES
        while len(list(Path(target_dir).glob('*.csv'))) < NUM_PROCESSES and len(zip_files) > 0:
            next_zip_file = zip_files.pop(0)
            if next_zip_file not in processed_files:
                unzip_file(next_zip_file, target_dir)
                processed_files.append(next_zip_file)

        for file in sorted(Path(target_dir).glob('*.csv')):
            pool_process.apply_async(process_time, args=(file, polygonShp))

        pool_process.close()
        pool_process.join()


# TODO: Parameter settings
CITY = '...'

PATH_SHP = f"..."
polygonShp = gpd.read_file(PATH_SHP)[['name', 'area', 'geometry']]
PATH_ZIP = f"..."
PATH_DATA = Path(f"...")
PATH_RESULT1 = f"..."

CHUNKSIZE = 10000000
NUM_PROCESSES = 6


if __name__ == '__main__':
    zip_files = sorted(Path(PATH_ZIP).glob('*.zip'))
    process_main(zip_files, PATH_DATA)

    endtime_all = time.time()
    print(CITY, "Full process time consumption:", (endtime_all - starttime_all) / 60, "minutes")

