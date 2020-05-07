import sys
from pyspark import SparkConf, SparkContext
#import scipy
#import numpy
from math import sqrt


appName = "Data Engineering"
master = 'local'

conf = SparkConf().setAppName(appName).setMaster(master)
conf.set("spark.executor.memory", "4g")
conf.set("spark.driver.memory", "4g")
conf.set("spark.driver.maxResultSize", "5g")
sc = SparkContext(conf=conf)

data = [appName, 'on tuesday']

# Set the directory paths for 2018/2020/2020_first_4
directory_path_2020 = "C:/Users/tzvet/OneDrive - TU Eindhoven/Masters Year 1/Q4/2IMD15/Milestones/Milestone_1/2020/"
directory_path_2018 = "C:/Users/tzvet/OneDrive - TU Eindhoven/Masters Year 1/Q4/2IMD15/Milestones/Milestone_1/2018/"
directory_path_2020_4 = "C:/Users/tzvet/OneDrive - TU Eindhoven/Masters Year 1/Q4/2IMD15/Milestones/Milestone_1/2020_first_4/"
STOCK_DIR = directory_path_2020_4
final_path = directory_path_2020
# Get the file names of all the files in the given directory
path = final_path
fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration())
list_status = fs.listStatus(sc._jvm.org.apache.hadoop.fs.Path(path))
file_names = ["file:/" + final_path + file.getPath().getName() for file in list_status]
#file_names.remove(folder_to_save_to)
num_files = len(file_names)
print(file_names)


def get_average(dataset):
    average = 0
    for x in range(len(dataset)):
        average += dataset[x]
    return average / len(dataset)


def pearson_correlation(in1, in2):
    if len(in1) is not len(in2):
        return Null
    in1_avg = get_average(in1)
    in2_avg = get_average(in2)

    topsum = 0
    blssum = 0
    brssum = 0
    for x in range(len(in1)):
        topsum += (in1[x] - in1_avg) * (in2[x] - in2_avg)
        blssum += (in1[x] - in1_avg)**2
        brssum += (in2[x] - in2_avg)**2

    return topsum / (sqrt(blssum) * sqrt(brssum))


def prep_file(lines_string):
    """"Method to preprocess the file based on the stock dataset
    parameters:
        file_name - string containing file name"""
    #lines = read_file(file_name)  # Read file
    #print("PRINTING line sting")
    #print(lines_string)
    #lines = lines # Get the lines for the file to be processed
    lines = lines_string.split('\r\n')
    #print("PRINTING LINES")
    #print(lines)

    #Find out which month of data we're dealing with
    first_date = lines[0].split(",")[0]
    if first_date == "\n":
        first_date = lines[1].split(",")[0]
    cur_month = find_month(first_date)

    # Find first date in dataset
    first_nr = find_first_date_number(first_date, cur_month)

    # Get closing prices at end of each day
    closing_prices = []
    counter = 0
    for date in cur_month:
        cp_holder = 0
        line = lines[counter].split(",")
        while line[0] == date:
            cp_holder = line[5]
            counter += 1
            line = lines[counter].split(",")
        closing_prices.append([date, cp_holder])

    prepped = post_prep(closing_prices)

    return prepped

def prep(line):
    return (line.split(",")[5], 1)

def post_prep(closing_prices):
    empty_dates = []
    for cp in range(len(closing_prices)):
        if closing_prices[cp][1] == 0:
            empty_dates.append(cp + 1)

    first = 0
    for x in range(len(closing_prices)):
        if (x+1) not in empty_dates:
            first = x
            break

    for x in range(first):
        closing_prices[x][1] = closing_prices[first][1]

    last_price = 0
    new_closing_prices = []
    # CHANGED THIS FOR-LOOP
    for x in range(len(closing_prices)):
        if closing_prices[x][1] == 0:
            new_closing_prices.append(float(last_price))
        else:
            last_price = closing_prices[x][1]
            new_closing_prices.append(float(last_price))
    return new_closing_prices

def find_month(date):
    jan_dates = create_dates(1,31) #Create dates for january
    feb_dates = create_dates(2, 29) #Create dates for february
    mar_dates = create_dates(3, 31) #Create dates for march
    apr_dates = create_dates(4, 30) #Create dates for april
    may_dates = create_dates(5, 31) #Create dates for may

    if date in jan_dates:
        cur_month = jan_dates
    elif date in feb_dates:
        cur_month = feb_dates
    elif date in mar_dates:
        cur_month = mar_dates
    elif date in apr_dates:
        cur_month = apr_dates
    elif date in may_dates:
        cur_month = may_dates
    else:
        return AssertionError
    return cur_month

def create_dates(month, days):
    if month < 10:
        x = "0%d" % month
    else:
        x = str(month)
    dates = ["%s/0%d/2020" % (x, i) for i in range(1, 10)]

    for x in ["%s/%d/2020" % (x, i) for i in range(10, days+1)]:
        dates.append(x)
    return dates

def find_first_date_number(first_date, cur_month):
    counter = 0
    for date in cur_month:
        if first_date is date:
            return counter
        else:
            counter += 1
    return 31


def read_file(file_name):
    file = open("%s/%s.txt" % (STOCK_DIR, file_name))
    lines = file.readlines()
    file.close()
    print(lines)
    return lines


def load_stock_file(filename):
    file = sc.textFile("%s/filename" % (STOC_DIR))
    file_par = filename.parallelize(file.collect())
    return file_par



#import os
#
#dir_list_txt = os.listdir(STOCK_DIR)
#dir_list = []
#for item in dir_list_txt:
#    dir_list.append(item.replace(".txt", ""))
#for name in dir_list:
#    prep_file(name)
#

# Load all the files from the selected directory and create on big RDD

rdd_whole = sc.wholeTextFiles(final_path)

# rdd.foreach(lambda pair: print(pair))
filtered = rdd_whole.mapValues(lambda value: prep_file(value)) # keeps the rdd as a tuple

#######filtered = rdd_whole.map(lambda pair: prep_file(pair[1])) # removes the first part and containg only the values


# Initilialize 2D array containing the indices to be set
table_indices = []
n = num_files
for i in range(n):
    table_indices.append(list(range(i*n, (i+1)*n)))


dict_table_indices = {}
def get_indices(ind, matrix):
    listed = []
    if ind != 0:
        for i in range(ind):
            listed.append(matrix[i][ind])

    listed = listed + matrix[ind][ind+1:]# add +1 to remove diagonal
    return listed

for file in file_names:
    index_file = file_names.index(file)
    indices = get_indices(index_file, table_indices)
    dict_table_indices[file] = indices

list_dict_table_indices = []
for k in dict_table_indices:
    list_dict_table_indices.append((k,dict_table_indices[k]))


#print("Filtered spark stuff")
#filtered.foreach(lambda rdd: print(rdd))

rdd_dict = sc.parallelize(list_dict_table_indices)
#rdd_dict.foreach(lambda x: print(x))

joined_ind_with_stock_rdd = rdd_dict.join(filtered)
#print("X")
#joined_ind_with_stock_rdd.foreach(lambda x: print(x))

def transform_tuple(tup):
    key = tup[0]
    indices_tup = tup[1][0]
    stocks = tup[1][1]
    tuple_to_ret = ((key, stocks), indices_tup)
    return tuple_to_ret

inv_map = joined_ind_with_stock_rdd.map(lambda pair: transform_tuple(pair))
#inv_map.foreach(lambda x: print(x))

flt_map = inv_map.flatMapValues(lambda x: x)
#flt_map.foreach(lambda x: print(x))

def swap_key_values(tup):
    key = tup[0]
    value = tup[1]
    return (value, key)

swaped_flt_map = flt_map.map(lambda pair: swap_key_values(pair))
#swaped_flt_map.foreach(lambda x: print(x))

reduced = swaped_flt_map.reduceByKey(lambda tup1, tup2: pearson_correlation(tup1[1], tup2[1]))
print("Resultoooooooooooooooooooo")
reduced.foreach(lambda x: print(x))



