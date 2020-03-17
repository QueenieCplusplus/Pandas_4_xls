# -*- coding: utf-8 -*-
'''
TodoPandasFunc on 2020.3.16.
Duration: 1-2 work days
@author: QueenPy
COL: 600-
'''
from sqlalchemy import func # to do select
from sqlalchemy import exc 
from sqlalchemy import cast # tO do type cast
import string
#from SQLalchemyInstance import instance
#from DB.table_name import Model_Class
import pandas as pd
import numpy as np
import datetime 
import requests as req


def df_generator():
    res_df = pd.DataFrame() # to insert the param
    print(res_df) 

def number_df_initialier():
    dt = {'0.25M': [0.4, 0.3], '0.5M': [0.5, 0.4], '1Y':[0.38, 0.39] }
    df = pd.DataFrame(data=dt)
    return(df)

def df_maker():
    dt = {'idx':['rateA', 'rateA-'], '0.25M': [0.4, 0.3], '0.5M': [0.5, 0.4], '1Y':[0.38, 0.39]}
    df = pd.DataFrame(data=dt)
    print(df)
    return df

#       idx   0.25M   0.5M    1Y
# 0   rateA     0.4    0.5   0.38
# 1   rateA-    0.3    0.4   0.39

def np_producer():
    npdt = [ ['rateA', 0.4, 0.5, 0.38], ['rateA-', 0.3, 0.4, 0.39]]
    df = pd.DataFrame(np.array(npdt), columns=['idexd', '0.25M', '0.5M', '1Y'])
    print(df)
    return df

#     idexd   0.25M   0.5M    1Y
# 0   rateA     0.4    0.5  0.38
# 1   rateA-    0.3    0.4  0.39

def show_range_index():
    dfdf= df_maker()
    print(dfdf.index)
    #RangeIndex(start=0, stop=2, step=1)

def show_column_label():
    temp_df =np_producer()
    print(temp_df.columns)
    #Index(['idexd', '0.25M', '0.5M', '1Y'], dtype='object')

def show_axes_as_axis():
    temp_df =np_producer()
    print(temp_df.axes)
    #[RangeIndex(start=0, stop=2, step=1), Index(['idexd', '0.25M', '0.5M', '1Y'], dtype='object')]
    # RangeIndex as row axis lable & ColumnsIndex as Column Axis Labels, which returns themselves in order.

def show_size():
    temp_df =np_producer()
    dfdf= df_maker()
    print(temp_df.size, dfdf.size)
    #8 #8

def show_shape():
    dfdf= df_maker()
    print(dfdf.shape)
    # (2, 4) means 2 rows *n 4 columns

def check_miss_value_in_bool():
    # alias isna = isnull
    dfdf= df_maker()
    print(dfdf.isna())

    #        idx    0.25M   0.5M     1Y
    # 0    False   False    False  False
    # 1    False   False    False  False

def axis_iterator():
    dfdf= df_maker()
    print(dfdf.__iter__)
#    <bound method NDFrame.__iter__ of       idx  0.25M  0.5M    1Y
#                                       0   rateA    0.4   0.5  0.38
#                                       1  rateA-    0.3   0.4  0.39>

# take key to select particular data
#@ return series or dataframe
#@ param: key = label, axis, level = position, drop_level if true then return label itself
def xs_indicator():
    dfdf= df_maker()
    idx_data = dfdf.xs('idx', axis=1)
    print('xs process completed, return result:', idx_data)

    #  0     rateA
    #  1     rateA-
    #  Name: idx, dtype: object

def rows_interator():
    temp_df = rating_yield_curve_df_gen()

    #     0.25M  0.5M   1Y
    # A     0.1   0.2  0.5
    # A-    0.3   0.4  0.6

    row_0 = next(temp_df.iterrows())[0:3] # A row from col_0.25 to col_0.5
    print(row_0)
    # ('A',
    # 0.25M    0.1
    # 0.5M     0.2
    # 1Y       0.5)

    # Name: A, dtype: float64

def items_iterator():
    temp_df = rating_yield_curve_df_gen()

    #     0.25M  0.5M   1Y
    # A     0.1   0.2  0.5
    # A-    0.3   0.4  0.6

    item_0 = next(temp_df.iteritems())
    print(item_0)

    # ('0.25M', 
    # A    0.1
    # A-   0.3)

    # Name: 0.25M, dtype: float64

def tuples_iterator():
    temp_df = rating_yield_curve_df_gen()

    #     0.25M  0.5M   1Y
    # A     0.1   0.2  0.5
    # A-    0.3   0.4  0.6

    tuples_0 = next(temp_df.itertuples())
    print(tuples_0)

    #Pandas(Index='A', _1=0.1, _2=0.2, _3=0.5)

def look_up():

    temp_df = rating_yield_curve_df_gen()

    #     0.25M  0.5M   1Y
    # A     0.1   0.2  0.5
    # A-    0.3   0.4  0.6

    #@ return ndarray[founded_value]
    #@param: row_labels, col_labels
    ratingAminus_ytm_in1Y= temp_df.lookup('A-', '1Y')
    print(ratingAminus_ytm_in1Y)
    # TypeError: Index(...) must be called with a collection of some kind, 'A-' was passed

#@params: key= col, default_return_val_if_not_found
def df_getter():
    temp_df = rating_yield_curve_df_gen()

    #     0.25M  0.5M   1Y
    # A     0.1   0.2  0.5
    # A-    0.3   0.4  0.6

    get_val = temp_df.get('0.5M', 'null')
    print(get_val)

    #A     0.2
    #A-    0.4
    #Name: 0.5M, dtype: float64

#@ return dataframe
#@ param: expression, inplace
def columns_queririer(query_string):
    num_df = number_df_initialier()
    query_statement = query_string
    query_result = num_df.query(query_statement)
    print(query_result)


# eqals to the func df % other
#@ param: other, axis='columns'
def arithmetics_subtractor():
    num_df = number_df_initialier()
    sub_df = num_df.sub(0.01)
    print(sub_df)

    #     0.25M  0.5M   1Y
    # 0   0.39  0.49  0.37
    # 1   0.29  0.39  0.38

def arithmetics_dividor():
    num_df = number_df_initialier()
    num_df = num_df.div(10)
    print(num_df)
    #TypeError: unsupported operand type(s) for /: 'str' and 'int'
    #    0.25M  0.5M     1Y
    #0   0.04  0.05  0.038
    #1   0.03  0.04  0.039

    #    0.25M  0.5M     1Y
    # 0   0.04  0.05  0.038
    # 1   0.03  0.04  0.039

def comparer_to():
    num_df = number_df_initialier()
    print(num_df.eq(0.4))

    # 0.25M   0.5M     1Y
    # 0   True  False  False
    # 1  False   True  False

#@ return: scalar, ndarray, pd object
#@ params: expr_string, inplace_bool faulse in default
def eval_executor():
    random_df = pd.DataFrame({'0.25M': range(1, 6), '0.5M': range(10, 0, -2), '1Y': [0.3]*5})
    #    0.25M  0.5M   1Y
    # 0      1    10  0.3
    # 1      2     8  0.3
    # 2      3     6  0.3
    # 3      4     4  0.3
    # 4      5     2  0.3

    eval_df = random_df.eval('`0.25M` + `1Y`') # backtick quote ``
    print(eval_df)
    # 0    1.3
    # 1    2.3
    # 2    3.3
    # 3    4.3
    # 4    5.3

# items
#@ iterate over(col_name_as_label, seriesRow)
#@ return iterable[[tuple[union[hashable]], series]]
def clovid19_item_func():

    clovid19_globe_df = pd.DataFrame({'china':[0, 76288, 81079], 'jp':[0, 851, 1538], 'kr':[0, 833, 3736], 'tw':[1, 4, 59]}, index=['01', '02', '03'])
    print(clovid19_globe_df)

    #     china    jp    kr  tw
    # 01      0     0     0   1
    # 02  76288   851   833   4
    # 03  81079  1538  3736  59

    for label, content in clovid19_globe_df.items():
        print('label', label)
        print('confirmed case for clovid 19 for jan, feb, march are:', content, sep='\n')

    # label china
    # confirmed case for clovid 19 for jan, feb, march are:
    # 01        0
    # 02    76288
    # 03    81079
    # Name: china, dtype: int64
    # label jp
    # confirmed case for clovid 19 for jan, feb, march are:
    # 01       0
    # 02     851
    # 03    1538
    # Name: jp, dtype: int64
    # label kr
    # confirmed case for clovid 19 for jan, feb, march are:
    # 01       0
    # 02     833
    # 03    3736
    # Name: kr, dtype: int64
    # label tw
    # confirmed case for clovid 19 for jan, feb, march are:
    # 01     1
    # 02     4
    # 03    59

def set_index_for_clovid19_tw():

    clovid19_globe_df = pd.DataFrame({'china':[0, 76288, 81079], 'jp':[0, 851, 1538], 'kr':[0, 833, 3736], 'tw':[1, 4, 59]}, index=['01', '02', '03'])

    #     china    jp    kr  tw
    # 01      0     0     0   1
    # 02  76288   851   833   4
    # 03  81079  1538  3736  59

    tw_clovid19_df = clovid19_globe_df.set_index('tw')
    print(tw_clovid19_df)

    #     china    jp    kr
    # tw
    # 1       0     0     0
    # 4   76288   851   833
    # 59  81079  1538  3736

    clovid19_df = clovid19_globe_df.set_index(pd.Index([1, 2, 3])) # as same as dataframe.reset_index()
    print(clovid19_df)
    #    china    jp    kr  tw
    # 1      0     0     0   1
    # 2  76288   851   833   4
    # 3  81079  1538  3736  59

def two_df_merger():
    df_clovid19_tw = pd.DataFrame({'month':[1, 2, 3], 'cfrm case':[1, 4, 59]})
    df_clovid19_jp = pd.DataFrame({'month':[1, 2, 3], 'cfrm case':[0, 851, 1538]})

    merged_df = df_clovid19_tw.merge(df_clovid19_jp, left_on='month', right_on='month')
    print(merged_df)

    #     month  cfrm case_x  cfrm case_y (default valu_col_name)
    # 0      1            1            0
    # 1      2            4          851
    # 2      3           59         1538

def truncator():
    clovid19_globe_df = pd.DataFrame({'china':[0, 76288, 81079], 'jp':[0, 851, 1538], 'kr':[0, 833, 3736], 'tw':[1, 4, 59]}, index=['01', '02', '03'])

    north_east_asia_clovid19 = clovid19_globe_df.truncate(before='jp', after='kr', axis=1) # axis = columns
    print(north_east_asia_clovid19)

    #       jp    kr
    # 01     0     0
    # 02   851   833
    # 03  1538  3736

def replacer():
    life_series = pd.Series(['chees', 'egg', 'oliver', 'mint', 'cat', 'water', 'sunshine', 'hackerbook', 'mac', 'internet']) # one dimention array with (index) axis label

    # 0         chees
    # 1           egg
    # 2        oliver
    # 3          mint
    # 4           cat
    # 5         water
    # 6      sunshine
    # 7    hackerbook
    # 8           mac
    # 9      internet

    life_series = life_series.replace('cat', 'iphone')
    print(life_series)

    # 0         chees
    # 1           egg
    # 2        oliver
    # 3          mint
    # 4        iphone (replace result)
    # 5         water
    # 6      sunshine
    # 7    hackerbook
    # 8           mac
    # 9      internet

def rating_yield_curve_df_gen():
    df = pd.DataFrame([[0.1, 0.2, 0.5], [0.3, 0.4, 0.6]], index= ['A', 'A-'], columns=['0.25M', '0.5M', '1Y'])
    print(df)
    return df

    #     0.25M  0.5M   1Y
    # A     0.1   0.2  0.5
    # A-    0.3   0.4  0.6

def show_stack():
    result_df = rating_yield_curve_df_gen() 
    print(result_df.stack())

    # A   0.25M    0.1
    #     0.5M     0.2
    #     1Y       0.5

    # A-  0.25M    0.3
    #     0.5M     0.4
    #     1Y       0.6

def tuples2dfcol_convertor():
    month = pd.MultiIndex.from_tuples([('month', 0.38), ('month',0.25), ('month', 0.5), ('month', 12)])
    df_from_tuples = pd.DataFrame([[0.3, 0.4, 0.5, 0.6], [0.7, 0.8, 0.9, 1.0]] ,index=['a', 'a-'], columns=month)
    print(df_from_tuples)

    #             month
    #     0.38  0.25  0.50  12.00
    # a    0.3   0.4   0.5   0.6
    # a-   0.7   0.8   0.9   1.0

def where_executor():
    s = pd.Series(range(13))
    nov_dec_month = s.where(s>10)

    # if cond(means condition) is true, then the element is used.

    # 0      NaN
    # 1      NaN
    # 2      NaN
    # 3      NaN
    # 4      NaN
    # 5      NaN
    # 6      NaN
    # 7      NaN
    # 8      NaN
    # 9      NaN
    # 10     NaN

    # 11    11.0
    # 12    12.0
    # dtype: float64

    ytm_all_months = s.where(s>10, 0.3)
    print(ytm_all_months)

    # 0      0.3 (defaut val)
    # 1      0.3
    # 2      0.3
    # 3      0.3
    # 4      0.3
    # 5      0.3
    # 6      0.3
    # 7      0.3
    # 8      0.3
    # 9      0.3
    # 10     0.3
    # 11    11.0 (val executed by where_func)
    # 12    12.0 (val executed by where_func)
    # dtype: float64

def mask_executor():
    hrs_timer = pd.Series(range(13))
    #print(hrs_timer.where(hrs_timer > 0))
    # 0      NaN
    # 1      1.0
    # 2      2.0
    # 3      3.0
    # 4      4.0
    # 5      5.0
    # 6      6.0
    # 7      7.0
    # 8      8.0
    # 9      9.0
    # 10    10.0
    # 11    11.0
    # 12    12.0
    # dtype: float64

    #print(hrs_timer.where(hrs_timer > 6, 'being_masked'))
    #     1.3.13
    # 0     being_masked
    # 1     being_masked
    # 2     being_masked
    # 3     being_masked
    # 4     being_masked
    # 5     being_masked
    # 6     being_masked
    # 7                7
    # 8                8
    # 9                9
    # 10              10
    # 11              11
    # 12              12
    # dtype: object

    print(hrs_timer.mask(hrs_timer > 6, 'being_masked'))
    
    #     1.3.13
    # 0                0
    # 1                1
    # 2                2
    # 3                3
    # 4                4
    # 5                5
    # 6                6
    # 7     being_masked
    # 8     being_masked
    # 9     being_masked
    # 10    being_masked
    # 11    being_masked
    # 12    being_masked
    # dtype: object

def covariance_calculator():
    ytm_df = pd.DataFrame([('A', 0.25, 0.3), ('A-', 0.25, 0.4), ('A', 0.5, 0.4), ('A-', 0.5, 0.3)], columns=['rate', 'mat', 'ytm'])
    
    #     rate  mat  ytm
    # 0    A   0.25  0.3
    # 1    A-  0.25  0.4
    # 2    A   0.50  0.4
    # 3    A-  0.50  0.3

    print(ytm_df.cov())
    #         mat       ytm
    # mat  0.020833  0.000000
    # ytm  0.000000  0.003333

def dfapply_lambda():
    cal_df = pd.DataFrame([[3]*3, [4]*3, [5]*3], columns=['W', 'L', 'H'])
    #print(cal_df)

    # W  L  H
    # 0  3  3  3
    # 1  4  4  4
    # 2  5  5  5

    #print(cal_df.apply(np.sqrt))
    #     W         L         H
    # 0  1.732051  1.732051  1.732051
    # 1  2.000000  2.000000  2.000000
    # 2  2.236068  2.236068  2.236068

    #print(cal_df.apply(np.sum, axis=0)) # 0 means index
    # 1.3.13
    # W    12
    # L    12
    # H    12

                       #@ param1 can be a lambda
    #print(cal_df.apply(np.sum, axis=1)) # 1 means columns
    # 1.3.13
    # 0     9
    # 1    12
    # 2    15
    # dtype: int64

    empty_df = pd.DataFrame([], columns=['col_1', 'col_2', 'col_3'])
    print(empty_df)
    # Empty DataFrame
    # Columns: [col_1, col_2, col_3]
    # Index: []

    print(empty_df.apply(lambda p: pd.Series([7, 8, 9], index=['col_1', 'col_2', 'col_3']), axis=1))
    # Empty DataFrame
    # Columns: [col_1, col_2, col_3]
    # Index: []

    print(empty_df.apply(lambda p:[6, 4], axis=1, result_type='broadcast'))
    # result_type, must be one of {None, 'reduce', 'broadcast', 'expand'}

def ewm_func_called():
    df = pd.DataFrame({'scorpio': [5, np.nan, 4, 6, 3]}, index=['sun', 'moon', 'water', 'venus', 'mars'])

    #            scorpio
    # sun        5.0
    # moon       NaN
    # water      4.0
    # venus      6.0
    # mars       3.0

    after_com_df = df.ewm(com=0.5).mean()
    print(after_com_df)

    #         scorpio
    # sun    5.000000
    # moon   5.000000
    # water  4.100000
    # venus  5.486486
    # mars   3.779661
   
if __name__ == '__main__':
        
    #df_generator()
    #df_maker()
    #np_producer()
    #show_range_index()
    #show_column_label()
    #show_axes_as_axis()
    #show_size()
    #show_shape()
    #check_miss_value_in_bool()
    #axis_iterator()
    #xs_indicator()

    #columns_queririer('`0.5M` == `0.25M`') # backtick quote
    #arithmetics_subtractor()
    #arithmetics_dividor()
    #comparer_to()
    #eval_executor()
    #clovid19_item_func()
    #set_index_for_clovid19_tw()
    #two_df_merger()
    #truncator()
    #replacer()
    #rating_yield_curve_df_gen()
    #show_stack()
    #tuples2dfcol_convertor()
    #rows_interator()
    #items_iterator()
    #tuples_iterator()
    #look_up()
    #df_getter()
    #where_executor()
    #mask_executor()
    #covariance_calculator()
    #dfapply_lambda()
    ewm_func_called()

# dataframe is 2-dimensional size-mutab; 
# potential heterogeneous tabular data structure with labeled axes
# data is aligned in a rows & columns.
