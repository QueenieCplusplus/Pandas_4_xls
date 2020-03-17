# -*- coding: utf-8 -*-
'''
TodoSQL on 2020.3.12, 16-17
Duration: 2.5 work days
Using values with lambdaintead of iloc
@author: QueenPy
COL: 180-
'''
#from dateutil.relativedelta import relativedelta
from sqlalchemy import func # to do select
from sqlalchemy import exc 
import string

from sqlalchemy.sql import select
from sqlalchemy.sql import literal_column

#from SQLalchemyInstance import instance
#from DB.table_name import Model_Class
import pandas as pd
import numpy as np
import datetime 
import requests as req

# columns of table_autre.rating_yield_curve:
# date, currency, rating, maturity, ytm

# url = 'mysql+ ://queenie:queenie@IP:Port/queenie'

series2dictOfIndex = lambda s: {idx.name: str(getattr(s, idx.name, 'no value exists')) for idx in s.DataFrame.index} 
series2dictOfColumn = lambda s: {col.name: str(getattr(s, col.name, 'no value exists')) for col in s.DataFrame.columns} 


# row2dict = lambda row: {col.name: str(getattr(row, col.name)) for col in row.__table__.columns} if row else {}

# AttributeError: 'str' object has no attribute '__table__'

#sheet_name = 'FormosaCurve'
#res = extractor(query_table_name=sheet_name) #this is pd df

def alternative_convertor():

    res_df = pd.DataFrame() # to insert the param
    print(res_df) 
    # [3 rows x 14 columns] in Virtue

#          0          1          2          3         4       ...
# 0    (Tenor)       (1M)       (3M)       (6M)     (1Y)      ...
# 1       A         0.028      0.029      0.031     0.033     ...
# 2       A-        0.029      0.030      0.032     0.035     ...

# [3 rows x 5~14 columns] in POC    

    # bond_df = pd.DataFrame(data, index like array, col like array)
    # data includes ndarray, iterable, dict, df

    bond_df = pd.DataFrame(res_df, index=['maturity', 'ytm_A', 'ytm_Aminus'])
    #print(bond_df)

    #              0    1    2    3    4    ...
    # maturity    NaN  NaN  NaN  NaN  NaN   ...
    # ytm_A       NaN  NaN  NaN  NaN  NaN   ...
    # ytm_Aminus  NaN  NaN  NaN  NaN  NaN   ...

    formosa_df = pd.DataFrame(bond_df, columns=['Tenor', '1M', '3M', '6M', '1Y', '2Y', '3Y', '4Y', '5Y', '6Y', '7Y', '8Y', '9Y', '10Y'])
    #print(formosa_df)

    #              Tenor  1M  3M  6M  1Y  2Y  3Y  4Y  5Y  6Y  7Y  8Y  9Y  10Y
    # maturity      NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN  NaN
    # ytm_A         NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN  NaN
    # ytm_Aminus    NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN NaN  NaN

 
    #formosa_dict = series2dictOfIndex(formosa_df)
    #print(formosa_dict.empty())
    # f"The truth value of a {type(self).__name__} is ambiguous. "
    # ValueError: The truth value of a DataFrame is ambiguous. Use a.empty, a.bool(), a.item(), a.any() or a.all().

    #print(res_df.values) # from df -> dict array
    # [
    #     ['到期年限\n(Tenor)' '1個月\n(1M)' '3個月\n(3M)' '6個月\n(6M)' '1年\n(1Y)' '2年\n(2Y)' '3年\n(3Y)' '4年\n(4Y)' '5年\n(5Y)' '6年\n(6Y)' '7年\n(7Y)' '8年\n(8Y)' '9年\n(9Y)' '10年\n(10Y)']
    #     ['A' 0.028041999999999997 0.029218 0.030507 0.032783 0.034441 0.035841 0.036617000000000004 0.037393 0.03803 0.038666 0.039224 0.039782000000000005 0.040339]
    #     ['A-' 0.029752 0.030785 0.03202 0.034843 0.036591 0.037301 0.037936 0.03857 0.039313 0.040056 0.040538 0.04102 0.041502]
    # ]

    mat_array = res_df.values[0][2:14]
    #print('maturity is:', mat_array) # from df -> dict array # 0-13
    # <class 'numpy.ndarray'>
    # maturity is: ['3個月\n(3M)' '6個月\n(6M)' '1年\n(1Y)' '2年\n(2Y)' '3年\n(3Y)' '4年\n(4Y)' '5年\n(5Y)' '6年\n(6Y)' '7年\n(7Y)' '8年\n(8Y)' '9年\n(9Y)' '10']
    mat_list = mat_array.tolist()
    #print('listify:', mat_list)

    rateAYtm = res_df.values[1][2:14]
    #print('A rating ytm:' , rateAYtm)
    # A rating ytm: [0.029218 0.030507 0.032783 0.034441 0.035841 0.036617000000000004 0.037393 0.03803 0.038666 0.039224 0.039782000000000005 0.040339]
    rateAYtm_list = rateAYtm.tolist() 
    #print('listify:', rateAYtm_list)

    rateAminusYtm = res_df.values[2][2:14]
    #print('A minus rating ytm:' , rateAminusYtm)
    # A minus rating ytm: [0.030785 0.03202 0.034843 0.036591 0.037301 0.037936 0.03857 0.039313 0.040056 0.040538 0.04102 0.041502]
    rateAminusYtm_list = rateAminusYtm.tolist() 
    #print('listify:', rateAminusYtm_list)

    rate_0 = ['A'] * 12
    # print(rate_0)
    #['A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A']

    rate_1 = ['A-'] * 12
    # print(rate_1)
    #['A-', 'A-', 'A-', 'A-', 'A-', 'A-', 'A-', 'A-', 'A-', 'A-', 'A-', 'A-']

    # hint to do month2year string.split('\n')[1].strip('()')
    month2year = lambda tmpList: [ ele.split('\n')[1].strip('()').strip('Y') for ele in tmpList ]
    mat_temp_list= month2year(mat_list)
    print('after lambda:', mat_temp_list)
    # ['3M', '6M', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10']


    forlooplambda = lambda numList: [ int(num.split('M')[0])/12 for num in mat_temp_list[0:2] ] 
    mat_temp_list2 = forlooplambda(mat_temp_list)
    print('yearify process:', mat_temp_list2)
    #yearify process: [0.25, 0.5]
    
    #mat_yearify_list = mat_temp_list replace with mat_temp_list2 for mat_temp_list[0:2] 

    replaceX = lambda bondList: [ element.replace('3M', str(mat_temp_list2[0])) for element in bondList ]
    mat_yearify_list0 = replaceX(mat_temp_list)
    print(mat_yearify_list0)

    replaceY = lambda bondList: [ element.replace('6M', str(mat_temp_list2[1])) for element in bondList ]
    mat_yearify_list = replaceY(mat_yearify_list0)
    print(mat_yearify_list)

    # two_lambda_meger = lambda tmpList: [ele.split('\n')[1].strip('()').strip('Y') and int(ele.split('M')[0])/12 for ele in tmpList]
    # mat_yearify_list = two_lambda_meger(mat_list)
    # print('after merger lambda:',mat_yearify_list )

    data_A = [rate_0, mat_list, rateAYtm_list]
    formosa_curve_bond_df_A = pd.DataFrame({'rating': rate_0, 'maturity': mat_yearify_list, 'ytm': rateAYtm})
    print(formosa_curve_bond_df_A)

    data_A_minus = [rate_1, mat_list, rateAminusYtm_list]
    formosa_curve_bond_df_A_minus = pd.DataFrame({'rating': rate_1, 'maturity': mat_yearify_list, 'ytm': rateAminusYtm})
    print(formosa_curve_bond_df_A_minus)

    return formosa_curve_bond_df_A, formosa_curve_bond_df_A_minus

# def demo_getAttr():

#     class A(object):
#         bar = 0
#         foo = 'hi'

#     instanceA = A()
#     # getattr(obj, name, default)
#     print(getattr(instanceA, 'bar'))
#     print(getattr(instanceA, 'foo'))
#     print(getattr(instanceA, 'goo', 1))

#     #0
#     #hi
#     #1    

if __name__ == '__main__':

    newdf, newdf2 = alternative_convertor()

    #demo_getAttr()

    try:
        records = newdf.to_dict(orient='records')
        print(records)
        records2 = newdf2.to_dict(orient='records')
        print(records2)

        sqlalchemyInstance.session.bulk_insert_mappings(Model_class, records)
        sqlalchemyInstance.session.bulk_insert_mappings(Model_class, records2)
        sqlalchemyInstance.session.commit()

    except exc.IntegrityError as e:
        print('catch the exception', str(e))
        sqlalchemyInstance.session.rollback()