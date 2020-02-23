import pandas as pd

#Your data
df = pd.DataFrame( {'Item':['Food','Clothing','Housing','Transportation'], 'Cost':[6000,3000,10000,5000]})

#Use xlsxwriter
writer = pd.ExcelWriter('Livecost.xlsx',engine='xlsxwriter')  

#Write to an excel file
df.to_excel(writer,sheet_name='Sheet1',index=False)

#Load workbook
workbook  = writer.book
worksheet = writer.sheets['Sheet1']
row=0
for item, cost in df.iterrows():
    row += 1
#Write data
worksheet.write('A6', 'Total:')
worksheet.write(row+1, 1, '=SUM(B2:B5)')

#Create a chart
chart = workbook.add_chart({'type': 'column'})

#Configure the data to series of the chart
chart.add_series({'values': '=Sheet1!$B$2:$B$5','name': 'My Cost','categories': '=Sheet1!$A$2:$A$5'})

#Insert the chart 
worksheet.insert_chart('E2', chart)

#Close file
writer.save()





