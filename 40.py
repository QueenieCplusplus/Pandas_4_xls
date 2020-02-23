
# coding: utf-8

# In[1]:


import pandas as pd


# In[28]:


df=pd.read_excel("C:\Users\Administrator\Desktop\QR-Proximity.xlsx")


# In[8]:


df.head()


# In[11]:


Prdata=df['x1'].fillna(0)


# In[12]:


Prdata.head()


# In[23]:


trvals=Prdata.str.contains("Proximity").fillna(False)


# In[25]:


Final=Prdata[trvals]


# In[26]:


Final.to_excel("C:\Users\Administrator\Desktop\Proximity.xlsx")


# In[34]:


df.head()
arval=df['x2']


# In[55]:


Area=df['x2'].where(df['x2'] <= 1).dropna()


# In[59]:


Area.to_excel("C:\Users\Administrator\Desktop\Newvals.xlsx")

