"""
    In this file the selection of the features is made and the call to the producer function to send data by streaming too.
"""

import pandas as pd

from services.kafka import kafka_producer
from sklearn.model_selection import train_test_split 


df_2015 = pd.read_csv('./Data/2015.csv')
df_2016 = pd.read_csv('./Data/2016.csv')
df_2017 = pd.read_csv('./Data/2017.csv')
df_2018 = pd.read_csv('./Data/2018.csv')
df_2019 = pd.read_csv('./Data/2019.csv')


def transform_2015(df):
    #Eliminación de columnas 
    columns_to_drop = ['Country', 'Region', 'Happiness Rank', 'Standard Error', 'Dystopia Residual']
    df = df.drop(columns_to_drop, axis=1)
    #Renombramiento de columnas
    df = df.rename(columns={
    'Happiness Score':'happiness_score',
    'Economy (GDP per Capita)':'gdp',
    'Family':'family',
    'Health (Life Expectancy)':'healthy_life_expectancy',
    'Freedom':'freedom',
    'Trust (Government Corruption)':'government_corruption',
    'Generosity':'generosity'
    })
    #Creación columna 'year'
    df['year'] = 2015
    #Acomodamiento del orden de las columnas
    df = df[['year', 'happiness_score', 'gdp', 'family', 'healthy_life_expectancy', 'freedom', 'government_corruption', 'generosity']]
    return df

def transform_2016(df):
    #Eliminación de columnas 
    columns_to_drop = ['Country', 'Region', 'Happiness Rank', 'Lower Confidence Interval', 'Upper Confidence Interval', 'Dystopia Residual']
    df = df.drop(columns_to_drop, axis=1)
    #Renombramiento de columnas
    df = df.rename(columns={
    'Happiness Score':'happiness_score',
    'Economy (GDP per Capita)':'gdp',
    'Family':'family',
    'Health (Life Expectancy)':'healthy_life_expectancy',
    'Freedom':'freedom',
    'Trust (Government Corruption)':'government_corruption',
    'Generosity':'generosity'
    })
    #Creación columna 'year'
    df['year'] = 2016
    #Acomodamiento del orden de las columnas
    df = df[['year', 'happiness_score', 'gdp', 'family', 'healthy_life_expectancy', 'freedom', 'government_corruption', 'generosity']]
    return df

def transform_2017(df):
    #Eliminación de columnas 
    columns_to_drop = ['Country', 'Happiness.Rank', 'Whisker.high', 'Whisker.low', 'Dystopia.Residual']
    df = df.drop(columns_to_drop, axis=1)
    #Renombramiento de columnas
    df = df.rename(columns={
    'Happiness.Score':'happiness_score',
    'Economy..GDP.per.Capita.':'gdp',
    'Family':'family',
    'Health..Life.Expectancy.':'healthy_life_expectancy',
    'Freedom':'freedom',
    'Trust..Government.Corruption.':'government_corruption',
    'Generosity':'generosity'
    })
    #Creación columna 'year'
    df['year'] = 2017
    #Acomodamiento del orden de las columnas
    df = df[['year', 'happiness_score', 'gdp', 'family', 'healthy_life_expectancy', 'freedom', 'government_corruption', 'generosity']]
    return df

def transform_2018_2019(df, year):
    #Eliminación de columnas 
    columns_to_drop = ['Overall rank', 'Country or region']
    df = df.drop(columns_to_drop, axis=1)
    #Renombramiento de columnas
    df = df.rename(columns={
    'Score':'happiness_score',
    'GDP per capita':'gdp',
    'Social support':'family',
    'Healthy life expectancy':'healthy_life_expectancy',
    'Freedom to make life choices':'freedom',
    'Perceptions of corruption':'government_corruption',
    'Generosity':'generosity'
    })
    #Creación columna 'year'
    df['year'] = year
    #Acomodamiento del orden de las columnas
    df = df[['year', 'happiness_score', 'gdp', 'family', 'healthy_life_expectancy', 'freedom', 'government_corruption', 'generosity']]
    return df

df_2015 = transform_2015(df_2015)
df_2016 = transform_2016(df_2016)
df_2017 = transform_2017(df_2017)
df_2018 = transform_2018_2019(df_2018, 2018)
df_2019 = transform_2018_2019(df_2019, 2019)

df_total = pd.concat([df_2015, df_2016, df_2017, df_2018, df_2019])
#print(df_total.shape)
columns_to_drop = ['year', 'government_corruption', 'generosity']
df_total = df_total.drop(columns_to_drop, axis=1)

X=df_total[['gdp', 'family', 'healthy_life_expectancy', 'freedom']]
y=df_total['happiness_score']
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)



if __name__ == "__main__":
    kafka_producer(X_test)