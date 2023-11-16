library(openxlsx)
library(dplyr)
library(ggplot2)
library(corrplot)

datos = read.xlsx('data_happiness.xlsx')

ggplot(datos, aes(x = gdp, y = happiness_score)) + geom_point()

#Coeficiente de correlacion 
#Un coeficiente de correlacion por encima de 0.6 se considera alto para el modelo
correlacion <- cor(datos, method="pearson")
corrplot.mixed(correlacion, lower="circle", upper="number")

modelo <- lm(happiness_score ~ gdp + healthy_life_expectancy  + freedom, data=datos)
summary(modelo)

shapiro.test(modelo$residuals)
