# Databricks notebook source
## uso base interna CO2
library(ggplot2)
ggplot(data = CO2) +
geom_point(mapping = aes(x = conc, y = uptake))

# COMMAND ----------

#Mapa de Bolhas
# Libraries
install.packages("gridExtra")
library(ggplot2)
library(dplyr)
# carga dos dados
install.packages("gapminder")
library(gapminder)
data <- gapminder %>% filter(year=="2007") %>% dplyr::select(-year)
# Exibição do gráfico
data %>%
arrange(desc(pop)) %>%
mutate(country = factor(country, country)) %>%
ggplot(aes(x=gdpPercap, y=lifeExp, size=pop, color=continent)) +
geom_point(alpha=0.7) +
scale_size(range = c(.5, 24), name="População (M)") +
labs(x = "Valor per Capta", y = "Expectativa Vida")

# COMMAND ----------

library(ggplot2)
# base interna mtcars - Violino
dados <- ggplot(mtcars, aes(factor(cyl), mpg))
dados + geom_violin()