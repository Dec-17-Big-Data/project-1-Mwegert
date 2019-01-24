import matplotlib.pyplot as plt
import numpy as np
from textwrap import wrap
from operator import itemgetter
import random


bottom10 = False; # else top10. Change this var to change plot


gradRates = open('Project1_Q1/p1_q1_OUT/part-r-00000')

s = gradRates.readline()

n = 10 #number of countries to plot
countryGradRates = []

while s != '':
    temparr = s.split(' ')
    tempDouble = round(float(s.replace('\n','').split('\t')[1]), 2)
    tempStr = (' '.join(temparr[3 : len(temparr) - 3]))
    countryGradRates.append([tempStr, tempDouble]) # country : grad ratio

    s = gradRates.readline()

sorted_countryGradRates = sorted(countryGradRates, key=itemgetter(1))

if bottom10:
    countries = ['-\n'.join(wrap(x[0], 9)) for ind,x in enumerate(sorted_countryGradRates) if ind < n]
    plottableGradValues = [x[1] for ind,x in enumerate(sorted_countryGradRates) if ind < n]
    fileName = 'Question1_Bottom10'
    plotTitle = 'Countries with the lowest tertiary graduation rates (where data is available)'
else:
    countries = ['-\n'.join(wrap(x[0], 9)) for ind,x in enumerate(sorted_countryGradRates) if ind < len(sorted_countryGradRates) and ind > len(sorted_countryGradRates) - 11]
    plottableGradValues = [x[1] for ind,x in enumerate(sorted_countryGradRates) if ind < len(sorted_countryGradRates) and ind > len(sorted_countryGradRates) - 11]
    fileName = 'Question1_Top10_below30'
    plotTitle = 'Countries just below 30% tertiary graduation rate (from most recent available data)'

index = np.arange(n)
width = 0.8

plt.figure(figsize=(15, 8.5))

colorArray = [[0.2,0.1,0.5], [114/255, 147/255, 203/255], [225/255, 151/255, 76/255], [132/255, 186/255, 91/255], [4/255,176/255,180/255],
[211/255, 94/255, 96/255], [204/255, 194/255, 16/255], [146/255, 36/255, 40/255], 'black', [62/255, 150/255, 81/255], [207/255,171/255,229/255], [11/255,133/255,0]]
colors = []
for c in countries:
    colors.append(random.choice(colorArray))
plt.bar(index, plottableGradValues, width, color = colors)

plt.xlabel('Countries')
plt.ylabel('Graduation rates (%)')
plt.xticks(index, countries, wrap = 'true')
plt.yticks(np.arange(0,max(plottableGradValues)*1.25, round(max(plottableGradValues)*1.25/10, 1)))
plt.title(plotTitle)

plt.savefig(fileName)