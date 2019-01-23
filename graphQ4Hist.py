import matplotlib.pyplot as plt
import numpy as np
from textwrap import wrap
from operator import itemgetter
import random


gradRates = open('Project1_Q4/p1q4out/part-r-00000')

s = gradRates.readline()

countryGradRates = []

while s != '':
    temparr = s.split(' ')
    tempDouble = round(float(s.replace('\n','').split('\t')[1]), 2)
    tempstr = temparr[10]
    countryGradRates.append([tempstr[0:3], tempDouble]) # country : grad ratio

    s = gradRates.readline()

sorted_countryGradRates = sorted(countryGradRates, key=itemgetter(1))

countries = [x[0] for x in sorted_countryGradRates]
plottableGradValues = [x[1] for x in sorted_countryGradRates]
fileName = 'Question4_Hist'
plotTitle = 'Change in male employment from the year 2000 across various countries'

plt.figure(figsize=(15, 8.5))

histbins = np.arange(-25,50,2)
plt.hist(plottableGradValues, bins = histbins)
plt.xlabel('Relative change in employment (%)')
plt.ylabel('Count')

plt.title(plotTitle)

plt.savefig(fileName)