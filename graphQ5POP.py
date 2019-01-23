import matplotlib.pyplot as plt
import numpy as np
from textwrap import wrap
from operator import itemgetter
import random
import re

unempFileName = 'Question5_POP_total_unemp_plot' # for plot
advUnempFileName = 'Question5_POP_adv_unemp_POP_plot'


# unemp with advanced ed file (for US)
advUnempFile = open('Project1_Q5/p1q5out/part-r-00000')

advUnemp = advUnempFile.readline()

usaAdvUnemp = []

while advUnemp != '':
    temparr = re.split(r"[ \t]", advUnemp) # split by tabs and spaces

    tempDouble = round(float(temparr[len(temparr) - 1]), 2) # value

    if temparr[0] == "USA":
        year = temparr[7]
        year = year[0:4]
        usaAdvUnemp.append([year, tempDouble]) # country : unemp ratio
    
    advUnemp = advUnempFile.readline()


# total unemp file (for US)
UnempFile = open('Project1_Q5_part2/p1q5p2out/part-r-00000')

Unemp = UnempFile.readline()

usaUnemp = []

while Unemp != '':
    temparr = re.split(r"[ \t]", Unemp) # split by tabs and spaces

    tempDouble = round(float(temparr[len(temparr) - 1]), 2) # value

    if temparr[0] == "USA":
        year = temparr[8]
        year = year[0:4]
        usaUnemp.append([year, tempDouble]) # country : unemp ratio

    
    Unemp = UnempFile.readline()

# unemp with advanced ed file (Global)
GlobalAdvFile = open('Project1_Q5_POP/q5popout/part-r-00000')
globalAdvLine = GlobalAdvFile.readline()

globalAdvUnemp = []

while globalAdvLine != '':
    temparr = globalAdvLine.split('\t')
    tempDouble = round(float(temparr[len(temparr) - 1]), 2)
    year = temparr[0]
    globalAdvUnemp.append([year, tempDouble])

    globalAdvLine = GlobalAdvFile.readline()

GlobalFile = open('Project1_Q5_POP2/p1q5p2out/part-r-00000')
globalUnempLine = GlobalFile.readline()

globalUnemp = []

while globalUnempLine != '':
    temparr = globalUnempLine.split('\t')
    tempDouble = round(float(temparr[len(temparr) - 1]), 2)
    year = temparr[0]
    globalUnemp.append([year, tempDouble])

    globalUnempLine = GlobalFile.readline()



xvalues = [x[0] for x in globalUnemp]
yvalues = [x[1] for x in globalUnemp]

plt.figure(figsize=(15, 8.5))
plt.grid(b = True, axis = 'y')
plt.scatter(xvalues, yvalues)

plt.xlabel('Year')
plt.ylabel('Unemployment rates (%)')
plt.title('Unemployment rates over time. Global vs US. (by population)')


xvalues = [x[0] for x in usaUnemp]
yvalues = [x[1] for x in usaUnemp]
plt.scatter(xvalues, yvalues, marker = '*', c = 'red')
plt.legend(['Global avg by population','USA'])

plt.savefig(unempFileName)
# total unemp file saved



xvalues = [x[0] for x in globalAdvUnemp]
yvalues = [x[1] for x in globalAdvUnemp]

plt.figure(figsize=(15, 8.5))
plt.grid(b = True, axis = 'y')
plt.scatter(xvalues, yvalues)

plt.xlabel('Year')
plt.ylabel('Unemployment rates (%)')
plt.title('Unemployment rates for people with advanced education. Global vs US. (by population)')


xvalues = [x[0] for x in usaAdvUnemp]
yvalues = [x[1] for x in usaAdvUnemp]
plt.scatter(xvalues, yvalues, marker = '*', c = 'red')
plt.legend(['Global avg by Population','USA'])

plt.savefig(advUnempFileName)