import matplotlib.pyplot as plt
import numpy as np
from textwrap import wrap
from operator import itemgetter
import random
import re

# FIRST FILE
filename = 'Question2_plot'
relativeChangeFile = open('Project1_Q2/p1_q2_OUT/part-r-00000')

relativeChange = relativeChangeFile.readline()
parsedWords = []

while relativeChange != '':
    parsedWords = re.split(r"[ \t]", relativeChange)

    if parsedWords[5] == 'enrollment,' and parsedWords[6] == 'tertiary,' and parsedWords[7] == 'female':
        # School enrollment, tertiary, female
        relativeChange = round(float(parsedWords[len(parsedWords) - 1]),2)
        break
    relativeChange = relativeChangeFile.readline()
# 'relativeChange' is the desired variable

# SECOND FILE
enrollmentRatesFile = open('Project1_Q2_p2/p1_q2_p2_OUT/part-r-00000')

enrollmentLine = enrollmentRatesFile.readline()

enrollmentRates = []

while enrollmentLine != '':
    temparr = re.split(r"[ \t]", enrollmentLine)

    tempDouble = round(float(temparr[len(temparr) - 1]), 2)
    year = temparr[len(temparr) - 7]
    enrollmentRates.append([year, tempDouble]) # country : grad ratio

    enrollmentLine = enrollmentRatesFile.readline()

xvalues = [x[0] for x in enrollmentRates]
yvalues = [x[1] for x in enrollmentRates]

index = np.arange(len(xvalues))

plt.figure(figsize=(15, 8.5))
plt.grid(b = True, axis = 'y')
plt.scatter(xvalues, yvalues)

plt.xlabel('Year')
plt.ylabel('Enrollment rates (%)')
plt.title('Tertiary enrollment rates over time, ' + str(relativeChange) + '% average relative increase/yr')

plt.savefig(filename)