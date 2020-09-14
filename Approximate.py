import numpy as np
import pandas as pd
import random
import math
random.seed(0)
exact_count = []
approx_count = []
approx_error = []
for i in range(0,5):
    # random number generation
    n = np.random.randint(1000000,10000000)
    exact_count.append(n)
    
    print("Random Number generated" + str(n))
    x = 0
    y = 0
    z = 0
    for i in range(0,n):
        if(i%1000000) == 0:
            print("progress =" + str((i/n)*100)+ "%")
            # calculation of probabilities with 3 counter
        prob_x = 1/math.pow(2,x)
        prob_y = 1/math.pow(2,y)
        prob_z = 1/math.pow(2,z)
           # increamenting the counter on the basis of probability
        val_x = np.random.choice(2,1, p = [1-prob_x,prob_x])
        val_y = np.random.choice(2,1, p = [1-prob_y,prob_y])
        val_z = np.random.choice(2,1, p = [1-prob_z,prob_z])

        if val_x == 1:
            x = x + 1
        if val_y == 1:
            y = y + 1
        if val_z == 1:
            z = z + 1  
        
        # taking the average of all the 3 counters
        average = (x + y + z)/3
            
           

    counter = round(math.pow(2,average)-1,3)
    approx_count.append(counter)

    # calculation of errors
    error = round((abs(n - counter)/n)*100,3)
    approx_error.append(error)
    print("counter = "+ str(counter) + "error = " + str(error)+"%")
data_frame = pd.DataFrame({
    'Exact count of events':exact_count,
    'Approximate count':approx_count,
    'Approximate error':approx_error
    
})
data_frame
    
