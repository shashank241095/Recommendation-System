import os
import sys
from pyspark import *
import itertools
import collections
import csv
sc = SparkContext("local[*]", "recommendation System")
import operator
from itertools import combinations
import operator
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
import time
# Load and parse the data
#data = sc.textFile("some.csv")

# inputfile2=sys.argv[1]
inputfile2="/Users/shashank24/PycharmProjects/s/pathtofile"

# inputfile2="/Users/shashank24/PycharmProjects/s/yelp_train.csv"
# testfile2="test121.csv"
testfile2="/Users/shashank24/PycharmProjects/s/path_to_cross_validation_data"

# testfile2="/Users/shashank24/PycharmProjects/s/yelp_val.csv"

# output2=sys.argv[4]
output2="Any_output"
a=1


rdd_main=sc.textFile(inputfile2)

fi = rdd_main.first()

data = rdd_main.filter(lambda a: a != fi)

datax=data.map(lambda y:y.split(",")[0])
datay=data.map(lambda y:y.split(",")[1])

user_d=dict()
bus_d=dict()

user_inv=dict()
bus_inv=dict()

counter1=0
for i in datax.distinct().collect():
    user_d[i]=counter1
    user_inv[counter1]=i
    counter1=counter1+1

counter2 = counter1+1
for i in datay.distinct().collect():
    bus_d[i] = counter2
    bus_inv[counter2]=i
    counter2 = counter2 + 1

dataf=data.map(lambda i:i.split(",")).map(lambda u:(user_d[u[0]],bus_d[u[1]],float(str(u[2]))))


rank = 10
numIterations = 20


rddtest=sc.textFile(testfile2)



fitest = rddtest.first()

testdata = rddtest.filter(lambda a: a != fitest)


testdata5=testdata.map(lambda a:a.split(",")).map(lambda a:(a[0],a[1],a[2]))

dataytest=testdata.map(lambda y:y.split(",")[1])
testor=testdata.map(lambda i:i.split(",")).map(lambda u:((u[0],u[1]),float(str(u[2]))))


c=0
counter2=counter2+1
for i in dataytest.distinct().collect():
    if(i not in bus_d):
        c=c+1
        bus_d[i] = counter2
        bus_inv[counter2]=i
        counter2 = counter2 + 1



testd1=testdata.map(lambda i:i.split(",")).map(lambda u:(user_d[u[0]],bus_d[u[1]],float(str(u[2]))))


testd=testdata.map(lambda i:i.split(",")).map(lambda u:(user_d[u[0]],bus_d[u[1]]))
testfull=testd.map(lambda t:((t[0],t[1]),None))

####
datafull=data.map(lambda i:i.split(",")).map(lambda u:((user_d[u[0]],bus_d[u[1]]),float(str(u[2]))))
dataf1=datafull.subtractByKey(testfull)

lambda_=0.26

model = ALS.train(dataf, rank, numIterations,lambda_)


###


testdata1 = testd.map(lambda p: (p[0], p[1]))



def roundoff(x):
    if(x[1]>5):
        r=5.0
    elif(x[1]<1):
        r=1.0
    else:
        r=x[1]
    return (x[0],r)

predictions = model.predictAll(testdata1).map(lambda r: ((r[0], r[1]), r[2])).map(lambda x:roundoff(x)).collectasMap




rdd_main = sc.textFile(inputfile2)

rdd_main1=rdd_main.map(lambda x:x.split(","))


fi = rdd_main.first()

data = rdd_main.filter(lambda a: a != fi)





rdd1 = data.map(lambda line: (line.split(",")[1], line.split(",")[0]))# bussiness as key

fi2 = rdd_main1.first()

data2 = rdd_main1.filter(lambda a: a != fi2)

ratingsRdd = data2.map(lambda x : (((str(x[0])), str(x[1])), float(str(x[2]))))

############################
testRdd = sc.textFile(testfile2).map(lambda a:a.split(","))

header2 = testRdd.first()

testRdd1 = testRdd.filter(lambda x : x != header2)
test6=testRdd1.map(lambda a:(a[0],a[1],float(str(a[2]))))

# avg=test6.map(lambda a:a[2]).mean()
avg=ratingsRdd.map(lambda a:a[1]).mean()

testRdd3 = testRdd1.map(lambda x : ((x[0], x[1]), None))
#########################
trainingRdd = ratingsRdd.subtractByKey(testRdd3)

trainingRdd1 = trainingRdd.map(lambda x : (x[0][0], (x[0][1], x[1])))

trainingRdd2 = trainingRdd.map(lambda x : (x[0][1], (x[0][0], x[1])))


cc=trainingRdd2.count()
print("count",cc)

####################
uitemrat=trainingRdd1.groupByKey().mapValues(dict).collectAsMap()

iuserrat=trainingRdd2.groupByKey().mapValues(dict).collectAsMap()
#################
uitemratb=sc.broadcast(uitemrat)

iuserratb=sc.broadcast(iuserrat)



countnew=0
testloop=testRdd3.map(lambda a:a[0])

print(testloop.take(5))
c2=0
import math
countnew2=0
c3=0
ce=0
finaln=0
finald=0

####
#####
#######
def func(t_data):

    user_item_rating=uitemratb.value
    item_user_rating=iuserratb.value

    activeuser = t_data[0]
    activeitem = t_data[1]

    similarity=[]
    countaa=0
    sum8a=0
    if(activeitem in item_user_rating):### cold start problem
        verticalactive=item_user_rating.get(activeitem)

        for k,v in verticalactive.items():

            sum8a = sum8a + verticalactive[k]
            countaa=countaa+1


        if(activeuser in user_item_rating):
            verticalactivelist=verticalactive.keys()

            toloop=user_item_rating.get(activeuser)

            for items in toloop:
                activelist=[]
                otherlist=[]
                getotherusers=item_user_rating.get(items)# to get vertical list of other users
                counta=0
                counto=0
                sumo=0
                suma=0
                sim=0
                sum8o=0
                for user,rating in getotherusers.items():
                    if(user in verticalactive.keys()):
                        # otherlist.append(rating)
                        # activelist.append(verticalactive[user])
                        activelist.append(user)
                        counta=counta+1
                        sumo=sumo+rating
                        suma=suma+verticalactive[user]
                    sum8o=sum8o+rating
                    counto=counto+1


                if(counta==0):
                    avga=sum8a/countaa
                    avgo=sum8o/counto
                else:
                    avga=suma/counta
                    avgo=sumo/counta
                num=0
                od=0
                an=0
                for user in activelist:

                    num=num+(getotherusers.get(user)-avgo)*(verticalactive.get(user)-avga)

                    od=(((getotherusers.get(user))-avgo)* ((getotherusers.get(user))-avgo))+od
                    an=(((verticalactive.get(user))-avga)*((verticalactive.get(user))-avga))+an
                deno=math.sqrt(od)
                dena=math.sqrt(an)
                denominator=deno*dena

                if(num==0 or denominator==0):
                    similarity.append((items,avg)) ## giving similarity 0 to num or den 0

                else:
                    sim=num/denominator
                    similarity.append((items,sim))


            # fil=sc.parallelize(similarity)

            # filterresult=fil.takeOrdered(2,lambda a:-a[1])

            filterresult=sorted(similarity,key=lambda x: x[1],reverse=True)
            n=len(similarity)


            pn=0
            pd=0
            j=0
            for i in filterresult:
                pn=pn+(i[1]*toloop.get(i[0]))
                pd=pd+abs(i[1])
                if(j>n/2):
                    break
                j=j+1

            if(pd!=0):
                predict=pn/pd
                return predict
            else:
                v=avg
                for k,v in item_user_rating.items():
                    if(activeitem==k):
                        l22=len(v.values())
                        sum22=sum(v.values())
                v=sum22/l22


                return v

        else:
            return avg





    else:
        l2=0
        s2=0
        avgz=avg
        if(activeuser  in user_item_rating):
            print(1)
            for k,v in user_item_rating.items():
                if(k==activeuser):
                    s2=sum(v.values())
                    l2=len(v.values())

            avgz=s2/l2

        #cold start no item present
        predict=avgz
        return predict





def roundoff(x):
    if(x[2]>5.0):
        prediction=5.0
    elif(x[2]<0):
        prediction=0.0
    else:
       prediction=x[2]
    return (x[0],x[1],prediction)









candidate_pairs = set()

mapdict = {}

ans = testloop.map(lambda x:(x[0],x[1],func(x))).map(roundoff)

# func(["wf1GqnKQuvH-V3QN80UOOQ","fThrN4tfupIGetkrz18JOg"])


# To calciulate RMSE
f10=open(output2,"w")

f10.write("user_id"+"business_id"+"prediction")
f10.write("\n")
for i in ans.collect():
    f10.write(str(i[0])+','+str(i[1])+','+str(i[2]))
    f10.write("\n")

f10.close()

fcalculate = sc.textFile(output2)

fi9 = fcalculate.first()
fcalculate = fcalculate.filter(lambda a: a != fi9)

fSplitValues = fcalculate.map(lambda x: x.split(',')).map(lambda x: ((x[0],x[1]),float(str(x[2]))))
testDataSplitValues = testRdd1.map(lambda x: ((x[0],x[1]),float(str(x[2]))))
joinTestData = testDataSplitValues.join(fSplitValues).map(lambda x: ((x[0],x[1]),abs(x[1][0]-x[1][1])))

rdd11=joinTestData.map(lambda x:x[1]**2).reduce(lambda x,y:x+y)

rmse=math.sqrt(rdd11/fSplitValues.count())
print("RMSE: ",rmse)

diff01 = joinTestData.filter(lambda x: 0 <= x[1] < 1)
diff12 = joinTestData.filter(lambda x: 1 <= x[1] < 2)
diff23 = joinTestData.filter(lambda x: 2 <= x[1] < 3)
diff34 = joinTestData.filter(lambda x: 3 <= x[1] < 4)
diff4 = joinTestData.filter(lambda x: 4 <= x[1])

# print(">=0 and <1: " + str(diff01.count()))
# print(">=1 and <2: " + str(diff12.count()))
# print(">=2 and <3: " + str(diff23.count()))
# print(">=3 and <4: " + str(diff34.count()))
# print(">=4: " + str(diff4.count()))