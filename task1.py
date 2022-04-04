from pyspark import SparkContext
import sys
from graphframes import *
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType

def selection(x1,x2):
    intersection = x1[1] & x2[1]
    if len(intersection) >= threshold and x1[0]!=x2[0]:
        return (x1[0],x2[0])


if __name__ == '__main__':

    threshold = int(sys.argv[1])
    input_path = sys.argv[2]
    output_path = sys.argv[3]

    sc = SparkContext('local[*]','task1')
    sc.setLogLevel("ERROR")
    d = sc.textFile(input_path).map(lambda x: x.split(","))
    head = d.first()
    data = d.filter(lambda x: x != head).map(lambda x:(x[0],x[1]))

    #user:{business set}
    u_b = data.groupByKey().map(lambda x:(x[0],set(x[1])))   

    #all pair
    u_b_pair = u_b.cartesian(u_b).map(lambda x: selection(x[0],x[1])).filter(lambda x: x is not None).distinct()

    #all edge
    edge = u_b_pair.filter(lambda x: x[0] < x[1])

    #all node
    node = u_b_pair.groupByKey().map(lambda x:x[0])

    spark = SparkSession.builder.getOrCreate()
    df_edge = spark.createDataFrame(edge, ['src', 'dst'])
    df_node = spark.createDataFrame(node, StringType()).toDF("id")

    #create graph
    graph = GraphFrame(df_node, df_edge)
    final = graph.labelPropagation(maxIter=5)
    out = final.rdd.map(lambda c: (c[1], c[0])).groupByKey().map(lambda x:list(x[1]))
    result = out.map(lambda x: sorted(x)).collect()
    result = sorted(result, key=lambda x: (len(x), x))

    with open(output_path, 'w') as f:
        for i in result:
            for j in i[0:-1]:
                f.write("'")
                f.write(j)
                f.write("',")
            f.write("'")
            f.write(i[-1])
            f.write("'")
            f.write("\n")





