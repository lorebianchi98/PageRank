import sys
import re
from operator import add
from pyspark import SparkContext

def parse(line):
    node = line[line.find("<title>") + 7: line.find("</title>")]
    outlinks = re.findall("\\[\\[(.*?)\\]\\]", line)
    return node, outlinks

def distribute(title,outlinks,mass):
    yield title,0  # emitted to not lose node with no inlinks
    num_tuple = len(outlinks)
    for out in outlinks:
        yield out,mass/num_tuple

def rank(summatory):
    return alpha/total_pages + (1-alpha) * summatory


if __name__ == "__main__":

    if len(sys.argv) != 5:
        sys.exit(-1)

    src = sys.argv[1]
    dst = sys.argv[2]
    iteration = int(sys.argv[3])
    alpha = float(sys.argv[4])

    sc = SparkContext("yarn", "PageRank")

    wiki  = sc.textFile(src)

    node = wiki.map(parse).cache() # get tuple with (title,[adjacency list])

    total_pages = node.count()

    init = node.map(lambda x: (x[0],x[1],1/total_pages)) # set initial value of mass

    for iteration in range(iteration):

        distribute_mass = init.flatMap(lambda x: distribute(x[0],x[1],x[2])) # for each contribution make tuple with (title,contribution)
        sum = distribute_mass.reduceByKey(add) # sum contribution per key (title,sum)
        mass = sum.mapValues(rank) # apply the rank formula on sum values (title,rank)
        recap = mass.join(node) # retrieve adj list from node
        init = recap.map(lambda x: (x[0],x[1][1],x[1][0])) # make tuple in format (title,[adj list],mass)


    sort = mass.sortBy(lambda x: -x[1]) # sort using rank

    sort.saveAsTextFile(dst)
	