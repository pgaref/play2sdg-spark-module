"""
    Plot Experiment Data Module
    First Attempt to integrate Seaborn library
    pg1712@imperial.ac.uk
    Panagiotis Garefalakis
"""
import sys, getopt
import numpy as np
np.random.seed(9221999)
import pandas as pd
from scipy import stats, optimize
import matplotlib.pyplot as plt
import seaborn as sns


def sine_wave(n_x, obs_err_sd=1.5, tp_err_sd=.3):
    x = np.linspace(0, (n_x - 1) / 2, n_x)
    y = np.sin(x) + np.random.normal(0, obs_err_sd) + np.random.normal(0, tp_err_sd, n_x)
    return y

def plot_sin_example():
    sns.set(palette="Set2")
    sines = np.array([sine_wave(31) for _ in range(20)])
    sns.tsplot(sines);
    sns.plt.savefig("mesos_load_timeseries.pdf", bbox_inches="tight")
    sns.plt.show()

def random_walk(n, start=0, p_inc=.2):
    return start + np.cumsum(np.random.uniform(size=n) < p_inc)

def mesos_load_timeseries(data):

    # removes extra dim pou dn xerw apo pou emfanistike
    data = data.squeeze()
    step = np.arange(1, data.shape[0] + 1)
    labels = np.array(["node1", "node2", "node3"])

    # print('walks', walks.shape)
    # print(('data', data.shape))
    # print(('step', step.shape))
    # print(('labels', labels.shape))

    # nodes = pd.Series(labels, name="Mesos Nodes")
    # sns.tsplot(data[:,0], time=step)
    # sns.tsplot(data[:,1], time=step)
    # sns.tsplot(data[:,2], time=step)
    # sns.plt.show()

    # Matplotlib keeping the seaborn style

    # Calc Mean and Std
    mean = data.mean(axis=1)
    std = np.std(data, axis=1)

    # Plot mean and 1-std
    plt.plot(step, mean, linewidth=2, label='Avg.')
    plt.fill_between(step, mean - std, mean + std, alpha=0.2)

    for i in range(data.shape[1]):
        plt.plot(step, data[:,i], linewidth=1, linestyle='--', alpha=0.5, label=labels[i])

    plt.legend(loc='lower right', frameon=True, framealpha=0.7)
    plt.xlabel('time in (s)')
    plt.ylabel('load %')

    plt.savefig("mesos_load_timeseries.pdf", bbox_inches="tight")
    plt.show()


def parse_file(filename):
    dataset = []
    ar1 = []
    ar2 = []
    ar3 = []
    with open(filename) as f:
        for line in f:
            # print(('Got line ', line))
            linesplit = line.split(" ")
            starts = line.rfind("[")
            ends = line.rfind("]")
            digits = line[starts+1: ends].replace(",", "").strip()
            digits = digits.split(" ")
            ar1.append(float(digits[0]))
            ar2.append(float(digits[1]))
            ar3.append(float(digits[2]))
            #print 'ar: ', array2d
            #print 'ds: ', dataset
        dataset.append(ar1)
        dataset.append(ar2)
        dataset.append(ar3)
    data = np.dstack( [[s]for s in dataset] )
    # for d in dataset:
    #     print((len(d)))
    # print("my data")
    # print(data)
    return data


def main(argv):
    #defailt values
    inputfile = 'mesos_stats18-6-2015#08:05:56.log'
    outputfile = ''

    try:
        opts, args = getopt.getopt(argv,"hi:o:",["ifile=","ofile="])
    except getopt.GetoptError:
        print 'seaborn_timeseries_plot.py -i <inputfile> -o <outputfile>'
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print 'seaborn_timeseries_plot.py -i <inputfile> -o <outputfile>'
            sys.exit()
        elif opt in ("-i", "--ifile"):
            inputfile = arg
        elif opt in ("-o", "--ofile"):
            outputfile = arg
    print 'Input file is "', inputfile
    print 'Output file is "', outputfile

    dataset = parse_file(inputfile)
    #plot_sin_example()
    mesos_load_timeseries(dataset)



if __name__ == "__main__":
    main(sys.argv[1:])