#coding=UTF-8

"""
@update: use the swap instaed of insert
"""
import random
import math # math.exp
import time
import numpy as np
from itertools import permutations, combinations

disagree_pairs = {}

def get_tau_dist(cur_rank,ranks):
	n_voters, n_candidates = ranks.shape
	tau = 0
	global disagree_pairs
	for rank in ranks:
		for i, j in combinations(range(n_candidates), 2):
			if rank[i] < 0 or rank[j] < 0:
				continue
			if (cur_rank[i] - cur_rank[j])*(rank[i] - rank[j]) < 0:
				tau += 1
				disagree_pairs.setdefault((i,j), 0)
				disagree_pairs[(i,j)] += 1
	return tau

def distance_affected(cur_rank, ranks, index_a, index_b): # if the two index are close with, we don't need to compare all pairwise
	n_voters, n_candidates = ranks.shape
	tau = 0
	index_min = min(index_a, index_b)
	index_max = max(index_a, index_b)
	for rank in ranks:
		for i, j in combinations(range(n_candidates), 2):
			if i not in range(index_min, index_max+1) and j not in range(index_min, index_max+1):
				continue
			if rank[i] < 0 or rank[j] < 0:
				continue
			tau += (cur_rank[i] - cur_rank[j])*(rank[i] - rank[j]) < 0
	return tau

def annealing(ranks, cur_rank, temperature_begin=1.0e+100, temperature_end=.1, cooling_factor=.9, nb_iterations=1):
	n_voters, n_candidates = ranks.shape
	best_rank = cur_rank[:]
	best_tau = get_tau_dist(best_rank, ranks)

	try:
		for iteration in range(nb_iterations):
			temperature = temperature_begin # every iter begins from the same temperature
			cur_rank = best_rank[:] # from 2nd time, restart from the best rank instead of random rank in the 1st time
			cur_tau = best_tau
			new_rank = best_rank[:]
			new_tau = best_tau

			while temperature > temperature_end:
				index = random.choice(disagree_pairs.keys())

				new_rank = cur_rank.copy()
				new_rank[index[0]], new_rank[index[1]] = new_rank[index[1]], new_rank[index[0]]
				new_tau = get_tau_dist(new_rank,ranks)

				diff = new_tau - cur_tau # cur_tau is the best tau we get now 
				if diff < 0 or math.exp(-diff/temperature) > random.random():
					cur_rank = new_rank # found move: new->cur
					cur_tau = new_tau
					del disagree_pairs[index]
				else:
					new_rank = cur_rank # no found move, so reset 
					new_tau = cur_tau

				if cur_tau < best_tau:
					best_rank = cur_rank
					best_tau = cur_tau
				temperature = temperature * cooling_factor
			print iteration, ":", best_tau
	except KeyboardInterrupt, e:
		print "Interrupted on user demand."
		print 'performed iterations: %d' % iteration 
	
	return best_rank, best_tau

def main():
	# ranks = np.array([
	# 				[5,8,7,2,6,9,10,1,4,3], # youku
	# 				[7,4,6,1,9,8,-1,3,5,2], # iqiyi
	# 				[4,6,8,2,7,-1,-1,1,5,3] # tudou
	# 				# [4,7,8,3,5,9,6,-1,2,1]
	# 				])  0   1   2   3   4    5   6  7   8   9   10  11  12  13  14  15  16  17  18  19  20  21  22  23
	ranks = np.array([[-1 , 8, -1 , 6 , 3 , 4 ,-1, -1, -1, -1 , 5 ,-1 ,-1 , 9 ,-1 ,-1 ,-1 , 1, -1 ,-1 ,-1 , 2 ,-1,  7],
					  [-1, 10 ,11 ,-1 , 4 , 6 ,-1 , 5 , 8 , 9 ,-1 ,-1 ,-1 ,-1 ,-1 ,-1 , 3 , 2 ,12, -1 , 7 , 1 ,-1 ,-1],
					  [-1 ,-1, -1, -1, -1, -1, -1, -1, -1 ,-1 ,-1 ,-1 ,-1 ,-1 ,-1 ,-1 ,-1 ,-1 ,-1 ,-1 ,-1, -1, -1 , 1],
					  [-1 ,-1 , 3 ,-1 ,-1 ,-1, -1, -1 , 2 ,-1 ,-1 ,-1 ,-1, -1, -1 ,-1, -1 ,-1 ,-1, -1 ,-1 ,-1, -1 , 1],
					  [10, 12, 15,  6,  3 , 5 ,18 ,-1 , 8 ,11 , 4 , 9 ,-1 ,14, 17, -1 ,-1 , 1 ,16 ,-1,  7 , 2 ,-1 ,13],
					  [14, 15 ,17, 10 , 6 , 9 ,20 , 7 ,11 ,13 , 8 ,12 ,-1 ,16 ,18 ,-1,  4 , 1 ,19 , 3 ,-1 , 2 , 5, -1],
					  [18, 17, 16, 12 , 7 , 9 ,-1 , 8 ,14 ,15 , 6 ,13 ,-1 ,19 ,20 , 1 , 5 , 2 ,21 , 4 ,10,  3 ,-1 ,11],
					  [-1 ,15 ,17 , 8 , 5 , 7 ,-1,  6 ,14 ,11 ,-1 ,10 ,12 ,13, 18 ,-1 , 3 , 1 ,16 , 2 , 9 , 4 ,-1 ,-1],
					  [-1 ,12 ,-1 , 3 ,-1 , 7 ,15 , 1 ,13,  9 , 4 , 8 ,11 ,14 ,-1 ,-1 , 5 ,-1 ,16 ,-1 , 6 ,10, -1 , 2],
					  [-1 ,-1 ,12 , 2 , 1 , 4  ,9 , 3 , 7 ,-1 , 5 , 8 ,-1 ,-1 ,10 ,-1 ,-1 ,-1 ,11 ,-1 , 6 ,-1 ,-1 ,-1]])
	init_rank = np.array([18, 17, 16, 12,  7 , 9 ,22 , 8 ,14 ,15  ,6 ,13 ,23 ,19, 20 , 1 , 5 , 2 ,21 , 4 ,10 , 3 ,24 ,11])
					 #   [16,18, 23 ,10 , 6 , 9, 22 , 8 ,14, 15 , 7 ,13 ,17 ,19 ,21 , 1 , 5 , 2 ,24 , 4 ,12,  3 ,20 ,11]
	start_time = time.time()
	new_rank, new_tau = annealing(ranks = ranks, cur_rank = init_rank, temperature_begin = 200, temperature_end = 0.1, nb_iterations = 200)
	end_time = time.time()
	print new_rank, new_tau, end_time - start_time

# if __name__ == '__main__':
# 	main()
