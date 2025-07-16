import numpy as np
from scipy import sparse
import csv
import pickle
import networkx as nx
import matplotlib.pyplot as plt
from cdlib.algorithms import louvain
import json

if __name__ == '__main__':
    # import user-subreddit post behavior
    accounts = {}
    with open('user_postings.csv') as csvfile:
        reader = csv.reader(csvfile)
        idx = 0
        for row in reader:
            if len(row) == 2:
                continue
            accounts[idx] = list(set(row[1:]))
            idx += 1

    # get unique subreddits
    subreddits = list(set([v for values in accounts.values() for v in values]))

    # map subreddits to index mappings
    subidx = {}
    idx_to_sub = {}
    for i in range(len(subreddits)):
        subidx[subreddits[i]] = i
        idx_to_sub[i] = subreddits[i]

    # print total number of subreddits
    print('Total subreddits: {}'.format(len(subreddits)))

    # prepare sparse cols and rows
    row = []
    col = []
    for user, sublist in accounts.items():
        for sub in sublist:
            row.append(subidx[sub])
            col.append(user)

    # build subreddit-user relation matrix
    submat = sparse.csr_matrix((np.ones(len(row)), (row, col)))
    print(submat.shape)
    # create final subreddit-subreddit relations
    srs = submat * submat.T
    # strip small degree
    pmat = srs.toarray()
    pmat[pmat < 10] = 0

    # build percentage matrix
    diag = 1 / srs.diagonal()
    pmat = np.multiply(pmat, diag.reshape((-1, 1)))

    # threshold percentages
    pmat[pmat < 0.05] = 0

    # remove edges that are only one-sided
    pmat = np.multiply(pmat, pmat.T)
    pmat = pmat > 0

    # save pmat as sparse matrix on disk
    X = sparse.csr_matrix(pmat)
    sparse.save_npz("pmat.npz", X)
    del X

    print("starting with network stuff")
    G = nx.from_numpy_matrix(pmat, create_using=nx.Graph())
    G = nx.relabel_nodes(G, idx_to_sub)

    # remove isolates and self edges
    G.remove_edges_from(list(G.selfloop_edges()))
    G.remove_nodes_from(list(nx.isolates(G)))

    # save all remaining subreddits (will later on be used in the evaluation)
    with open("interest_map_subreddits.csv", "w") as file:
        file.write(",".join(list(G.nodes())))

    # identify disconnected components
    subgraphs = list(nx.connected_component_subgraphs(G))
    print(len(subgraphs))
    # persist subgraphs based on size
    small = []
    medium = []
    large = []

    for g in subgraphs:
        # print(list(g.nodes()))
        if len(g.nodes()) <= 3:
            small.append(list(g.nodes()))
        elif len(g.nodes()) <= 50:
            medium.append(list(g.nodes()))
        else:
            com = louvain(g)
            j = json.loads(com.to_json())
            large.append(j["communities"])
            res = com.to_node_community_map()
            node_colors = [res[n][0] for n in g.nodes()]
            plt.figure(figsize=(20,14))
            nx.draw_networkx(g, node_color=node_colors)
            plt.savefig("interest_map_large_community_" + list(g.nodes())[0] + ".pdf")
    print("saving_communities")

    with open("small_communities.csv", "w") as file:
        for c in small:
            file.write(",".join(c) + "\n")

    with open("medium_communities.csv", "w") as file:
        for c in medium:
            file.write(",".join(c) + "\n")

    with open("large_communities.csv", "w") as file:
        for c in large:
            for subc in c:
                file.write(",".join(subc) + "\n")

