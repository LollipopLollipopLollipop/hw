from pyspark import SparkContext
import sys

def selection(x1,x2):
    intersection = x1[1] & x2[1]
    if len(intersection) >= threshold and x1[0]!=x2[0]:
        return (x1[0],x2[0])

def bfs_construction(network, n):
    ###############################################
    #initialize settings
    n_list = network[n]
    all_nodes = set(n_list).union({n})
    
    #create parent dictionary
    parent_dict = {}
    for i in network[n]:
        parent_dict[i] = [n]
  
    #initialize GN score
    gn_score_dict = {}
    for nn in network.keys():
        if nn!= n:
            gn_score_dict[nn] = 1
        
    #initialize bfs value
    value_dict = {}
    for j in all_nodes:
        value_dict[j] = 1
        
    #record state on each level
    ls = []
    
    #build bfs
    while n_list!=[]:
        #all nodes connected to n
        last_state = n_list
        #list of list of each step
        ls.append(n_list)
        #clear to [] for next step
        n_list = []
        for k in last_state:
            # r is the list of n's list
            for r in network[k]:
                #n's nodes' nodes
                #stop until all nodes are recorded
                if r not in all_nodes:
                    #record 
                    if r not in n_list:
                        n_list.append(r)
                    if r in parent_dict.keys():
                        #add the other child of n's nodes
                        parent_dict[r]+= [k]
                    else:
                        #create the child of non-n's nodes
                        parent_dict[r] = [k]
                    if r in value_dict.keys():
                        value_dict[r] = value_dict[r]+value_dict[k]
                    else:
                        value_dict[r] = value_dict[k]
                        
        all_nodes = set(n_list).union(all_nodes)   
    ls.reverse()
        
    #get the wieghted score
    gn_ls = {}
    for m in ls:
        for mm in m:
            for p in parent_dict[mm]:
                    
                tuple_pair = tuple(sorted((p,mm)))
                weight = value_dict[p]/value_dict[mm]
                update_value = gn_score_dict[mm]*weight
                gn_ls[tuple_pair] = update_value
                if p!=n:
                    gn_score_dict[p]=gn_score_dict[p]+update_value
                
    return gn_ls

            
def get_between(network):
    result = {}
    for n in set(network.keys()):
        gn_ls = bfs_construction(network, n)
        for nn in gn_ls:
            if nn  not in result.keys():
                result[nn]= gn_ls[nn]/2
            else:
                result[nn]  = result[nn]+gn_ls[nn]/2
    return result 

def node_bfs(network,n):
    result = set()
    n_list = [n]
    while n_list!=[]:
        new = n_list[0]
        del n_list[0]
        for c in network[new]:
            if c not in result:
                result.add(c)
                n_list+=[c]
    result = tuple(result)
    return result

def community(u_u_node,network):
    all_c_list_step=[]
    all_c_list=[]
    n_dict ={}
    for n in u_u_node:
        if n not in all_c_list:
            c_list = node_bfs(network,n)
            all_c_list=all_c_list+list(c_list)
            all_c_list_step.append(c_list)
            sub_dic = {}
            for i in c_list:
                sub_dic[i]=network[i]
            n_dict[c_list]=sub_dic
    return all_c_list_step,all_c_list,n_dict  

def modularity_between(v):
    result ={}
    k = v.keys()
    for n in k:
        b = bfs_construction(v, n)    
        for j in b:
            if j not in result.keys():
                result[j] = b[j]/2
            else:
                result[j] = result[j]+b[j]/2
    return result

def find_cut(v):
    result = {}
    pre = modularity_between(v)
    max_cut = max(pre.values())
    for i in pre.keys():
        if pre[i]==max_cut:
            result[i]=max_cut
    return result

def q_calculator(all_c_list_step,dic_u_u,denominator):
    result = 0
    for c in all_c_list_step:
        if len(c)!=1:
            for p in permutations(c,2):
                a = 0
                i = p[0]
                j = p[1]
                ii = dic_u_u[i]
                jj = dic_u_u[j]
                if j in ii:
                    a = 1
                result = a-(len(jj)*len(ii))/denominator + result
        else:
            continue
    return result 

def which_cut(all_c_between,q_last):
    q = q_last
    s = 0
    all_i = all_c_between.items()
    for i in all_i:
        k = i[0]
        v = i[1]
        max_v = max(v.values())
        if s<max_v:
            s=max_v
            state = k
    which = n_dict[state]
    which_c = all_c_between[state]      
    which_2 = copy.deepcopy(which)
    which_c_set = set()
    for i in which_c.keys():
        #remove the edge       
        e1 = i[0]
        e2 = i[1]
        which_2[e1].remove(e2)
        which_2[e2].remove(e1)        
        for j in i:
            which_c_set.add(j)       
    return which,which_c,which_2,which_c_set,q,state

def final_cut(which,which_c,which_2,which_c_set):
    cc ={}
    e = {}
    k1 = which.keys()
    key_2 = which_2.keys()
    for n in which_c_set:
        out = set(node_bfs(which_2,n))
        
        if out != k1:
            key_22 = tuple(out)
            
            if key_22 not in cc.keys():
                d = {}
                for i in out:
                    d[i]=which_2[i]
                if len(d.keys())>1:
                    e[key_22] = find_cut(d)
                else:
                    e[key_22] = {}
                cc[key_22]=d
        
        else:
            k11 = tuple(k1)
            cc[k11]= which_2
            if len(which_2.keys())>1:
                e[k11] = find_cut(which_2)
            else:
                e[k11] = {}

    return cc,e 

def get_final(gap_q,all_c_list_step,gap,state):
    n_dict.pop(state)
    all_c_between.pop(state)
    all_c_list_step.remove(state)
    cc_i = cc.items()
    e_i = e.items()
    for i in add_ls:
        all_c_list_step.append(i)
    for j in cc_i:
        n_dict[j[0]]=j[1]
    for x in e_i:
        all_c_between[x[0]]=x[1]
    return all_c_list_step,all_c_between,n_dict


      


if __name__ == '__main__':

    threshold = int(sys.argv[1])
    input_path = sys.argv[2]
    output1_path = sys.argv[3]
    output2_path = sys.argv[4]

    sc = SparkContext('local[*]','task2')
    sc.setLogLevel("ERROR")

    d = sc.textFile(input_path).map(lambda x: x.split(","))
    head = d.first()
    data = d.filter(lambda x: x != head).map(lambda x:(x[0],x[1]))

    #user:{business set}
    u_b = data.groupByKey().map(lambda x:(x[0],set(x[1])))
    #all pair
    u_u_pair = u_b.cartesian(u_b).map(lambda x: selection(x[0],x[1])).filter(lambda x: x is not None).distinct()
    #node list
    u_u_list = u_u_pair.groupByKey().map(lambda x:(x[0],list(x[1]))).collect()
    #u u dictionary
    dic_u_u = dict(u_u_list)

    between_nodes = get_between(dic_u_u)
    out = {k: v for k, v in sorted(between_nodes.items(), key=lambda x: (-x[1],x[0][0]))}
    #output 1
    out_list = list(out.items())

    #modularity
    gap = 10**(-5)
    u_u_node = set(dic_u_u.keys())

    all_c_list_step,all_c_list,n_dict = community(u_u_node,dic_u_u)

    all_c_between = {}
    for i in n_dict.items():
        k = i[0]
        vv = i[1]
        vv_key = vv.keys()
        if len(vv_key)==1:
            v = {}
        else:
            v = find_cut(vv) 
        all_c_between[k] = v

    denominator = 0
    for i in dic_u_u.values():
        ii = len(i)
        denominator+=ii

    q = q_calculator(all_c_list_step,dic_u_u,denominator)/denominator
    q_last = q_calculator(all_c_list_step,dic_u_u,denominator)/denominator

    while 1==1:
        
        all_c_list_step2 = []
        for i in all_c_list_step:
            all_c_list_step2.append(i)
        
        which,which_c,which_2,which_c_set,q,state = which_cut(all_c_between,q_last)
        
        cc,e = final_cut(which,which_c,which_2,which_c_set)

        all_c_list_step2.remove(state)
        add_ls = list(cc.keys())
        for i in add_ls:
            all_c_list_step2.append(i)
        q_last = q_calculator(all_c_list_step2,dic_u_u,denominator)/denominator
        gap_q = q-q_last
        if abs(gap_q)>=gap:
            all_c_list_step,all_c_between,n_dict=get_final(gap_q,all_c_list_step,gap,state)
        else:
            break   

    out2_2 = []
    for i in all_c_list_step:
        i = sorted(i)
        out2_2.append(i)
    out2_2 = [v for v in sorted(out2_2, key=lambda x: (len(x),x[0]))]

    with open(output1_path, 'w') as f:
        for i in out_list:
            
            f.write("('")
            f.write(i[0][0])
            f.write("',")
            f.write("'")
            f.write(i[0][1])
            f.write("'),")
            f.write(str(round(i[1],5)))
            f.write("\n")

    with open(output2_path, 'w') as f:
        for i in out2_2:
            for j in i[0:-1]:
                f.write("'")
                f.write(j)
                f.write("',")
            f.write("'")
            f.write(i[-1])
            f.write("'")
            f.write("\n")



























































