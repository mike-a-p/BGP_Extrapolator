import sys
import os
import psutil
import psycopg2
import named_tup
import time
from progress_bar import progress_bar
from collections import deque
from AS import AS
from SQL_querier import SQL_querier

class AS_Graph:

    def __init__(self):
        self.ases = dict()
        self.ases_with_anns = list()
        self.ases_by_rank = dict()
        self.strongly_connected_components = list()

    def __repr__(self):
        return str(self.ases)

    def  add_relationship(self, asn,neighbor,relation):
        """Adds an AS relationship to the provided graph (dictionary)

        """
        
        #append if this key has an entry already, otherwise make new entry
        if(asn not in self.ases):
            self.ases[asn] = AS(asn)
        self.ases[asn].add_neighbor(neighbor,relation)
        return

    def read_relationships_from_db(self,num_entries = None):
        sys.stdout.write("Initializing Relationship Graph\n")

        #Select as_relationships table
        querier = SQL_querier()
       	if(num_entries is not None):
            querier.select_relationships(num_entries)
        else:
            numLines = querier.count_entries('relationships')
            querier.select_relationships()

        print("\tFilling Graph...")

        #Progress bar setup 
        start_time = time.time()
        if(num_entries is not None):
            i = 0
            progress = progress_bar(num_entries)
        else:
            progress = progress_bar(numLines)
        
        for record in querier.cursor:
            named_r = named_tup.Relationship(*record)
            #If it's not a cone
            if named_r.cone_as is None:
                #If it's peer-peer (no customer)
                if(named_r.customer_as is None):
                    self.add_relationship(named_r.peer_as_1,named_r.peer_as_2,1)
                    self.add_relationship(named_r.peer_as_2,named_r.peer_as_1,1)
                #if it's provider-consumer
                if(named_r.provider_as is not None):
                    self.add_relationship(named_r.customer_as[0],named_r.provider_as,0)
                    self.add_relationship(named_r.provider_as,named_r.customer_as[0],2)
            progress.update()

            if(num_entries is not None):
                i = i + 1
                if(i>num_entries):
                    break

        neighbor_time = time.time()
        sys.stdout.write('\n')
        #cursor.close()

        return

    def read_seperate_relationships_from_db(self):
        print("Initializing Relationship Graph")
        querier = SQL_querier()

        numLines = querier.count_entries('customer_providers')
        querier.select_customer_providers()
        for record in querier.cursor:
            self.add_relationship(record[1],record[2],0)
            self.add_relationship(record[2],record[1],2)

        numLines = querier.count_entries('peers')
        querier.select_peers()
        for record in querier.cursor:
            self.add_relationship(record[1],record[2],1)
            self.add_relationship(record[2],record[1],1)

    def assign_ranks(self):
        self.find_strong_conn_components()
        self.combine_components()
        self.decide_ranks()
        return

    def combine_components(self):
        """Takes the SCCs of this graph and exchanges them for "super nodes".
            These super nodes have the providers, peers and customers than all
            nodes in the SCC would have. These providers, peers, and customers
            point to the new super node.


        """

        print("\tCombining Components")
        #TODO allow for announcements with known paths to be given to large components
        large_components = list()
        for component in self.strongly_connected_components:
            if(len(component)>1):
                large_components.append(component)
        progress = progress_bar(len(large_components))

        for component in large_components:
            #Create an AS using an "inner" AS to avoid collision
            #TODO maybe change it to some known unique value, ideally integer
            #grab ASN of first AS in component
            new_asn = self.ases[component[0]].asn
            combined_AS = AS(new_asn)
            combined_cust_anns = list()
            combined_peer_prov_anns = list()

            #get providers, peers, customers from "inner" ASes
            #only if they aren't also in "inner" ASes
            for asn in component:

                if(self.ases[asn].anns_from_customers):
                    combined_cust_anns.extend(self.ases[asn].anns_from_customers)
                if(self.ases[asn].anns_from_peers_providers):
                    combined_peer_prov_anns.extend(self.ases[asn].anns_from_peers_providers)

                for provider in self.ases[asn].providers:
                    if(provider not in component):
                        combined_AS.add_neighbor(provider, 0)
                        #replace old customer reference from provider
                        #TODO maybe make faster removal function since list is sorted
                        prov_AS = self.ases[provider]
                        prov_AS.customers.remove(asn)
                        prov_AS.append_no_dup(prov_AS.customers,new_asn)
                for peer in self.ases[asn].peers:
                    if(peer not in component):
                        combined_AS.add_neighbor(peer, 1)
                        peer_AS = self.ases[peer]
                        peer_AS.peers.remove(asn)
                        peer_AS.append_no_dup(peer_AS.peers,new_asn)
                for customer in self.ases[asn].customers:
                    if(customer not in component):
                        combined_AS.add_neighbor(customer,2)
                        cust_AS = self.ases[customer]
                        cust_AS.providers.remove(asn)
                        cust_AS.append_no_dup(cust_AS.providers,new_asn)
                self.ases.pop(asn,None)

            all_combined_anns = combined_cust_anns + combined_peer_prov_anns
            num_anns = len(all_combined_anns)
            duplicate_anns = list()
            for i in range(num_anns):
                for j in range(i+1,num_anns):
                    if(all_combined_anns[i].prefix == all_combined_anns[j].prefix
                        and all_combined_anns[i].origin == all_combined_anns[j].origin):
                        if(all_combined_anns[i].as_path_length < all_combined_anns[j].as_path_length):
                            duplicate_anns.append(all_combined_anns[i])
                        else:
                            duplicate_anns.append(all_combined_anns[j])
            for dup in duplicate_anns:
                if dup in combined_cust_anns:
                    combined_cust_anns.remove(dup)
                if dup in combined_peer_prov_anns:
                    combined_peer_prov_anns.remove(dup)

            combined_AS.anns_from_customers = combined_cust_anns
            combined_AS.anns_from_peers_providers = combined_peer_prov_anns
            self.ases[combined_AS.asn] = combined_AS
            progress.update()
        print()
        return

    def find_strong_conn_components(self):

        #Begins Tarjan's Algorithm
        #index is node id in DFS from 
        index = 0
        stack = list()
        components = list()
        
        print("\tFinding Strongly Connected Components")
        progress = progress_bar(len(self.ases))
        
        for asn in self.ases:
            index = 0
            AS = self.ases[asn]
            if(AS.index is None):
                self.strong_connect(AS,index,stack,components)
            progress.update()
        self.strongly_connected_components = components
        print()
        return components

    def strong_connect(self,AS,index,stack,components):
    #Generally follows Tarjan's Algorithm. Does not use real recursion

        iteration_stack = list()
        iteration_stack.append(AS)
        SCC_id = 0

        while(iteration_stack):
            node = iteration_stack.pop()
            #If node hasn't been visited yet
            #initialize Tarjan variables
            if(node.index is None):
                node.index = index
                node.lowlink = index
                index = index + 1
                stack.append(node)
                node.onstack = True
            recurse = False
            for provider in node.providers:
                prov_AS = self.ases[provider]
                if(prov_AS.index is None):
                    iteration_stack.append(node)
                    iteration_stack.append(prov_AS)
                    recurse = True
                    break
                elif(prov_AS.onstack == True):
                    node.lowlink = min(node.lowlink, prov_AS.index)
            #if recurse is true continue to top of "iteration_stack"
            if(recurse): continue
            if(node.lowlink == node.index):
                #DO pop node WHILE node != top
                #stack until "node" is ASes in current component
                component = list()
                while(True):
                    top = stack.pop()
                    top.onstack = False
                    top.SCC_id = SCC_id
                    component.append(top.asn)
                    if(node == top):
                        break
                components.append(component)
                SCC_id = SCC_id + 1
            
            #if "dead end" was hit and it's not part of component
            if(iteration_stack):
                prov = node
                node = iteration_stack[-1]
                node.lowlink = min(node.lowlink, prov.lowlink)
        return

    def decide_ranks(self):
        customer_ases = list()

        for asn in self.ases:
            if(not self.ases[asn].customers):
                customer_ases.append(asn) 
                self.ases[asn].rank = 0
        self.ases_by_rank[0] = customer_ases

        for i in range(1000):
            ases_at_rank_i_plus_one = list()

            if(i not in self.ases_by_rank):
                return self.ases_by_rank

            for asn in self.ases_by_rank[i]:
                for provider in self.ases[asn].providers:
                    prov_AS = self.ases[provider]
                    if(prov_AS.rank is None):
                        skip_provider = 0
                        
                        for prov_cust in prov_AS.customers:
                            prov_cust_AS = self.ases[prov_cust]
                            if (prov_cust_AS.rank is None and
                                prov_cust_AS.SCC_id != prov_AS.SCC_id):
                                skip_provider = 1
                                break
                        if(skip_provider):
                                continue
                        else:
                            self.ases[provider].rank = i + 1
                            self.append_no_dup(ases_at_rank_i_plus_one,provider)
            if(ases_at_rank_i_plus_one):
                self.ases_by_rank[i+1] = ases_at_rank_i_plus_one
        return self.ases_by_rank
         

    def append_no_dup(self, this_list, asn, l = None, r = None):
   
        #initialize l and r on first call
        if(l is None and r is None):
            l = 0
            r = len(this_list)-1
        #if r is greater than l, continue binary search
        if(r>=l):
            half = int(l + (r-l)/2)
                #if asn is found in the list, return without inserting
            if(this_list[half] == asn):
                return
            elif(this_list[half] > asn):
                return self.append_no_dup(this_list, asn, l, half-1)
            else:
                return self.append_no_dup(this_list, asn, half+1, r)
        #if r is less than l, insert asn
        else:
            this_list[r+1:r+1] = [asn]
        return

