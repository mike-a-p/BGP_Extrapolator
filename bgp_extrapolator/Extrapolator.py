'''
Created on Aug 23, 2018

@author: Mike P

May Require 'psycopg2'
"pip3 install psycopg2"
'''
import sys
import os
import time
import csv
from datetime import datetime
import math
import multiprocessing as mp
import named_tup
from SQL_querier import SQL_querier
from AS_Graph import AS_Graph
from AS import AS
from Announcement import Announcement
from progress_bar import progress_bar

class extrapolator:
    def __init__(self):
        """Initialize basic extrapolator variables.

        """
        self.querier = SQL_querier()
        self.graph = AS_Graph()
        self.ases_with_anns = list()
        self.today = datetime.today().strftime('%Y_%m_%d')
        self.results_counter = 0
        return


    #TODO Throw proper errors when checking existence of tables.
    # Combine all set_x_table functions

    def set_ann_input_table(self,table_name):
        """Set name of table to retrieve announcements from. If table with name
            doesn't exist program exits.

        Args:
            table_name (:obj:`str`): Name of input table.

        """

        exists = self.querier.exists_table(table_name)
        if(exists):
            self.ann_input_table_name = table_name
        else:
            print("Table name \"" + table_name + "\" not found in database. Check config file or database.")
            sys.exit()
        return

    def set_results_table(self,table_name):
        """Set name of table to send results to. If table with name doesn't
            exist program exits.

        Args:
            table_name (:obj:`str`): Name of results table.
        
        """

        self.querier.set_results_table(table_name)
        return
    
    def set_peers_table(self,table_name):
        """Set name of table to get peer-peer AS relationships from. If table 
            with name doesn't exist program exits.

        Args:
            table_name (:obj:`str`): Name of peer-peer table.

        """

        self.graph.set_peers_table(table_name)
        return
    
    def set_customer_provider_table(self,table_name):
        """Set name of table to get customer-provider AS relationships from. If
            table with name doesn't exist program exits.

        Args:
            table_name (:obj:`str`): Name of customer-provider table.

        """

        self.graph.set_customer_provider_table(table_name)
        return
        
    def set_graph_table(self,table_name):
        """Set name of table to read/write processed AS relationship data to.
            If table with name doesn't exist program exits.

        Args:
            table_name (:obj:`str`): Name of graph table.

        """

        self.graph.set_graph_table(table_name)
        return

    def perform_propagation(self, max_total_anns = None,iteration_size = None, test = False):
        """Performs announcement propagation and uploads results to database.
            :meth:`~insert_announcements()`\n 
            :meth:`~prop_anns_sent_to_peers_providers()`\n
            :meth:`~propagate_up()`\n
            :meth:`~propagate_down()`\n
            :meth:`~upload_to_db()`\n

        Args:
            use_db (:obj:`int` or `boolean`): Signifies using database to gather data.
            num_announcements (:obj:`int`, optional): Number of announcements to propagate.

        """

        #Currently written to append to existing rows in DB, one row per AS is 
        #initialized.
        #Will be unused when database schema changes
#        self.querier.initialize_results_keys(list(self.graph.ases.keys()))
        if(not test):
            try:
                os.mkdir(self.today)
                self.prefix_group_filename = "./" + self.today + "/prefix_grouping.txt"
            except FileExistsError:
                print("Directory", self.today, """already exists, consider removing 
                    this directory or running with '-t' (--test) arg.""") 
                
        start_time = time.time()
      
        #Default group size is 1000 for propagation
        if(iteration_size is None):
            max_group_anns = 1000
        else:
            max_group_anns = iteration_size
       
        #List prefixes by how many times they are announced
        #With simplified list (bgp dump)  this equates to how many origins 
        #announce it.
        print("Ordering prefixes by frequency...") 
        prefix_counts = self.querier.count_prefix_amounts(self.ann_input_table_name)


        #Perform propagation in groups of prefixes.
        #i:j identify a section of prefixes to use
        i = len(prefix_counts)-1
        j = len(prefix_counts)-1
        total_anns = 0
        stop = False

        #While the list of prefixes isn't exhausted and the next prefix doesn't exceed max_total_anns
        while(i >=0 and ((not max_total_anns) or total_anns + prefix_counts[i].count <= max_total_anns)):
            anns_in_group = 0
            #While prefixes aren't exhausted and the next prefix doesn't exceed max_group_anns
            while(i>=0 and anns_in_group + prefix_counts[i].count < max_group_anns):
                #Also, if next prefix exceeds max_total_anns don't add it
                if(max_total_anns and total_anns + prefix_counts[i].count > max_total_anns):
                    break
                anns_in_group+= prefix_counts[i].count
                total_anns +=prefix_counts[i].count
                i-=1
            prefixes_to_use = prefix_counts[i+1:j+1]
            j=i
    
            #Perform propagation on group
            self.insert_announcements(prefixes_to_use)
            self.prop_anns_sent_to_peers_providers()
            self.propagate_up()
            start_down = time.time()
            self.propagate_down()
            #Save results of group, they will be removed from memory
            if(not test):
                self.save_results(prefixes_to_use)
            self.graph.clear_announcements()
     
        #Save graph 
    #    if(not test):
     #       self.graph.save_graph_to_db()

        end_time = time.time()
        print("Total Time To Extrapolate: " + str(end_time - start_time) + "s")
        return
   
    def send_all_announcements(self,asn,to_peers_providers = False, to_customers = False):
        """Sends all announcements kept by an AS to it's neighbors.
    
        Args:
            asn (:obj:`int`): ASN of AS sending announcements
            to_peers_providers (:obj:`boolean`, optional): Whether or not 
                announcement will be sent to peers and providers.
            to_customers (:obj:`boolean`, optional): Whether or not 
                announcement will be sent to customers.

        """

        source_as = self.graph.ases[asn]
        if(to_peers_providers):
            anns_to_providers = list()
            anns_to_peers = list()
            for ann in source_as.all_anns:
                ann = source_as.all_anns[ann]
                #Peers/providers should not be sent anns that came from peers/providers
                if(ann.priority < 2):
                    continue

                #Priority after decimal is reduced by 0.01 for AS_PATH length
                new_length_priority = ann.priority - int(ann.priority)
                new_length_priority -=  0.01

                #For providers priority before decimal is 2 (from customer)
                new_priority = 2 + new_length_priority
                this_ann = Announcement(ann.prefix, ann.origin, new_priority, asn)
                anns_to_providers.append(this_ann)
                
                #For peers priority before decimal is 1 (from peer)
                new_priority = 1 + new_length_priority
                this_ann = Announcement(ann.prefix, ann.origin, new_priority, asn)
                anns_to_peers.append(this_ann)

            #Give peers/providers announcements
            for provider in source_as.providers:
                self.graph.ases[provider].receive_announcements(anns_to_providers)
            for peer in source_as.peers:
                self.graph.ases[peer].receive_announcements(anns_to_peers)

        if(to_customers):
            anns_to_customers = list()
            for ann in source_as.all_anns:
                ann = source_as.all_anns[ann]

                #Priority after decimal is reduced by 0.01 for AS_PATH length
                #Priority before decimal is 0 (from provider)
                new_length_priority = ann.priority - int(ann.priority)
                new_length_priority -=  0.01
                new_priority = 0 + new_length_priority

                this_ann = Announcement(ann.prefix, ann.origin, new_priority, asn)
                anns_to_customers.append(this_ann)

            #Give customers announcements
            for customer in source_as.customers:
                self.graph.ases[customer].receive_announcements(anns_to_customers)

        return

    def prop_anns_sent_to_peers_providers(self):
        """Send announcements known to be sent to a peer or provider of each AS to
            the other peers and providers of each AS
       
        """
        print("Propagating Announcements Sent to Peers/Providers...")
        
        for asn in self.ases_with_anns:
            source_as = self.graph.ases[asn]
            source_as.process_announcements()
            anns_to_send = source_as.anns_sent_to_peers_providers
            if(anns_to_send):
                for peer in source_as.providers:
                    self.graph.ases[peer].receveive_announcements(anns_to_send)
                for provider in source_as.providers:
                    self.graph.ases[provider].receveive_announcements(anns_to_send)
        return

    def propagate_up(self):
        """Propagate announcements that came from customers to peers and providers
        
        """
        
        print("Propagating Announcements From Customers...")
        graph = self.graph
        progress = progress_bar(len(graph.ases_by_rank))
        for level in range(len(graph.ases_by_rank)):
            for asn in graph.ases_by_rank[level]:
                graph.ases[asn].process_announcements()
                if(graph.ases[asn].all_anns):
                    self.send_all_announcements(asn, to_peers_providers = True, 
                                               to_customers = False)
            progress.update()
        progress.finish()
        return

    def propagate_down(self):
        """From "top" to "bottom"  send the best announcements at every AS to customers

        """
        print("Propagating Announcements To Customers...")
        graph = self.graph 
        progress = progress_bar(len(graph.ases_by_rank))
        for level in reversed(range(len(graph.ases_by_rank))):
            for asn in graph.ases_by_rank[level]:
                graph.ases[asn].process_announcements()
                if(graph.ases[asn].all_anns):
                    self.send_all_announcements(asn, to_peers_providers = False,
                                                to_customers = True)
            progress.update()
        progress.finish()
        return

    def give_ann_to_as_path(self, as_path, prefix, hop):
        """Record announcement to all ASes on as_path
        
        Args:
            as_path(:obj:`list` of :obj:`int`): ASNs showing the path taken by
                an announcement. Leftmost being the most recent.
            prefix(:obj:`string`): An IP address set e.g. 123.456.789/10 
                where 10 depicts the scope of the IP set.
            hop(:obj:`str`): First ASN after origin.

        """
         
        #avoids error for anomaly announcement with no path
        if(as_path is None):
                return

        #as_path is ordered right to left, so rev_path is the reverse
        rev_path = as_path[::-1]

        ann_to_check_for = Announcement(prefix,rev_path[0],None,None)

        #i used to traverse as_path
        i = 0 
        for asn in rev_path: 
            i = i + 1
            if(asn not in self.graph.ases):
                continue
            comp_id = self.graph.ases[asn].SCC_id
            if(comp_id in self.ases_with_anns):
                #If AS has already recorded origin/prefix pair, stop
                if(self.graph.ases[comp_id].already_received(ann_to_check_for)):
                    continue
            sent_to = None
            #If not at the most recent AS (rightmost in rev_path), record the AS it is sent to next
            if(i<len(as_path)-1):
                #similar to rec_from() function, could get own function
                found_sent_to = 0
                asn_sent_to = rev_path[i+1]
                if(asn_sent_to not in self.graph.strongly_connected_components[comp_id]):
                    if(asn_sent_to in self.graph.ases[comp_id].providers):
                        sent_to = 0
                        found_sent_to = 1
                    if(not found_sent_to):
                        if(asn_sent_to in self.graph.ases[comp_id].peers):
                            sent_to = 1
                            found_sent_to = 1
                    if(not found_sent_to):
                        if(asn_sent_to in self.graph.ases[comp_id].customers):
                            sent_to = 2
                            found_sent_to = 1
            
	    #Used to identify when ASes in path aren't neighbors
            broken_path = False
            #assign 'received_from' if AS isn't first in as_path
            received_from = 3
            if(i > 1):
                if(rev_path[i-1] in self.graph.ases[comp_id].providers):
                    received_from = 0
                elif(rev_path[i-1] in self.graph.ases[comp_id].peers):
                    received_from = 1
                elif(rev_path[i-1] in self.graph.ases[comp_id].customers):
                    received_from = 2
                elif(rev_path[i-1] == asn):
                    received_from = 3
                else:
                    broken_path = True
                    
            this_path_len = i-1
            path_length_weighted = 1 - this_path_len/100

            priority = received_from + path_length_weighted
            if(not broken_path):
                announcement = Announcement(prefix,rev_path[0],priority,rev_path[i-1])
                if(sent_to == 1 or sent_to == 0):
                    self.graph.ases[asn].sent_to_peer_or_provider(announcement)
                
                announcement = [announcement,]
                self.graph.ases[asn].receive_announcements(announcement)
                #ases_with_anns
                self.ases_with_anns.append(comp_id)

        self.ases_with_anns = list(set(self.ases_with_anns))
        return

    def insert_announcements(self,prefixes):
        """Asks for all announcements that use each prefix in prefixes and 
            inserts them into the graph using :meth:`~give_ann_to_as_path()`
            
        Args:
            prefixes (:obj:`list` of :obj: `str`): Prefixes to get 
                announcements for.
      
        """

        print("\nInserting Announcements...")

        start_time = time.time()

        records = list()
        #Get all announcements for each prefix in 'prefixes'
        for prefix in prefixes:
            records.append(self.querier.select_anns_by_prefix(self.ann_input_table_name,prefix.prefix))

        for prefix_anns in records:
            for ann in prefix_anns:
                if(ann.element_type == 'A'):
                    self.give_ann_to_as_path(ann.as_path, ann.prefix, ann.next_hop)
        return

    #TODO add option to overwrite results if directory already exists
    def save_results(self,prefixes):
        print("Saving Propagation results to DB")
        start_time = time.time()
        #append prefix group to prefix group file. Used to see which files contain which prefixes.
        with open(self.prefix_group_filename,'a') as outfile:
            prefix_group = list()
            #prefixes from arg are 'records' from database.
            for record in prefixes:
                prefix_group.append(record.prefix)
            prefix_group = ','.join(prefix_group)
            outfile.write(str(self.results_counter) + " - " + prefix_group)
            self.results_counter+=1

        file_name = "./" + self.today + "/" + str(self.results_counter) + ".txt"
        with open(file_name, 'a') as outfile:
            asn_list = list(self.graph.ases.keys())
            graph_size = len(asn_list)
            progress = progress_bar(len(asn_list))
            for asn in asn_list:
                #identify section by asn
                outfile.write(str(asn) + "\n\n")
                outfile.write("prefix,origin,priority,received_from_asn\n")
                for prefix in self.graph.ases[asn].all_anns:
                    ann = self.graph.ases[asn].all_anns[prefix]
                    outfile.write("%s,%s,%s,%s\n"%(ann.prefix,ann.origin,ann.priority,ann.received_from_asn))
                outfile.write("\n\n")
                progress.update()
            progress.finish()
            end_time = time.time()
            print("Time To Save Announcements: " + str(end_time - start_time) + "s")
        return 
