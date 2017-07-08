'''
This Program is intended for figuring out anomalous transaction which is more
than mean plus 3 times the standard deviation of the last T purchase amounts
for D degree of friendship in a social network.
author : Sumit Bhattacharya
'''
import numpy
import json
from collections import OrderedDict

class Person(object):
    def __init__(self, unique_id, degree, T=2, purchase_amt=None, ts=None, friends=None):
        self.unique_id = unique_id
        self.degree = degree
        if friends:
            self.friends = friends
        else:
            self.friends = set()
        self.friendList = set()
        self.friendList_fof = set()
        self.txn =[]
        self.ts = ts
        self.purchase_amt = purchase_amt
        if purchase_amt:
            #self.txn[purchase_amt] = ts
            self.txn.append((ts, purchase_amt))
        self.T = T

    def AddFriend(self, friendid):
        self.friends.add(friendid)
        return self.friends

    def RemoveFriend(self, friendid):
        self.friends.remove(friendid)
        return self.friends

    def AddTxn(self, ts, purchase_amt):
        self.ts = ts
        self.purchase_amt = purchase_amt
        if purchase_amt:
            self.txn.append((ts, purchase_amt))

    # gets the friend of a friend upto D degree of friendship
    def GetFriends(self, degree, D, friendList, parent_obj):
        if degree < D:
            friendList.add(self)
        if degree > 0:
            for each in self.friends:
                if each != parent_obj:
                    each.GetFriends(degree-1, D, friendList, parent_obj)
        return friendList

class Anomaly_detection():
    def __init__(self):
        self.dummy=0

    def BeFriend(self,profile,lob,D):
        p_object_nm = 'P' + profile['id1']
        p_object = p_object_nm
        # create new obj if obj is not already created for a person
        # obj of friend 1 addind friend 2
        if p_object_nm not in lob:
            p_object = Person(profile['id1'], D)
            lob[p_object_nm] = p_object
        p_object = lob[p_object_nm]

        # create obj of friend 2 added by friend 1
        p_object_2_nm = 'P' + profile['id2']
        p_object_2 = p_object_2_nm
        if p_object_2_nm not in lob:
            p_object_2 = Person(profile['id2'], D)
            lob[p_object_2_nm] = p_object_2
        p_object_2 = lob[p_object_2_nm]

        # adding friends - friend 1 adding friend 2
        p_object.AddFriend(p_object_2)
        # since frienship is bothway, adding friend 1 to friend 2
        p_object_2.AddFriend(p_object)

    def UnFriend(self,profile,lob):
        p_object_nm = 'P' + profile['id1']
        p_object = p_object_nm
        p_object = lob[p_object_nm]

        # finding object of person unfriended
        p_object_2_nm = 'P' + profile['id2']
        p_object_2 = p_object_2_nm
        p_object_2 = lob[p_object_2_nm]
        # friend 1 unfriending friend 2
        p_object.RemoveFriend(p_object_2)
        # friend 2 unfriending friend 1
        p_object_2.RemoveFriend(p_object)

    def main(self):
        # List of obj of all the buddies
        lob={}
        # input batch file as history data
        batch_file='./log_input/batch_log.json'
        #batch_file='./sample_dataset/batch_log.json'
        with open(batch_file) as bf:
            befriend_identifier='befriend'
            purchase_identifier = 'purchase'
            unfriend_identifier = 'unfriend'
            line_counter = 0
            for line in bf:
                profile = json.loads(line)

                if line_counter == 0:
                    ## processing the header of batch file
                    if profile['D']:
                        D = profile['D']
                        D = int(D)
                    if profile['T']:
                        T = profile['T']
                        T = int(T)

                if line_counter > 0:
                    ## processing when purchase event happen
                    if profile['event_type'] == purchase_identifier:
                        p_object_nm = 'P' + profile['id']
                        p_object = p_object_nm
                        # create new obj if obj is not already created for a person
                        if p_object_nm not in lob:
                            p_object = Person(profile['id'], D, T, profile['amount'], profile['timestamp'] )
                            #p_object.AddFriend(p_object) ## Add ownself as friend
                            lob[p_object_nm] = p_object
                        else:
                            p_object = lob[p_object_nm]
                            p_object.AddTxn(profile['timestamp'], profile['amount'])
                    ## processing when add friend events happen
                    if profile['event_type'] == befriend_identifier:
                        self.BeFriend(profile, lob, D)

                    ## Unfriend request processign
                    if profile['event_type'] == unfriend_identifier:
                        self.UnFriend(profile,lob)


                line_counter = +1

        #print(lob)
        # for d_items in lob:
        #     print('OBJECT ITEMS' + d_items + ' are ' + str(lob[d_items].friends))
        #     for temp_id in lob[d_items].friends:
        #         print(temp_id.unique_id + 'amount' + str(temp_id.txn) )

        ## Starting to process from stream file
        stream_file='./log_input/stream_log.json'
        output_file='./log_output/flagged_purchases.json'
        with open(stream_file) as sf:
            for line in sf:
                profile = json.loads(line, object_pairs_hook=OrderedDict)
                # at the event of purchase
                if profile['event_type'] == purchase_identifier:
                    p_object_nm = 'P' + profile['id']
                    p_object = p_object_nm
                    if p_object_nm in lob:
                        # list to hold sorted txn
                        intermideate = []
                        # find D degree of friends
                        p_object = lob[p_object_nm]
                        p_object_friends = p_object.GetFriends(D, D, p_object.friendList, p_object)
                        p_object.friendList = set()

                        for lob_obj in p_object_friends:
                            #print('OBJECT ITEMS' + p_object_nm + ' are ' + str(lob_obj.unique_id))
                            if lob_obj != p_object:
                                intermideate = intermideate + lob_obj.txn

                        # sorting in the order of data received
                        firstTvals = sorted(intermideate,cmp=lambda x,y: cmp(x[0], y[0]))[-T:]
                        firstTvals = [i[1] for i in firstTvals]
                        #print(firstTvals)
                        mean = "%.2f" % (numpy.mean(map(float, firstTvals)))
                        # find sd of firstTvals
                        sd = "%.2f" % (numpy.std(map(float, firstTvals)))
                        #comparison
                        if len(firstTvals)>=2 and float(profile['amount']) > float(mean)+ (3*float(sd)):
                            # json dump
                            profile.update({'sd': str(sd), 'mean': str(mean)})
                            #print ('JSON', json.dumps(profile))
                            with open(output_file, 'a') as outfile:
                                json.dump(profile, outfile)
                                outfile.write('\n')

                        # adding the txn to the buyer
                        p_object.AddTxn(profile['timestamp'], profile['amount'])

                    else:
                        p_object = Person(profile['id'], D, T, profile['amount'], profile['timestamp'] )
                        #p_object.AddFriend(p_object) ## Add ownself as friend
                        lob[p_object_nm] = p_object

                ## processing when add friend events happen
                if profile['event_type'] == befriend_identifier:
                    self.BeFriend(profile, lob, D)


                ## Unfriend request processign
                if profile['event_type'] == unfriend_identifier:
                    self.UnFriend(profile,lob)

if __name__ == '__main__':
    ad = Anomaly_detection()
    ad.main()
