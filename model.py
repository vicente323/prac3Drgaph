#!/usr/bin/env python3
import datetime
import json
import pandas as pd 
import pydgraph
import ast


def set_schema(client):
    schema = """
    type Artist{
        artistName
        origin
        followedByU
    }

    type Albun{
        AlbunName
        Nsongs
        publishedBy
    }

    type Song{

        songName
        time
        releasedOn
        appearsOn
        likedBy
    }

    type user{
        userName
        Nfollowers
        followedBy
    }
    type playlist{
        playlistName
        Nsongs
        ownedBy
        songs
    }
    releasedOn: datetime .
    appearsOn: [uid] .
    artistName: string @index(exact) .
    origin: geo .
    AlbunName: string @index(exact) .
    Nsongs: int . 
    time: int .
    songName: string @index(exact) .
    publishedBy: [uid] .
    userName: string @index(exact) .
    Nfollowers: int @index(int) .
    followedBy: [uid]  .
    followedByU: [uid] .
    likedBy: [uid] .
    playlistName: string @index(exact) .
    ownedBy: [uid] .
    songs: [uid] @reverse .
    """
    return client.alter(pydgraph.Operation(schema=schema))

def data_parser(file:str):

    '''
        data parser, gets an txt and returns an array of dicts
    '''
    userfile = open(file,'r')
    line = userfile.readlines()
    line = line[0]

    ArrayOfData = ast.literal_eval(line)
  
    userfile.close()
    return(ArrayOfData)
      
  
def create_data(client):
  

    albunDataArray = data_parser('albun.txt')
    artistDataArray = data_parser('artist.txt')
    userDataArray = data_parser('users.txt')
    songsDataArray = data_parser('songs.txt')
    playlistDataArray = data_parser('playlist.txt')
    finalData = albunDataArray + artistDataArray + userDataArray + songsDataArray + playlistDataArray
    txn = client.txn()
    try:
            p = finalData
            
            response = txn.mutate(set_obj=p)

            # Commit transaction.
            commit_response = txn.commit()
            print(f"Commit Response: {commit_response}")

            print(f"UIDs: {response.uids}")
    finally:
            # Clean up. 
            # Calling this after txn.commit() is a no-op and hence safe.
            txn.discard()
    
def get_users_with_gtr_follows(client,x):
    # Create a new transaction.
    txn = client.txn()
    
    query = """query userWithXorMore($a: int) {
             all(func: gt(Nfollowers, $a)) {
                userName
                Nfollowers
            }
        }"""
    variables = {'$a': str(x)}
    res = client.txn(read_only=True).query(query, variables=variables)
    ppl = json.loads(res.json)
    print(f"Number of people named {x}: {len(ppl['all'])}")
    print(f"Data associated with {x}:\n{json.dumps(ppl, indent=2)}")

def getUsersOrder(client):
    query = """
        query getUsers(){
            getUsers(func: has(Nfollowers), order:{asc: Nfollowers}, first: 10, offset: 0) {
                userName
                Nfollowers
            }
        }
        """

        
    res = client.txn(read_only=True).query(query)
    ppl = json.loads(res.json)

    # Print results.
    print(f"Number of people : {len(ppl['all'])}")

def countUsers(client):
    query1 = """query countUsers() {
            all(func: has(userName)) {
               totalUsers:count(uid)
            }
        }"""

 
    res = client.txn(read_only=True).query(query1)
    ppl = json.loads(res.json)

    # Print results.
    print(f"Number of people : {len(ppl['all'])}")
    

def delete_user(client, name):
    # Create a new transaction.
    txn = client.txn()
    try:
        query1 = """query search_person($a: string) {
            all(func: eq(userName, $a)) {
               uid
            }
        }"""
        variables1 = {'$a': name}
        res1 = client.txn(read_only=True).query(query1, variables=variables1)
        ppl1 = json.loads(res1.json)
        for person in ppl1['all']:
            print("UID: " + person['uid'])
            txn.mutate(del_obj=person)
            print(f"{name} deleted")
        commit_response = txn.commit()
        print(commit_response)
    finally:
        txn.discard()
def search_friends_artists(client,artistName):
    query = """query searchArtist($a: string) {
        all(func: eq(artistName, $a)) {
           artistName
           followedByU{
          userName
          followedBy{

          userName}
            }
        }
    }"""
    variables = {'$a': artistName}
    res = client.txn(read_only=True).query(query, variables=variables)
    ppl = json.loads(res.json)
    print(f"Data associated with {artistName}:\n{json.dumps(ppl, indent=2)}")

def search_person(client, userName):
    query = """query search_person($a: string) {
        all(func: eq(userName, $a)) {
            userName
            Nfollowers
        }
    }"""

    variables = {'$a': userName}
    res = client.txn(read_only=True).query(query, variables=variables)
    ppl = json.loads(res.json)

    # Print results.
    print(f"Number of people named {userName}: {len(ppl['all'])}")
    print(f"Data associated with {userName}:\n{json.dumps(ppl, indent=2)}")


def drop_all(client):
    return client.alter(pydgraph.Operation(drop_all=True))
