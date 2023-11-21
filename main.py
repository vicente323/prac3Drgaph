#!/usr/bin/env python3
import os

import pydgraph

import model

DGRAPH_URI = os.getenv('DGRAPH_URI', 'localhost:9080')



def print_menu():
    mm_options = {
        1: "Create data",
        2: "Search person",
        3: "Delete person",
        4: "Drop All",
        5: "Exit",
        6: "search friends artist",
        7: "search users by more than Nfollowers",
        8: "search how may users are in the graph",
        9: "Get all users ordered"
        
    }
    for key in mm_options.keys():
        print(key, '--', mm_options[key])


def create_client_stub():
    return pydgraph.DgraphClientStub(DGRAPH_URI)


def create_client(client_stub):
    return pydgraph.DgraphClient(client_stub)


def close_client_stub(client_stub):
    client_stub.close()


def main():
    # Init Client Stub and Dgraph Client
    client_stub = create_client_stub()
    client = create_client(client_stub)

    # Create schema
    model.set_schema(client)

    while(True):
        print_menu()
        option = int(input('Enter your choice: '))
        if option == 1:
            model.create_data(client)
        if option == 2:
            person = input("Name: ")
            model.search_person(client, person)
        if option == 3:
            person = input("Name: ")
            model.delete_user(client, person)
        if option == 4:
            model.drop_all(client)
        if option == 5:
            model.drop_all(client)
            close_client_stub(client_stub)
            exit(0)

        if option == 6:
            Artist = input("Artist: ")
            model.search_friends_artists(client,Artist)
        if option == 7:
            x = input("Nfollowers: ")
            model.get_users_with_gtr_follows(client,int(x))
        if option== 8:
            model.countUsers(client)
        if option== 9:
            model.getUsersOrder(client)
if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print('Error: {}'.format(e))
