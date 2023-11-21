
import random
import pandas as pd
import datetime


# File for the dataset 
def randUsername():

    n1 = random.randint(0,6)
    n2 = random.randint(0,6)
    names1 =["Jhon","Andy","Sthep","Vicente","Leo","David","Diego"]
    names2 =["Panda","Super","Cool","Fantastik","Chill","Ms","Mr"]
    username = names1[n1] + " " + names2[n2]
    return username



def user_generator():

    users = []
    for x in range(0,5):
        n1 = random.randint(0,5)
        while n1 == x:
            n1 = random.randint(0,5)



        data = {
            "uid": '_:U'+str(x),
            "dgraph.type": 'user',
            "userName": randUsername(),
            "Nfollowers": random.randint(0,1000),
            "followedBy": [ {"uid":'_:U'+str(n1)}]
            
        }
        users.append(data)
    return users
def albun_generator():

    
    albunNames = ["Mm.. food","Postmortem","Joy as an act of resistance","Hot fuss","Disintegration","Discovery"]
    albunList= []
    for x in range(0,6):
        data = {
            "dgraph.type":'Albun',
            "uid":'_:A'+str(x),
            "Nsongs":10,
            "AlbunName":albunNames[x],
            "publishedBy":[{"uid":'_:Ar'+str(x)}]

        }
        albunList.append(data)
    return albunList


def song_list():
    songList =[]
    n2 = random.randint(1,5)
    for x in range(1,n2):
        rand =  random.randint(0,6)
        songList.append( {"uid":'_:S'+str(rand)})
    return songList
def playlist_generator():

    names1 = ["Las chidas para","Modo","Coleccion de"]
    names2 = ["Bailar","Estudiar","Salir en Bicicleta"]
    playlists = []
    for x in range (0,3):
        n1 = random.randint(0,2)
        n2 = random.randint(0,2)
        
        data  = {
            "dgraph.type": 'playlist',
            "uid":'_:P'+str(x),
            "playlistName":names1[x]+' '+names2[x],
            "Nsongs": 0,
            "ownedBy": [{"uid":'_:U'+str(x)}],
            "songs": song_list()


        }
        playlists.append(data)
    return playlists
def song_generator():
    

    songs = ["Rapp Snitch Knishes","220","Kil them with kindness","The man","disintegration","One more time"]
    songList = []
    
    for x in range(0,6):
        age = random.randint(1970,2023)
        time = random.uniform(1.5,5.5)
        data ={

            "dgraph.type": 'Song',
            "uid":'_:S'+str(x),
            "songName": songs[x],
            "time": round(time,2),
            "releasedOn": datetime.datetime(age, 7, 9, 10, 0, 0, 0).isoformat(),
            "appearsOn":[{"uid":'_:A'+str(x)}]

        }
        songList.append(data)
    return songList



def artist_generator():

    names1 = ["MF Doom","Dillom","IDLES","The Killers","The cure","Daft Punk"]
    cord = [-122.804489,45.485168,-45.27661,101.60098,-25.99617,128.74183]
    Artists =[]
    for x in range(0,5) :
        n1 = random.randint(0,5)
        n2 = random.randint(0,5)
        data ={
            "dgraph.type": 'Artist',    
            "uid": '_:Ar'+str(x),
            "artistName":names1[x],
            "origin": {
                "type": 'Point',
                "coordinates": [cord[n1], cord[n2]]
            },
            "followedByU":[{"uid":'_:U'+str(n2)}]

        }
        Artists.append(data)
    return Artists


def dataset_creator():

    #Users
    userfile = open("users.txt","w")
    users = user_generator()
    userfile.write(str(users))
    userfile.close()
    
    #Artist
    artistfile = open("artist.txt","w")
    artist = artist_generator()
    artistfile.write(str(artist))
    artistfile.close()
    
    #Albun
    albunfile = open("albun.txt","w")
    albun = albun_generator()
    albunfile.write(str(albun))
    albunfile.close()

    #Songs
    songsfile = open("songs.txt","w")
    songs = song_generator()
    songsfile.write(str(songs))
    songsfile.close()

    #Playlist
    playlistfile = open("playlist.txt","w")
    playlist = playlist_generator()
    playlistfile.write(str(playlist))
    playlistfile.close()



dataset_creator()