import requests

url_ckan='http://datos.gob.cl/api/3/action/package_list'
url_package='http://datos.gob.cl/api/3/action/package_show?id='
url_package_search='http://datos.gob.cl/api/3/action/package_search?'

#Listas de paquetes ckan (con sus ID)
ckan_packages = requests.get(url_ckan) #consultar api
ckan_packages = ckan_packages.json() #convertir respuesta a json


ckan_packages['result'][0:15]

#OBTENER TODOS LOS PAQUETES DE LA BASE DE DATOS CKAN
rows=str(1000) #maximo numero de resultados por consulta (maximo permitido es 1000)
packs=[] #contenedor de paquetes retornados
for s in range(0,4):
    start=str(s*int(rows)) #paginacion
    packages=requests.get(url_package_search+'rows='+rows+'&'+'start='+start)
    packages=packages.json()
    packs.extend(packages['result']['results'])


import copy
import pandas as pd
recursos=[]
for p in packs:
    for r in p['resources']:
        d=copy.deepcopy(r)
        #parámetros del paquete
        d['package_id']=p['id'] #id
        d['package_name']=p['name'] #nombre
        d['package_title']=p['title'] #titulo
        d['package_notes']=p['notes'] #notas
        if 'organization' in p.keys(): #si es que existe info de organizacion
            if p['organization']!=None:
                d['inst.']=p['organization']['title'] #si es que existe info del titulo incluirlo
                d['inst_name']=p['organization']['name'] #si es que existe info del titulo incluirlo
                d['inst_id']=p['organization']['id'] #si es que existe info del id inst.
            else:
                d['inst.']=None
                d['inst_name']=None
                d['inst_id']=None
        recursos.append(d)
recursos=pd.DataFrame(recursos) #convertir a df


recursos.to_csv("recursos.csv", header=True)




# import urllib
# url = 'https://datos.gob.cl/en/api/3/action/datastore_search?resource_id=cf0f7fc9-2462-47e0-8154-ab177175a0df&limit=5&q=title:jones'
# fileobj = urllib.urlopen(url)
# print
# fileobj.read()