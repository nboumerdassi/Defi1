#!/usr/bin/env python
# coding: utf-8

# ### Import des librairies

# In[1]:


import pandas as pd
from zipfile import ZipFile
import os


# ### Définition working directory (optionnel)

# In[2]:


os.chdir("C:/Users/FR021091/Downloads")


# ### Défi 1 - Lecture des données (Python)

# In[ ]:


#afficher la liste des fichiers triés par date de modification
#sorted(filter(os.path.isfile, os.listdir('.')), key=os.path.getmtime)


# #### Liste des fichiers identifiés

# In[ ]:


files = [
 'validations-reseau-ferre-nombre-validations-par-jour-1er-semestre.csv',
 'frequentation-gares.csv',
 'validations-reseau-surface-nombre-validations-par-jour-1er-trimestre.csv',
 'histo-validations-reseau-surface.csv',
 'validations-reseau-ferre-profils-horaires-par-jour-type-1er-semestre.csv',
 'comptage-voyageurs-trains-transilien.csv',
 'validations-reseau-surface-nombre-validations-par-jour-2eme-trimestre.csv',
 'comptage-multimodal-comptages.csv'
]

print(len(files),  len(list(set(files))) ) 


# #### Lecture des fichiers identifiés

# In[ ]:


DATA = {}

for f__i in files:
    print(f__i)
    df__i = pd.read_csv(f__i,   sep = ';')
    DATA[f__i] = df__i
    print(df__i.shape)
    print(df__i.head())


# #### Lecture des fichier zip (historique validations)

# In[ ]:


archives =  ['data-rf-2019.zip',
 'data-rf-2021.zip',
 'data-rf-2017.zip',
 'data-rf-2022.zip',
 'data-rf-2018.zip',
 'data-rf-2015.zip',
 'data-rf-2016.zip',
 'data-rf-2020.zip']


# #### Processing des zip

# In[ ]:


import pandas as pd
from zipfile import ZipFile
import os


def unzip(arch__i, output_dir="temp_unzip_folder"):
    print("=================================>", arch__i)
    with ZipFile(arch__i, 'r') as zf:
        print("NAMELIST :       ", zf.namelist())
        for file in zf.namelist():
            print(file)
            with zf.open(file) as f:
                # Check if it's another ZIP and extract it to a temporary location
                if file.endswith('.zip'):
                    if not os.path.exists(output_dir):
                        os.makedirs(output_dir, exist_ok = True)
                    temp_path = os.path.join(output_dir, file)
                    with open(temp_path, 'wb') as temp_file:
                        temp_file.write(f.read())
                    unzip(temp_path)
                    os.remove(temp_path)
                    continue
                
                if file.endswith('.txt'):
                    sep = '\t'
                elif (file.endswith('.csv')) | (file in ["data-rf-2022/2022_S2_PROFIL_FER.txt", "data-rf-2022/2022_S2_PROFIL_FER.txt" ] ) :
                    sep = ';'
                else:
                    continue  # Skip unknown file types
                
                df__i = pd.read_csv(f, sep=sep)
                DATA[file] = df__i
                
                print(df__i.shape)
                print(df__i.head())



# In[ ]:


from zipfile import ZipFile

for arch__i in archives:
    unzip(arch__i)    
            


# ### Données SNCF - Ligne H

# In[3]:


os.listdir("Donnees_sncf")


# In[3]:


pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

data_compt = pd.read_csv(os.path.join("Donnees_sncf",   'PDT&Comptage_2022_Hackaton_IdFM_280923.csv'), sep = ';')


# In[24]:


data_compt.columns


# In[5]:


ref_gares = pd.read_csv("referentiel-gares-voyageurs.csv", sep =';')

mapping1 = {}
mapping1['VCX'] = 'VMS'
mapping1['GNX'] = 'GRL'

# Utilisation de la méthode map pour transformer la colonne
ref_gares['TVS_transf'] = ref_gares['TVS'].map(mapping1)

# Si vous souhaitez conserver les valeurs originales lorsqu'il n'y a pas de mapping:
ref_gares['TVS_transf'].fillna(ref_gares['TVS'], inplace=True)


# In[6]:


mapping = {}
mapping['PNB'] =  'PNO'
mapping['VW'] =  'VWG'
mapping['EN'] = 'ENK'
mapping['NO'] = 'NOJ'
mapping['CL'] = 'CLH'
mapping['MLV'] = 'MVC'

# Utilisation de la méthode map pour transformer la colonne
data_compt['CodeTT0020Localite_transf'] = data_compt['CodeTT0020Localite'].map(mapping)

# Si vous souhaitez conserver les valeurs originales lorsqu'il n'y a pas de mapping:
data_compt['CodeTT0020Localite_transf'].fillna(data_compt['CodeTT0020Localite'], inplace=True)

data_compt = pd.merge(data_compt, ref_gares, left_on = "CodeTT0020Localite_transf", right_on = "TVS_transf", how = 'left')


# In[28]:


bools = pd.DataFrame([( i__i ,c__i  , (c__i in ref_gares['TVS'].unique()) ) for i__i, c__i in zip( data_compt['Libelle_PR'].unique()  ,data_compt['CodeTT0020Localite'].unique() )])

bools.loc[[not x for x in bools[2].tolist()] ]


# In[ ]:


print(data_compt.columns)


# In[10]:


ref_dt_passage = "pointJalonnement_pointHoraire_horaireObserve_Arrivee_realise"


# In[20]:


data_compt = data_compt[~data_compt["pointJalonnement_pointHoraire_horaireObserve_Arrivee_realise"].isnull()]


# In[21]:


from datetime import datetime

def classify_hour(date_str):
    # Vérifiez si la valeur est 'nan' ou autre valeur non valide
    if pd.isna(date_str) or date_str == 'nan':
        return "Donnée manquante"

    # Convertissez la chaîne en objet datetime
    try:
        date_obj = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S.%fZ")
    except ValueError:
        return "Format incorrect"
    
    # Obtenez l'heure
    hour = date_obj.hour
    
    # Vérifiez si l'heure est dans la plage d'heure de pointe
    if (6 <= hour < 10) or (16 <= hour < 19):
        return "heure de pointe"
    else:
        return "pas heure de pointe"



data_compt['heure_pointe'] = [classify_hour(str(x)) for x in data_compt[ref_dt_passage] ]


# In[22]:


data_compt['heure_pointe'].unique()


# In[26]:


agg = data_compt.copy(deep=True)
agg['Date'] = pd.to_datetime(agg['Date'], format="%d/%m/%Y")

# Ajout du jour de la semaine
agg['Weekday'] = agg['Date'].dt.strftime('%A')

# Trier les données
agg = agg.sort_values(["Date", "NumeroMarche_realise", "pointJalonnement_rang_realise"])

# Créer la colonne "Code UIC_shift"
agg['Code UIC_shift'] = agg.groupby(["Date", "NumeroMarche_realise"])['Code UIC'].shift(-1)

# Filtrer les lignes avec des NaN dans "Code UIC_shift"
agg_filtered = agg[agg['Code UIC_shift'].notna()][["Libelle_PR" ,"Code UIC", "Code UIC_shift", 'Tx_occup_places_tot', "Weekday", 'heure_pointe', 'Latitude', 'Longitude']]

# Grouper par "Code UIC", "Code UIC_shift" et "Weekday", et calculer la moyenne d'occupation
Res = agg_filtered.groupby(["Libelle_PR" ,'Code UIC', 'Code UIC_shift', 'Weekday', 'heure_pointe', 'Latitude', 'Longitude'])['Tx_occup_places_tot'].mean().reset_index()


# In[34]:


Res.sort_values('Tx_occup_places_tot', ascending= False).tail(10)


# In[ ]:


Res


# In[24]:



for h__i in Res['heure_pointe'].unique():
    Res[Res['heure_pointe'] == h__i].to_csv(f'data_byWeekday_{h__i}.csv', sep = ',', encoding = 'utf-8-sig', index = False)


# In[ ]:


agg = data_compt.copy(deep = True)
agg['Date'] = pd.to_datetime(agg['Date'], format = "%d/%m/%Y")

for dt_fix in agg['Date'].unique():


    agg = agg[(agg["Date"] == dt_fix)]

    Res = pd.DataFrame()
    for m__i in agg["NumeroMarche_realise"].unique():
        agg__i = agg[(agg["NumeroMarche_realise"] == m__i) ]


        agg__i = agg__i.sort_values(["NumeroMarche_realise", "pointJalonnement_rang_realise"])

        agg__i['Code UIC_shift'] = agg__i['Code UIC'].shift(-1)
        agg__i = agg__i[["Code UIC" ,  'Code UIC_shift',"Occupation"]].dropna()

        Res = pd.concat([Res, agg__i], axis = 0)
    Res = Res.groupby(['Code UIC', 'Code UIC_shift'])['Occupation'].mean().reset_index()

print(Res.head())


# In[ ]:


Res = Res.groupby(['Code UIC', 'Code UIC_shift'])['Occupation'].mean().reset_index()


# In[ ]:


dt_fix.strftime("%A")


# In[ ]:


Res.to_csv(f'example_{dt_fix.strftime("%Y_%m_%d")}.csv', sep = ',', encoding = 'utf-8-sig', index = False)


# In[ ]:


agg = data_compt.copy(deep = True).groupby([ "pointJalonnement_rang_realise" ,"Intitulé gare", "Code UIC", "Latitude", 'Longitude'])['Occupation'].mean().reset_index()


print(agg.shape)
print(agg.head())


# In[ ]:


data_compt[data_compt['Intitulé gare'] == "Marne-la-Vallée Chessy"]


# In[12]:


Res.to_csv('res.csv', encoding = 'utf-8-sig', sep = ';', index = False)


# In[ ]:


print(data_compt.columns)


# In[ ]:


print(data_compt.head())


# In[10]:


dt_fix = pd.to_datetime("2022-01-05")
agg = data_compt.copy(deep = True)
agg['Date'] = pd.to_datetime(agg['Date'], format = "%d/%m/%Y")
agg = agg[(agg["Date"] == dt_fix)]


Res = pd.DataFrame()
for m__i in agg["NumeroMarche_realise"].unique():
    agg__i = agg[(agg["NumeroMarche_realise"] == m__i) ]
    

    agg__i = agg__i.sort_values(["NumeroMarche_realise", "pointJalonnement_rang_realise"])

    agg__i['Code UIC_shift'] = agg__i['Code UIC'].shift(-1)
    agg__i = agg__i[["NumeroMarche_realise", "pointJalonnement_rang_realise" ,"Code UIC" ,  'Code UIC_shift',"Occupation"]]

    Res = pd.concat([Res, agg__i], axis = 0)
    
    


# In[17]:




agg[agg["NumeroMarche_realise"] ==124006.0].sort_values(["NumeroMarche_realise", "pointJalonnement_rang_realise"]).to_csv('res.csv', encoding = 'utf-8-sig', sep = ';', index = False)


# In[19]:


data_compt[['CodeTT0020Localite_transf', "Libelle_PR", "Intitulé gare",  'TVS']].drop_duplicates().to_csv('res2.csv', encoding = 'utf-8-sig', sep = ';', index = False)

