import pandas as pd
import datetime as dt

##URL des fichiers rassemblés au même endroit pour anticiper mise à jour ultérieures
URL=dict()

URL["zones_alerte"]="https://www.data.gouv.fr/fr/datasets/r/ac45ed59-7f4b-453a-9b3d-3124af470056"
URL["carte_zones_arretes"]="https://www.data.gouv.fr/fr/datasets/r/3ab674ef-ec02-44d7-a5f7-3002f5053d1e" #geojson peut-être carte actuelle
URL["carte_departements"]="https://www.data.gouv.fr/fr/datasets/r/90b9341a-e1f7-4d75-a73c-bbc010c7feeb" #geojson contour des départements
URL["zones_alerte_communes"]="https://www.data.gouv.fr/fr/datasets/r/25cfc138-313e-4e41-8eca-d13e3b04ca62"
URL["guide_restrictions"]="https://www.data.gouv.fr/fr/datasets/r/b6e9da4a-d9e1-4854-8ea1-a87b86a0a5ca"
URL["restrictions"]="https://www.data.gouv.fr/fr/datasets/r/07ebbc43-ea41-4e6d-a21c-58cedebe1320" #Problème de lecture
URL["arretes_2023"]="https://www.data.gouv.fr/fr/datasets/r/782aac32-29c8-4b66-b231-ab4c3005f574"
URL["arretes_2022"]="https://www.data.gouv.fr/fr/datasets/r/0fee8de1-c6de-4334-8daf-654549e53988"
URL["arretes_2021"]="https://www.data.gouv.fr/fr/datasets/r/c23fe783-763f-4669-a9b7-9d1d199fcfcd"
URL["arretes_2020"]="https://www.data.gouv.fr/fr/datasets/r/d16ae5b1-6666-4caa-930c-7993c4cd4188"
URL["arretes_2019"]="https://www.data.gouv.fr/fr/datasets/r/ed2e6cfa-1fe7-40a6-95bb-d9e6f99a78a0"
URL["arretes_2018"]="https://www.data.gouv.fr/fr/datasets/r/8ba1889e-5496-47a6-8bf3-9371086dd65c"
URL["arretes_2017"]="https://www.data.gouv.fr/fr/datasets/r/ab886886-9b64-47ca-8604-49c9910c0b74"
URL["list_dept"]="https://www.data.gouv.fr/fr/datasets/r/bbae8ea2-4d53-4f96-b7eb-9c08f66a07c5"

##Fonctions de nettoyage

def nettoyage_date(col_date):

    """
    """
       
    col_date=col_date.replace(to_replace='00',value='20', regex=True)
    col_date=pd.to_datetime(col_date)
    return col_date

def nettoyage_doublons(Arretes):

    """
    Le dataset des arrêtés contient des doublons dûs aux modifications d'arrêtés.
    Par exemple, les lignes dont les variables id_arrete ont pour valeurs 32957 et 32966 concernent l'arreté de restriction 2023-06-30-001, 
    pris sur la même zone d'alerte (12181). Ce qui diffère, c'est la date de fin d'arrêté qui passe du 7 juillet 2023 au 17 juillet 2023.
    Je peux supposer raisonnablement qu'il y a soit un arrêté modificatif qui prolonge l'arrêté initial, ou bien l'opérateur a fait une erreur et il
    a rempli la base de données une seconde fois.
    """

    #Nettoyage de base : lignes en double si arrêté couvre plusieurs années
    Arretes=Arretes.drop_duplicates()
    #Le dataset est trié dans l'ordre des arretés
    Arretes=Arretes.sort_values(by="id_arrete")

    #Suppression des doublons si même nom et même numéro d'arrêté. 
    #Nous gardons la dernière valeur du doublon considérée comme juste.
    Arretes=Arretes.drop_duplicates(subset=["id_zone","numero_arrete"], keep='last')

    return Arretes

def nettoyage_zones_alerte(Zones_alerte):
  
    #Création d'un ratio pour nettoyer les valeurs abérantes
    Zones_alerte["ratio_surface"]=Zones_alerte["surface_zone"]/Zones_alerte["surface_departement"]

    #Suppression de toutes les zones dont la surface est supérieure à celle du département
    Zones_alerte=Zones_alerte[Zones_alerte["ratio_surface"]<=1]

    #Suppression des zones dont la surface est nulle
    Zones_alerte=Zones_alerte[Zones_alerte["surface_zone"]>0]

    #NB : 5% des Zones d'alertes font moins de 1,2km carrés, l'article du code de l'environnement parle de "unité hydrologique cohérente"
    #d'après les publications des praticiens, une zone peut être de taille très petite (délimité par une butte).
    #Je choisi de garder les données de faibles valeurs, au risque d'être abérrantes car j'utiliserais plus tard une fonction somme 
    #(l'impact des faibles valeurs est négligeable)
    
    return Zones_alerte
    
def master_dataset(Arretes, Zones_alerte):  #Ca marche, ajouter le geojson pour la cartographie

    #Sélection des variables que l'on souhaite conserver
    Zones_alerte=Zones_alerte[["id_zone","type_zone","surface_zone","code_departement","surface_departement"]]
    Arretes=Arretes[["id_arrete","id_zone","nom_niveau","numero_niveau","debut_validite_arrete","fin_validite_arrete"]]

    #Fusion des deux tableaux de type inner, left aurait été mieux mais cela aurait conduit à  un tableau incomplet si absence d'id_zone dans le dataset
    #Zone_alerte
    Arretes_Zones_alerte=pd.merge(Arretes, Zones_alerte, on="id_zone", how="inner")

    #Ajout d'une variable durée
    Arretes_Zones_alerte["duree (jours)"]=Arretes_Zones_alerte["fin_validite_arrete"]-Arretes_Zones_alerte["debut_validite_arrete"]
    
    #Nous allons retravailler avec des entiers plutôt que des timedelta
    Arretes_Zones_alerte["duree (jours)"]=Arretes_Zones_alerte["duree (jours)"].dt.days
    
    #On s'assure de n'avoir que des durées positives
    Arretes_Zones_alerte=Arretes_Zones_alerte[Arretes_Zones_alerte["duree (jours)"]>0]

    
    return Arretes_Zones_alerte


##Chargement des données

#Pour simplifier le projet et économiser de la mémoire, nous ne téléchargerons que les
#jeux de données "zones d'alerte" et "arrêtés"
    
def fonction_recuperation():
    
    
    Zones_alerte=pd.read_csv(URL["zones_alerte"], low_memory=False)

    Arretes=list()

    for i in range(2020,2024):
        Arretes.append(pd.read_csv(URL["arretes_{}".format(i)], low_memory=False))

    Arretes=pd.concat(Arretes, axis=0)

    ##Nettoyage


    Arretes=Arretes.reset_index()
    Arretes=Arretes.drop(columns="index")


    Arretes["fin_validite_arrete"]=nettoyage_date(Arretes["fin_validite_arrete"])
    Arretes["debut_validite_arrete"]=nettoyage_date(Arretes["debut_validite_arrete"])

    Arretes=Arretes.dropna(subset=["debut_validite_arrete","fin_validite_arrete"])
    Arretes=nettoyage_doublons(Arretes)
    Zones_alerte=nettoyage_zones_alerte(Zones_alerte)

    ##Jointure des tableaux
    Arretes_Zones_alerte=master_dataset(Arretes, Zones_alerte)

    ##Sortie de fonctions
    return Arretes_Zones_alerte


