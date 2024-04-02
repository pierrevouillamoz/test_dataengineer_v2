from flask import Flask, render_template, request, url_for, flash, redirect
app = Flask(__name__)

import redis
import json
import pandas
import recuperation as recup
import exploitation as explo

r=redis.Redis(host="localhost", port=6379, db=0)

@app.route('/')
def index():
    
    """
    Affichage de la page d'acceuil
    """
    
    return render_template("index.html")

@app.route('/recuperation_nettoyage', methods=('POST',))
def recuperation_nettoyage():
    
    """
    Exécution du script de récupération des données.
    """
    
    #La récupération des données se fait en une ligne (on récupère un dataframe)
    Arretes_Zones_alerte=recup.fonction_recuperation()
    #On transforme le dataframe en json pour le stocker dans la base
    df_json=Arretes_Zones_alerte.to_json()
    #On le stocke dans la base
    r.flushdb()
    r.set("dataframe_key", df_json)
    return redirect(url_for('index'))

@app.route('/graph',methods=('GET','POST'))
def graph():
    
    """
    Exploitation des données (i.e. production du tableau de bord selon la date donnée)
    Renvoie vers la page graph.html
    """
    
    if request.method == "POST":
        date = request.form["date"]
        
        #Récupération du dataframe depuis la base de données
        df_json_from_redis=r.get('dataframe_key')
        data=pd.read_json(df_json_from_redis)
        
        #l'exploitation des données se fait en une ligne
        explo.fonction_exploitation(data=data,date=date)
        return render_template("graph.html", date=date)
    
    return render_template('graph.html', date=None)
 
if __name__ == "__main__":
    app.run("0.0.0.0", 5000)

app.run(debug=True)