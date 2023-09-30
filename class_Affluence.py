import pandas as pd
from datetime import datetime

class DataProcessor:
    
    def __init__(self, data_path, ref_gares_path):
        self.data_compt = pd.read_csv(data_path, sep=';')
        self.ref_gares = pd.read_csv(ref_gares_path, sep=';')
    
    def transform_ref_gares(self):
        mapping1 = {'VCX': 'VMS', 'GNX': 'GRL'}
        self.ref_gares['TVS_transf'] = self.ref_gares['TVS'].map(mapping1)
        self.ref_gares['TVS_transf'].fillna(self.ref_gares['TVS'], inplace=True)
        
    def transform_data_compt(self):
        mapping = {'PNB': 'PNO', 'VW': 'VWG', 'EN': 'ENK', 'NO': 'NOJ', 'CL': 'CLH', 'MLV': 'MVC'}
        self.data_compt['CodeTT0020Localite_transf'] = self.data_compt['CodeTT0020Localite'].map(mapping)
        self.data_compt['CodeTT0020Localite_transf'].fillna(self.data_compt['CodeTT0020Localite'], inplace=True)
        
    def merge_data(self):
        self.data_compt = pd.merge(self.data_compt, self.ref_gares, left_on="CodeTT0020Localite_transf", right_on="TVS_transf", how='left')
        
    def classify_hour(self, date_str):
        if pd.isna(date_str) or date_str == 'nan':
            return "Donnée manquante"
        try:
            date_obj = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S.%fZ")
        except ValueError:
            return "Format incorrect"
        hour = date_obj.hour
        return "heure de pointe" if (6 <= hour < 10) or (16 <= hour < 19) else "pas heure de pointe"
    
    def process_heure_pointe(self):
        ref_dt_passage = "pointJalonnement_pointHoraire_horaireObserve_Arrivee_realise"
        self.data_compt = self.data_compt[~self.data_compt[ref_dt_passage].isnull()]
        self.data_compt['heure_pointe'] = [self.classify_hour(str(x)) for x in self.data_compt[ref_dt_passage]]
        
    def aggregate_data(self):
        agg = self.data_compt.copy(deep=True)
        agg['Date'] = pd.to_datetime(agg['Date'], format="%d/%m/%Y")
        agg['Weekday'] = agg['Date'].dt.strftime('%A')
        agg = agg.sort_values(["Date", "NumeroMarche_realise", "pointJalonnement_rang_realise"])
        agg['Code UIC_shift'] = agg.groupby(["Date", "NumeroMarche_realise"])['Code UIC'].shift(-1)
        agg_filtered = agg[agg['Code UIC_shift'].notna()][["Libelle_PR" ,"Code UIC", "Code UIC_shift", 'Tx_occup_places_tot', "Weekday", 'heure_pointe', 'Latitude', 'Longitude']]
        self.res = agg_filtered.groupby(["Libelle_PR", 'Code UIC', 'Code UIC_shift', 'Weekday', 'heure_pointe', 'Latitude', 'Longitude'])['Tx_occup_places_tot'].mean().reset_index()
        
    def save_data(self):
        for h__i in self.res['heure_pointe'].unique():
            self.res[self.res['heure_pointe'] == h__i].to_csv(f'data_byWeekday_{h__i}.csv', sep=',', encoding='utf-8-sig', index=False)
            
    def process(self):
        self.transform_ref_gares()
        self.transform_data_compt()
        self.merge_data()
        self.process_heure_pointe()
        self.aggregate_data()
        self.save_data()
        

# Utilisation de la classe
processor = DataProcessor(os.path.join("Donnees_sncf", 'PDT&Comptage_2022_Hackaton_IdFM_280923.csv'), "referentiel-gares-voyageurs.csv")
processor.process()
