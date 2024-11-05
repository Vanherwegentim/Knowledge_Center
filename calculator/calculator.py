from .calculations import (
        bereken_afschrijvingen,
        bereken_balanstotaal,
        bereken_bruto_marge,
        bereken_dso,
        bereken_EBIT,
        bereken_EBITDA,
        bereken_EBITDA_marge,
        bereken_eigen_vermogen,
        bereken_financiele_schulden,
        bereken_handelsvorderingen,
        bereken_handelswerkkapitaal,
        bereken_liquide_middelen,
        bereken_netto_financiele_schuld,
        bereken_omzet,
        bereken_VERLIES,
        bereken_voorzieningen,
)

calculations = {
    'EBITDA': bereken_EBITDA,
    'verlies': bereken_VERLIES,
    
    'balanstotaal': bereken_balanstotaal,
    'eigen vermogen': bereken_eigen_vermogen,
    'voorzieningen': bereken_voorzieningen,
    'handelswerkkaptiaal': bereken_handelswerkkapitaal,
    'financiele schulden': bereken_financiele_schulden,
    'liquide middelen': bereken_liquide_middelen,
    'bruto marge': bereken_bruto_marge,
    'omzet': bereken_omzet,
    'EBITDA marge': bereken_EBITDA_marge,
    'afschrijvingen': bereken_afschrijvingen,
    'EBIT': bereken_EBIT,
    'Netto financiele schuld': bereken_netto_financiele_schuld,
    'handelsvorderingen': bereken_handelsvorderingen,
    'DSO': bereken_dso,
}


def bereken(what:str, company_id:int, date:str):
        '''
        Maakt een specifieke berekening van een bedrijf voor een bepaalde periode
        

        Vereiste:
            - what (str): Het soort berekening dat gemaakt moet worden. Map indien mogelijk naar een van volgende woorden (EBITDA, verlies, balanstotaal, eigen vermogenm voorzieningen, 
            handelswerkkapitaal, financiele schulden, liquide middelen, bruto marge, omzet, EBITDA marge, afschrijvingen, netto financiele schuld, handelsvorderingen, dso)
            - De company_id van het bedrijf, als je deze niet hebt maar wel de company naam, haal de id dan eerst uit de companies_ids_api_call tool. VRAAG NIET ACHTER DE ID.
            - date (str): eind datum van de gezochte periode in YYYY-MM-DD formaat

        Retourneert:
            - De uitkomst van de berekening
        
        '''
        
        #TODO: Match potential synonyms/typos to keys in calculations. (use cosine similarity or LLM)
        #Alternative: custom workflow with retry
        
        if what in calculations:
            return calculations[what](company_id, date)
        
        return f"""'Cannot perform the calculation for {what}. Currently only the following calculations are supported: {list(calculations.keys())}'"""