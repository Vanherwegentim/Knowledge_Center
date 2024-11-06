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
from utils import get_db_connection

from django.utils import timezone

calculations = {
    'EBITDA': bereken_EBITDA,
    'verlies': bereken_VERLIES,
    
    'balanstotaal': bereken_balanstotaal,
    'eigen vermogen': bereken_eigen_vermogen,
    'voorzieningen': bereken_voorzieningen,
    'handelswerkkapitaal': bereken_handelswerkkapitaal,
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
        als je de company_id niet hebt maar wel de company naam, haal de id dan eerst uit de companies_ids_api_call tool. VRAAG NIET ACHTER DE ID.
        

        Vereiste:
            - what (str): Het soort berekening dat gemaakt moet worden. Map indien mogelijk naar een van volgende woorden (EBITDA, verlies, balanstotaal, eigen vermogenm voorzieningen, 
            handelswerkkapitaal, financiele schulden, liquide middelen, bruto marge, omzet, EBITDA marge, afschrijvingen, netto financiele schuld, handelsvorderingen, DSO)
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

def vergelijk_op_basis_van(what:str, date:str, limit:int=10, order_by:str="DESC"):
    """
    Geeft de gevraagde hoeveelheid bedrijven terug gesorteerd op ASC or DESC voor een bepaalde periode

    Vereiste:
        - what (str): Het soort berekening dat gemaakt moet worden. Map indien mogelijk naar een van volgende woorden (EBITDA, verlies, balanstotaal, eigen vermogen, voorzieningen, 
            handelswerkkapitaal, financiele schulden, liquide middelen, bruto marge, omzet, EBITDA marge, afschrijvingen, netto financiele schuld, handelsvorderingen, dso)

    """
    date = timezone.datetime.fromisoformat(date)
    if limit > 100:
          return "Dit is een te groot aantal bedrijven. Kies aub een kleinere hoeveelheid."
    match what:
          case "EBITDA":
                sql = f"""WITH latest_period AS (
                            SELECT period_id, company_id,
                                            ROW_NUMBER() OVER (
                                                PARTITION BY company_id 
                                                ORDER BY 
                                                    (CASE WHEN end_date = fiscal_year_end AND DATE_PART('year', fiscal_year_end) = {date.year} THEN 1 ELSE 2 END),
                                                    end_date DESC
                                            ) AS rn
                                        FROM periods
                                        WHERE DATE_PART('year', end_date) = {date.year}
                                    )

                                    SELECT c.company_id, c.name, SUM(ad.value) AS total_value
                                    FROM companies c
                                    JOIN account_details ad ON c.company_id = ad.company_id
                                    JOIN periods p ON ad.period_id = p.period_id
                                    JOIN latest_period lp ON lp.company_id = c.company_id AND ad.period_id = lp.period_id AND lp.rn = 1
                                    WHERE ad.account_number SIMILAR TO '60%|61%|62%|64%|70%|71%|72%|73%|74%'
                                    GROUP BY c.company_id, c.name
                                    ORDER BY total_value {order_by}
                                    LIMIT {limit};
                                    """
          case "verlies":
                sql = f"""WITH latest_period AS (
                            SELECT period_id, company_id,
                                ROW_NUMBER() OVER (
                                    PARTITION BY company_id 
                                    ORDER BY 
                                        (CASE WHEN end_date = fiscal_year_end AND DATE_PART('year', fiscal_year_end) = {date.year} THEN 1 ELSE 2 END),
                                        end_date DESC
                                ) AS rn
                            FROM periods
                            WHERE DATE_PART('year', end_date) = {date.year}
                        )

                        SELECT c.company_id, c.name, SUM(ad.value) AS total_value
                        FROM companies c
                        JOIN account_details ad ON c.company_id = ad.company_id
                        JOIN periods p ON ad.period_id = p.period_id
                        JOIN latest_period lp ON lp.company_id = c.company_id AND ad.period_id = lp.period_id AND lp.rn = 1
                        WHERE ad.account_number SIMILAR TO '60%|61%|62%|63%|64%|65%|66%|67%|68%|70%|71%|72%|73%|74%|75%|76%|77%|78%'
                        GROUP BY c.company_id, c.name
                        ORDER BY total_value {order_by}
                        LIMIT {limit};
                        """
          case "balanstotaal":
                sql = f"""WITH latest_period AS (
                            SELECT period_id, company_id,
                                ROW_NUMBER() OVER (
                                    PARTITION BY company_id 
                                    ORDER BY 
                                        (CASE WHEN end_date = fiscal_year_end AND DATE_PART('year', fiscal_year_end) = {date.year} THEN 1 ELSE 2 END),
                                        end_date DESC
                                ) AS rn
                            FROM periods
                            WHERE DATE_PART('year', end_date) = {date.year}
                        )

                        SELECT c.company_id, c.name, SUM(ad.value) AS total_value
                        FROM companies c
                        JOIN account_details ad ON c.company_id = ad.company_id
                        JOIN periods p ON ad.period_id = p.period_id
                        JOIN latest_period lp ON lp.company_id = c.company_id AND ad.period_id = lp.period_id AND lp.rn = 1
                        WHERE ad.account_number SIMILAR TO '60%'
                        GROUP BY c.company_id, c.name
                        ORDER BY total_value {order_by}
                        LIMIT {limit};
                        """
          case "eigen vermogen":
                sql = "SIMILAR TO '16%'"
          case "voorziening":
                sql = f"""WITH latest_period AS (
                            SELECT period_id, company_id,
                                ROW_NUMBER() OVER (
                                    PARTITION BY company_id 
                                    ORDER BY 
                                        (CASE WHEN end_date = fiscal_year_end AND DATE_PART('year', fiscal_year_end) = {date.year} THEN 1 ELSE 2 END),
                                        end_date DESC
                                ) AS rn
                            FROM periods
                            WHERE DATE_PART('year', end_date) = {date.year}
                        )

                        SELECT c.company_id, c.name, SUM(ad.value) AS total_value
                        FROM companies c
                        JOIN account_details ad ON c.company_id = ad.company_id
                        JOIN periods p ON ad.period_id = p.period_id
                        JOIN latest_period lp ON lp.company_id = c.company_id AND ad.period_id = lp.period_id AND lp.rn = 1
                        WHERE ad.account_number SIMILAR TO '16%'
                        GROUP BY c.company_id, c.name
                        ORDER BY total_value {order_by}
                        LIMIT {limit};
                        """
          case "handelswerkkapitaal":
                sql = f"""WITH latest_period AS (
                            SELECT period_id, company_id,
                                ROW_NUMBER() OVER (
                                    PARTITION BY company_id 
                                    ORDER BY 
                                        (CASE WHEN end_date = fiscal_year_end AND DATE_PART('year', fiscal_year_end) = {date.year} THEN 1 ELSE 2 END),
                                        end_date DESC
                                ) AS rn
                            FROM periods
                            WHERE DATE_PART('year', end_date) = {date.year}
                        )

                        SELECT 
                            c.company_id, 
                            c.name, 
                            SUM(CASE WHEN ad.account_number SIMILAR TO '30%|31%|32%|33%|34%|35%|36%|37%|40' 
                                    THEN ad.value 
                                    ELSE 0 
                                END) 
                            - SUM(CASE WHEN ad.account_number LIKE '44%' 
                                    THEN ad.value 
                                    ELSE 0 
                                END) AS total_value
                        FROM companies c
                        JOIN account_details ad ON c.company_id = ad.company_id
                        JOIN periods p ON ad.period_id = p.period_id
                        JOIN latest_period lp ON lp.company_id = c.company_id AND ad.period_id = lp.period_id AND lp.rn = 1
                        GROUP BY c.company_id, c.name
                        ORDER BY total_value {order_by}
                        LIMIT {limit};
                        """
          case "financiele schulden":
                sql = f"""WITH latest_period AS (
                            SELECT period_id, company_id,
                                ROW_NUMBER() OVER (
                                    PARTITION BY company_id 
                                    ORDER BY 
                                        (CASE WHEN end_date = fiscal_year_end AND DATE_PART('year', fiscal_year_end) = {date.year} THEN 1 ELSE 2 END),
                                        end_date DESC
                                ) AS rn
                            FROM periods
                            WHERE DATE_PART('year', end_date) = {date.year}
                        )

                        SELECT c.company_id, c.name, SUM(ad.value) AS total_value
                        FROM companies c
                        JOIN account_details ad ON c.company_id = ad.company_id
                        JOIN periods p ON ad.period_id = p.period_id
                        JOIN latest_period lp ON lp.company_id = c.company_id AND ad.period_id = lp.period_id AND lp.rn = 1
                        WHERE ad.account_number SIMILAR TO '16%|17%|42%|43%'
                        GROUP BY c.company_id, c.name
                        ORDER BY total_value {order_by}
                        LIMIT {limit};
                        """
          case "liquide middelen":
                sql = f"""WITH latest_period AS (
                            SELECT period_id, company_id,
                                ROW_NUMBER() OVER (
                                    PARTITION BY company_id 
                                    ORDER BY 
                                        (CASE WHEN end_date = fiscal_year_end AND DATE_PART('year', fiscal_year_end) = {date.year} THEN 1 ELSE 2 END),
                                        end_date DESC
                                ) AS rn
                            FROM periods
                            WHERE DATE_PART('year', end_date) = {date.year}
                        )

                        SELECT c.company_id, c.name, SUM(ad.value) AS total_value
                        FROM companies c
                        JOIN account_details ad ON c.company_id = ad.company_id
                        JOIN periods p ON ad.period_id = p.period_id
                        JOIN latest_period lp ON lp.company_id = c.company_id AND ad.period_id = lp.period_id AND lp.rn = 1
                        WHERE ad.account_number SIMILAR TO '50%|51%|52%|53%|54%|55%|56%|57%|58%'
                        GROUP BY c.company_id, c.name
                        ORDER BY total_value {order_by}
                        LIMIT {limit};
                        """
          case "bruto marge":
                sql = f"""WITH latest_period AS (
                            SELECT period_id, company_id,
                                ROW_NUMBER() OVER (
                                    PARTITION BY company_id 
                                    ORDER BY 
                                        (CASE WHEN end_date = fiscal_year_end AND DATE_PART('year', fiscal_year_end) = {date.year} THEN 1 ELSE 2 END),
                                        end_date DESC
                                ) AS rn
                            FROM periods
                            WHERE DATE_PART('year', end_date) = {date.year}
                        )

                        SELECT 
                            c.company_id, 
                            c.name, 
                            SUM(CASE WHEN ad.account_number SIMILAR TO '70%|71%|72%|74%' 
                                    THEN ad.value 
                                    ELSE 0 
                                END) 
                            - SUM(CASE WHEN ad.account_number LIKE '60%' 
                                    THEN ad.value 
                                    ELSE 0 
                                END) AS total_value
                        FROM companies c
                        JOIN account_details ad ON c.company_id = ad.company_id
                        JOIN periods p ON ad.period_id = p.period_id
                        JOIN latest_period lp ON lp.company_id = c.company_id AND ad.period_id = lp.period_id AND lp.rn = 1
                        GROUP BY c.company_id, c.name
                        ORDER BY total_value {order_by}
                        LIMIT {limit};
                        """
          case "omzet":
                sql = f"""WITH latest_period AS (
                            SELECT period_id, company_id,
                                ROW_NUMBER() OVER (
                                    PARTITION BY company_id 
                                    ORDER BY 
                                        (CASE WHEN end_date = fiscal_year_end AND DATE_PART('year', fiscal_year_end) = {date.year} THEN 1 ELSE 2 END),
                                        end_date DESC
                                ) AS rn
                            FROM periods
                            WHERE DATE_PART('year', end_date) = {date.year}
                        )

                        SELECT c.company_id, c.name, SUM(ad.value) AS total_value
                        FROM companies c
                        JOIN account_details ad ON c.company_id = ad.company_id
                        JOIN periods p ON ad.period_id = p.period_id
                        JOIN latest_period lp ON lp.company_id = c.company_id AND ad.period_id = lp.period_id AND lp.rn = 1
                        WHERE ad.account_number SIMILAR TO '70%'
                        GROUP BY c.company_id, c.name
                        ORDER BY total_value {order_by}
                        LIMIT {limit};
                        """
          case "EBITDA marge":
                sql = f"""WITH latest_period AS (
                    SELECT period_id, company_id,
                        ROW_NUMBER() OVER (
                            PARTITION BY company_id 
                            ORDER BY 
                                (CASE WHEN end_date = fiscal_year_end AND DATE_PART('year', fiscal_year_end) = {date.year} THEN 1 ELSE 2 END),
                                end_date DESC
                        ) AS rn
                    FROM periods
                    WHERE DATE_PART('year', end_date) = {date.year}
                ),

                -- Bereken de EBITDA voor elk bedrijf
                ebitda AS (
                    SELECT c.company_id, c.name, SUM(ad.value) AS ebitda_value
                    FROM companies c
                    JOIN account_details ad ON c.company_id = ad.company_id
                    JOIN periods p ON ad.period_id = p.period_id
                    JOIN latest_period lp ON lp.company_id = c.company_id AND ad.period_id = lp.period_id AND lp.rn = 1
                    WHERE ad.account_number SIMILAR TO '60%|61%|62%|64%|70%|71%|72%|73%|74%'
                    GROUP BY c.company_id, c.name
                ),

                -- Bereken de omzet voor elk bedrijf
                marge AS (
                    SELECT c.company_id, SUM(ad.value) AS marge_value
                    FROM companies c
                    JOIN account_details ad ON c.company_id = ad.company_id
                    JOIN periods p ON ad.period_id = p.period_id
                    JOIN latest_period lp ON lp.company_id = c.company_id AND ad.period_id = lp.period_id AND lp.rn = 1
                    WHERE ad.account_number SIMILAR TO '70%'
                    GROUP BY c.company_id
                )

                -- Combineer EBITDA en marge om de EBITDA-marge te berekenen
                SELECT e.company_id, 
                    e.name, 
                    e.ebitda_value, 
                    m.marge_value, 
                    CASE 
                        WHEN m.marge_value <> 0 THEN e.ebitda_value / m.marge_value
                        ELSE NULL 
                    END AS ebitda_marge
                FROM ebitda e
                JOIN marge m ON e.company_id = m.company_id
                ORDER BY ebitda_marge {order_by}
                LIMIT {limit};
                """
          case "afschrijvingen":
                sql = f"""WITH latest_period AS (
                            SELECT period_id, company_id,
                                ROW_NUMBER() OVER (
                                    PARTITION BY company_id 
                                    ORDER BY 
                                        (CASE WHEN end_date = fiscal_year_end AND DATE_PART('year', fiscal_year_end) = {date.year} THEN 1 ELSE 2 END),
                                        end_date DESC
                                ) AS rn
                            FROM periods
                            WHERE DATE_PART('year', end_date) = {date.year}
                        )

                        SELECT c.company_id, c.name, SUM(ad.value) AS total_value
                        FROM companies c
                        JOIN account_details ad ON c.company_id = ad.company_id
                        JOIN periods p ON ad.period_id = p.period_id
                        JOIN latest_period lp ON lp.company_id = c.company_id AND ad.period_id = lp.period_id AND lp.rn = 1
                        WHERE ad.account_number SIMILAR TO '63%'
                        GROUP BY c.company_id, c.name
                        ORDER BY total_value {order_by}
                        LIMIT {limit};
                        """
          case "EBIT":
                return
          case "Netto financiele schuld":
                return
          case "handelsvorderingen":
                return
          case "DSO":
                return            
                
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            results = cursor.fetchall()
            return results
        
    
    

