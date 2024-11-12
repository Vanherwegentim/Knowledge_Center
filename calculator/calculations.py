from django.utils import timezone

from enums.account_type import AccountType
from utils import (
        get_acount_details_by_account_number,
        get_db_connection,
        get_period_ids,
)


def bereken_EBITDA(company_id:int, date:str):
        '''
        Deze tool berekent de EBITDA van een bedrijf

        Vereiste:
            - date (str): eind datum van de gezochte periode in YYYY-MM-DD formaat
            - De company_id van het bedrijf

        Retourneert:
            - De EBITDA
        
        Details:
        EBITDA, short for earnings before interest, taxes, depreciation, and amortization, is an alternate measure of profitability to net income. 
        It's used to assess a company's profitability and financial performance.
        '''
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                period_id = get_period_ids(cursor, company_id, date)
                if isinstance(period_id, str):  # If the result is the error message
                    return period_id
                sql = f"""SELECT *
                    FROM account_details
                    WHERE 
                        company_id = {company_id} AND
                        account_number SIMILAR TO '60%|61%|62%|64%|70%|71%|72%|73%|74%' AND
                        period_id = {period_id};
                    """
                cursor.execute(sql)
                records = cursor.fetchall()
                gain = sum([float(record[10]) for record in records])
                result = gain * -1
                return result

def bereken_OMZET(company_id:int, date:str):
    '''
    Deze tool berekent de OMZET van een bedrijf

    Vereiste:
        - date (str): eind datum van de gezochte periode in YYYY-MM-DD formaat
        - De company_id van het bedrijf

    Retourneert:
        - De OMZET
    
    Details:
    De omzet van uw bedrijf is het totale bedrag aan inkomsten uit de verkoop van producten en diensten in een bepaalde periode. Dit wordt ook wel de bruto-omzet genoemd.
    '''
    date = timezone.datetime.fromisoformat(date)
    
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            period_id = get_period_ids(cursor,company_id,date)
            if isinstance(period_id, str):  # If the result is the error message
                return period_id
            sql = f"""SELECT *
                FROM account_details
                WHERE 
                    company_id = {company_id} AND
                    account_number SIMILAR TO '70%' AND
                    period_id = {period_id};
                """
            cursor.execute(sql)
            records = cursor.fetchall()
            gain = sum([float(record[10]) for record in records])
            result = gain * -1
            return result
        
def bereken_VERLIES(company_id:int, date:str):
    '''
    Deze tool berekent het VERLIES van een bedrijf

    Vereiste:
        - date (str): eind datum van de gezochte periode in YYYY-MM-DD formaat
        - De company_id van het bedrijf

    Retourneert:
        - het VERLIES
    
    Details:
        Wanneer de totale inkomsten lager liggen als de totale uitgaven, dan spreekt men van verlies.    '''

    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            period_id = get_period_ids(cursor,company_id,date)
            if isinstance(period_id, str):  # If the result is the error message
                    return period_id
            sql = f"""SELECT *
                FROM account_details
                WHERE 
                    company_id = {company_id} AND
                    account_number SIMILAR TO '60%|61%|62%|63%|64%|65%|66%|67%|68%|70%|71%|72%|73%|74%|75%|76%|77%|78%' AND
                    period_id = {period_id};
                """
            cursor.execute(sql)
            records = cursor.fetchall()
            gain = sum([float(record[10]) for record in records])
            result = gain * -1
            return result

def bereken_balanstotaal(company_id:int, date:str):
        '''
        Berekent het balanstotaal van een bedrijf

        Vereiste:
            - De company_id van het bedrijf
            - date (str): eind datum van de gezochte periode in YYYY-MM-DD formaat

        Retourneert:
            - Het balanstotaal
        
        Details:
        Balanstotaal is het totaal van alle schulden en bezittingen, passiva en activa van een onderneming
        '''
        
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                period_id = get_period_ids(cursor,company_id,date)
                if isinstance(period_id, str):  # If the result is the error message
                    return period_id
                sql = f"""SELECT value
                    FROM account_details
                    WHERE 
                        company_id = {company_id} AND
                        account_type = '{AccountType.ASSET}' AND
                        period_id = {period_id};
                    """
                cursor.execute(sql)
                records = cursor.fetchall()
                result = sum([float(record[0]) for record in records])
                return result
            
def bereken_eigen_vermogen(company_id:int, date:str):
    '''
    Berekent het eigen vermogen van een bedrijf

    Vereiste:
        - De company_id van het bedrijf
        - date (str): eind datum van de gezochte periode in YYYY-MM-DD formaat

    Retourneert:
        - Het eigen vermogen
    
    Details:
    Het eigen vermogen is het saldo van de bezittingen ('activa') en schulden ('passiva') van een onderneming of organisatie
    '''
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            period_id = get_period_ids(cursor,company_id,date)
            records = get_acount_details_by_account_number(cursor, company_id, period_id, [10,11,12,13,14,15])
            result = sum([float(record[0]) for record in records])
            return result
        
def bereken_voorzieningen(company_id:int, date:str):
    '''
    Berekent de voorzieningen van een bedrijf

    Vereiste:
        - De company_id van het bedrijf
        - date (str): eind datum van de gezochte periode in YYYY-MM-DD formaat

    Retourneert:
        - de voorzieningen
    
    Details:
    De voorzieningen 
    '''
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            period_id = get_period_ids(cursor,company_id,date)
            if isinstance(period_id, str):  # If the result is the error message
                    return period_id
            additives = get_acount_details_by_account_number(cursor, company_id, period_id, [16])
            
            result = sum([float(record[0]) for record in additives])
            return result

def bereken_handelswerkkapitaal(company_id:int, date:str):
    '''
    Berekent het handelswerkkapitaal van een bedrijf

    Vereiste:
        - De company_id van het bedrijf
        - date (str): eind datum van de gezochte periode in YYYY-MM-DD formaat

    Retourneert:
        - Het handelswerkkapitaal
    
    Details:
    Het handelswerkkapitaal omvat de balansposten die nodig zijn voor de bedrijfsvoering, zoals debiteuren en crediteuren (en ook voorraden)
    '''
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            period_id = get_period_ids(cursor,company_id,date)
            if isinstance(period_id, str):  # If the result is the error message
                    return period_id
            additives = get_acount_details_by_account_number(cursor, company_id, period_id, [30, 31, 32, 33, 34, 35, 36, 37, 40])
            negatives = get_acount_details_by_account_number(cursor, company_id, period_id, [44])
            
            result = sum([float(record[0]) for record in additives]) - sum([float(record[0]) for record in negatives])
            return result
        
def bereken_financiele_schulden(company_id:int, date:str):
    '''
    Berekent de financiele schulden van een bedrijf

    Vereiste:
        - De company_id van het bedrijf
        - date (str): eind datum van de gezochte periode in YYYY-MM-DD formaat

    Retourneert:
        - de financiele schulden
    
    Details:
    De financiele schulden zijn een onderverdeling bij de schulden op meer dan één jaar.
    '''
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            period_id = get_period_ids(cursor,company_id,date)
            if isinstance(period_id, str):  # If the result is the error message
                    return period_id
            additives = get_acount_details_by_account_number(cursor, company_id, period_id, [16, 17, 42, 43])
            
            result = sum([float(record[0]) for record in additives])
            return result
        
def bereken_liquide_middelen(company_id:int, date:str):
    '''
    Berekent de financiele schulden van een bedrijf

    Vereiste:
        - De company_id van het bedrijf
        - date (str): eind datum van de gezochte periode in YYYY-MM-DD formaat

    Retourneert:
        - de financiele schulden
    
    Details:
    De financiele schulden zijn een onderverdeling bij de schulden op meer dan één jaar.
    '''
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            period_id = get_period_ids(cursor,company_id,date)
            if isinstance(period_id, str):  # If the result is the error message
                    return period_id
            additives = get_acount_details_by_account_number(cursor, company_id, period_id, [50, 51, 52, 53 ,54, 55, 56, 57, 58])
            
            result = sum([float(record[0]) for record in additives])
            return result

def bereken_bruto_marge(company_id:int, date:str):
    '''
    Berekent de bruto marge van een bedrijf

    Vereiste:
        - De company_id van het bedrijf
        - date (str): eind datum van de gezochte periode in YYYY-MM-DD formaat

    Retourneert:
        - Het bruto marge
    
    Details:
    De bruto marge is een verhouding die meet hoe winstgevend uw bedrijf is
    '''
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            period_id = get_period_ids(cursor,company_id,date)
            if isinstance(period_id, str):  # If the result is the error message
                    return period_id
            additives = get_acount_details_by_account_number(cursor, company_id, period_id, [70, 71, 72, 74])
            negatives = get_acount_details_by_account_number(cursor, company_id, period_id, [60])
            
            result = sum([float(record[0]) for record in additives]) - sum([float(record[0]) for record in negatives])
            return result

def bereken_omzet(company_id:int, date:str):
    '''
    Berekent de omzet van een bedrijf

    Vereiste:
        - De company_id van het bedrijf
        - date (str): eind datum van de gezochte periode in YYYY-MM-DD formaat

    Retourneert:
        - Het omzet
    
    Details:
    Het omzet is het totaalbedrag in een bepaalde periode van de verkopen door een bedrijf
    '''
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            period_id = get_period_ids(cursor,company_id,date)
            if isinstance(period_id, str):  # If the result is the error message
                    return period_id
            additives = get_acount_details_by_account_number(cursor, company_id, period_id, [70])
                        
            result = sum([float(record[0]) for record in additives])
            return result

def bereken_EBITDA_marge(company_id:int, date:str):
    '''
    Berekent de EBITDA marge van een bedrijf

    Vereiste:
        - De company_id van het bedrijf
        - date (str): eind datum van de gezochte periode in YYYY-MM-DD formaat

    Retourneert:
        - De EBITDA marge
    
    Details:
    De EBITDA marge geeft aan hoeveel cash een bedrijf genereert voor elke euro omzet.
    '''
    ebitda = bereken_EBITDA(company_id, date)
    omzet = bereken_omzet(company_id, date)
    return ebitda / omzet

def bereken_afschrijvingen(company_id:int, date:str):
    '''
    Berekent de afschrijvingen van een bedrijf

    Vereiste:
        - De company_id van het bedrijf
        - date (str): eind datum van de gezochte periode in YYYY-MM-DD formaat

    Retourneert:
        - De afschrijvingen
    
    Details:
    De afschrijvingen
    '''
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            period_id = get_period_ids(cursor,company_id,date)
            if isinstance(period_id, str):  # If the result is the error message
                    return period_id
            additives = get_acount_details_by_account_number(cursor, company_id, period_id, [63])
                        
            result = sum([float(record[0]) for record in additives])
            return result
    
def bereken_EBIT(company_id:int, date:str):
    '''
    Berekent de EBITDA marge van een bedrijf

    Vereiste:
        - De company_id van het bedrijf
        - date (str): eind datum van de gezochte periode in YYYY-MM-DD formaat

    Retourneert:
        - De EBITDA marge
    
    Details:
    De EBITDA marge geeft aan hoeveel cash een bedrijf genereert voor elke euro omzet.
    '''
    ebitda = bereken_EBITDA(company_id, date)
    afschrijvingen = bereken_afschrijvingen(company_id, date)
    return ebitda + afschrijvingen

def bereken_netto_financiele_schuld(company_id:int, date:str):
    '''
    Berekent de netto financiele schuld van een bedrijf

    Vereiste:
        - De company_id van het bedrijf
        - date (str): eind datum van de gezochte periode in YYYY-MM-DD formaat

    Retourneert:
        - De netto financiele schuld
    
    Details:
    De netto financiele schuld geeft het vermogen van de groep weer om de schulden terug te betalen op basis van de kasstromen gegenereerd door de bedrijfsactiviteiten
    '''
    schulden = bereken_financiele_schulden(company_id, date)
    liquide = bereken_liquide_middelen(company_id, date)
    return schulden - liquide
    

def bereken_handelsvorderingen(company_id:int, date:str):
    '''
    Berekent het handelsvorderingen van een bedrijf

    Vereiste:
        - De company_id van het bedrijf
        - date (str): eind datum van de gezochte periode in YYYY-MM-DD formaat

    Retourneert:
        - Het handelsvorderingen
    
    Details:
    Het handelsvorderingen zijn een boekhoudkundige rekening met alle uitstaande geldclaims die betrekking hebben op verkopen waarvan de betaling nog niet geïnd is
    '''
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            period_id = get_period_ids(cursor,company_id,date)
            if isinstance(period_id, str):  # If the result is the error message
                    return period_id
            additives = get_acount_details_by_account_number(cursor, company_id, period_id, [40])
            
            result = sum([float(record[0]) for record in additives])
            return result
        
def bereken_dso(company_id:int, date:str):
    '''
    Berekent de Days Sales Outstanding (DSO) van een bedrijf

    Vereiste:
        - De company_id van het bedrijf
        - date (str): eind datum van de gezochte periode in YYYY-MM-DD formaat

    Retourneert:
        - Het handelsvorderingen
    
    Details:
    De DSO geeft aan hoeveel dagen het gemiddeld duurt voordat een factuur betaald is nadat jouw bedrijf een product of dienst heeft geleverd
    '''            
    handelsvorderingen = bereken_handelsvorderingen(company_id, date)
    omzet = bereken_omzet(company_id, date)
    if isinstance(handelsvorderingen, str):  # If the result is the error message
        return handelsvorderingen
    if isinstance(omzet,str):
        return omzet
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            date = timezone.datetime.fromisoformat(date)
            period_ids_query = f"""
            SELECT end_date, fiscal_year_start
            FROM periods
            WHERE company_id = {company_id}
            AND (
                end_date = fiscal_year_end AND DATE_PART('year', fiscal_year_end) = {date.year}
                OR
                end_date = (
                    SELECT MAX(end_date)
                    FROM periods AS p2
                    WHERE p2.company_id = periods.company_id
                    AND DATE_PART('year', p2.end_date) = DATE_PART('year', periods.end_date)
                    AND DATE_PART('year', fiscal_year_end) = {date.year}
                )
            );
            """
            cursor.execute(period_ids_query)
            dates = cursor.fetchall()
            date_1 = timezone.datetime.strptime(str(dates[0][0]), '%Y-%m-%d').date()
            date_2 = timezone.datetime.strptime(str(dates[0][1]), '%Y-%m-%d').date()
            days = date_1 - date_2
    if omzet == 0:
        return "De omzet is nul dus kan dit niet berekend worden"
    return (handelsvorderingen / omzet) * days * -1
