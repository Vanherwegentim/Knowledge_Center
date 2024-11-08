import json

from django.utils import timezone
from llama_index.core.tools import FunctionTool

from enums.account_type import AccountType
from utils import (
    get_acount_details_by_account_number,
    get_db_connection,
    get_period_ids,
)

#cursor = conn.cursor()

def multiply(a: float, b: float) -> float:
    """Multiply two numbers and returns the product"""
    return a * b


def add(a: float, b: float) -> float:
    """Add two numbers and returns the sum"""
    return a + b

def account_api_call(company_id: str, page: int = 1, page_size: int = 100):
    """
    Haalt een specifieke pagina van accountdossiers op voor een bedrijf, geïdentificeerd door de company_id.

    Vereist:
    - company_id (str): Het unieke ID van het bedrijf waarvoor de accounts moeten worden opgehaald.
    - page (int): De pagina van resultaten die moet worden opgehaald (standaard is pagina 1).
    - page_size (int): Het aantal accountdossiers per pagina (standaard is 100).

    Retourneert:
    - Een lijst met accountdossiers voor de opgegeven pagina, of een foutmelding als het bedrijf niet bestaat of geen accounts heeft.
    """
    with open("silverfin_api_static_db/accounts.json", 'r') as file:
        accounts = json.load(file)
    
    # Get the accounts for the specified company
    company_accounts = accounts.get(str(company_id), "Geen accounts gevonden voor het opgegeven bedrijf.")
    
    if isinstance(company_accounts, str):  # If the result is the error message
        return company_accounts
    
    # Calculate the start and end indices for pagination
    start_index = (page - 1) * page_size
    end_index = start_index + page_size
    
    # Return the paginated results
    paginated_accounts = company_accounts[start_index:end_index]
    
    return paginated_accounts if paginated_accounts else "Geen meer accounts voor deze pagina."


def company_api_call(company_id: str):
    """
    Geeft informatie terug over het bedrijf met de gegeven ID
    Vereist:
    - company_id (int): Het unieke ID van het bedrijf waarvoor de gegevens moeten worden opgehaald.

    Retourneert:
    - Een dictionary met bedrijfsinformatie als het bedrijf wordt gevonden, of een foutmelding als het bedrijf niet bestaat.
    """
    with open("silverfin_api_static_db/companies.json", 'r') as file:
        companies = json.load(file)

    if companies[company_id]:
        return companies[company_id]
    return "Geen bedrijf gevonden met de opgegeven company_id."

def companies_ids_api_call(keywords: list = None):
    """
    Geeft de bedrijfs-ids met de overeenkomstige naam terug. Gebruik deze tool wanneer je de company_id niet weet.
    Vereist:
    - keywords: Een lijst met zoekwoorden om te filteren op bedrijfsnamen
    Retourneert:
    - Een lijst met de ids van alle bedrijven voor de opgegeven pagina, gefilterd op zoekwoorden.
    """
    companies = """
        SELECT company_id, name
        FROM companies
    """
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(companies)
            result = cursor.fetchall()

    # Filter results based on keywords
    if keywords:
        filtered_result = [
            (company_id, name) for company_id, name in result
            if any(keyword.lower() in name.lower() for keyword in keywords)
        ]
    else:
        filtered_result = result

    return filtered_result





def people_api_call(company_id: int, page: int = 1, page_size: int = 100):
    """
    Haalt een specifieke pagina van medewerkers op voor een bedrijf, geïdentificeerd door de company_id.

    Vereist:
    - company_id (int): Het unieke ID van het bedrijf waarvoor de medewerkers moeten worden opgehaald.
    - page (int): De pagina van resultaten die moet worden opgehaald (standaard is pagina 1).
    - page_size (int): Het aantal medewerkers per pagina (standaard is 100).

    Retourneert:
    - Een lijst met mensen die werken voor het opgegeven bedrijf voor de opgegeven pagina.
    """
    with open("silverfin_api_static_db/peoples.json", 'r') as file:
        peoples = json.load(file)

    # Get the employees for the specified company
    company_people = peoples.get(str(company_id), "Geen medewerkers gevonden voor het opgegeven bedrijf.")
    
    if isinstance(company_people, str):  # If the result is an error message
        return company_people

    # Calculate the start and end indices for pagination
    start_index = (page - 1) * page_size
    end_index = start_index + page_size
    
    # Return the paginated results
    paginated_people = company_people[start_index:end_index]
    
    return paginated_people if paginated_people else "Geen meer medewerkers voor deze pagina."


def period_api_call(company_id: int, page: int = 1, page_size: int = 100):
    """
    Haalt een specifieke pagina van periodes op voor een bedrijf, geïdentificeerd door de company_id.

    Vereist:
    - company_id (int): Het unieke ID van het bedrijf waarvoor de periodes moeten worden opgehaald.
    - page (int): De pagina van resultaten die moet worden opgehaald (standaard is pagina 1).
    - page_size (int): Het aantal periodes per pagina (standaard is 100).

    Retourneert:
    - Een lijst met periodes die horen bij het opgegeven bedrijf voor de opgegeven pagina.
    """
    with open("silverfin_api_static_db/periods.json", 'r') as file:
        periods = json.load(file)

    # Get the periods for the specified company
    company_periods = periods.get(str(company_id), "Geen periodes gevonden voor het opgegeven bedrijf.")
    
    if isinstance(company_periods, str):  # If the result is an error message
        return company_periods

    # Calculate the start and end indices for pagination
    start_index = (page - 1) * page_size
    end_index = start_index + page_size
    
    # Return the paginated results
    paginated_periods = company_periods[start_index:end_index]
    
    return paginated_periods if paginated_periods else "Geen meer periodes voor deze pagina."

def company_id_to_name_converter(company_id:int):
    with open("silverfin_api_static_db/company_ids.json", 'r') as file:
        companies = json.load(file)
        return companies[str(company_id)]
     
def has_tax_decreased_api_call(company_id:int, date: str):
    """
    Geeft de tax percentage van een bedrijf terug voor een bepaalde reconciliation. Als het tax percentage 20 is dan geniet het een verlaagd tarief, indien het 25 is dan geniet het bedrijf geen verlaagd tarief.
    Vereist:
    - date (str): eind datum van de gezochte periode in YYYY-MM-DD formaat
    - company (int): De id van het bedrijf.

    Retourneert:
        Het tax percentage van het bedrijf
    """

    sql = f"""SELECT tax_percentage
                from reconciliation_results
                where company_id = {company_id}"""
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            tax_percentage = cursor.fetchall()

    return tax_percentage
                

def period_id_fetcher(date:str, company_id:int):
    '''
    Geeft een lijst van perioden terug die eindigen op een bepaalde datum

    Vereist:
    - date (str): eind datum van de gezochte periode in YYYY-MM-DD formaat
    - company_id (int): id van het bedrijf waarin naar periodes gezocht word

    Retourneert:
    - Een lijst met periode ids die eindigen op de vooropgestelde datum
    '''
    date = timezone.datetime.fromisoformat(date)
    period_ids = f"""
        SELECT period_id
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
    
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(period_ids)
            period_ids = cursor.fetchall()
    if len(period_ids) == 0:
        return "Dit bedrijf heeft geen periode tijdens deze datum"
    return period_ids


# def account_on_company_id_fetcher(company_id:int):
#     sql = f"""SELECT *
#              FROM accounts
#              WHERE accounts.company_id = {company_id}"""
#     cursor.execute(sql)
#     record = cursor.fetchall()

#     return record
# account_tool = FunctionTool.from_defaults(fn=account_on_company_id_fetcher)

def account_details(company_id:int=0, period_id:int=0, account_id:int=0):
    """
    NIET voor berekeningen zoals EBITDA, eigen vermogen, en dergerlijke.
    Geeft een lijst van account_details terug. Account_details hangen af van de company_id, period_id en de account_id. Een account_details kunnen dezelfde account_id hebben omdat ze dan verschillen van periode.
    
    Vereist:
        - Een company_id of een period_id of account_id. Afhankelijk van wat er gegeven is wordt er een lijst teruggegeven. Vul maar 1 van de ids in en laten de andere defaulten naar 0 afhankelijk van wat de vraag is.
    
    Retourneert:
        - company_id, period_id, account_id, account_name, account_number, 
        number_without_suffix, original_name, original_number, account_type, 
        reconciliation_template_id, value, starred
    
    Details:
        - Value is de waarde van de account. Als er dus gevraagd wordt achter winst of verlies gaat dit belangrijk zijn.
    """
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            if company_id!=0:
                sql = f"""SELECT *
                    FROM account_details
                    WHERE account_details.company_id = {company_id}"""
                cursor.execute(sql)
                record = cursor.fetchall()

                return record
            
            elif period_id!=0:
                sql = f"""SELECT *
                    FROM account_details
                    WHERE account_details.period_id = {period_id}"""
                cursor.execute(sql)
                record = cursor.fetchall()

                return record
            
            elif account_id!=0:
                sql = f"""SELECT *
                    FROM account_details
                    WHERE account_details.account_id = {account_id}"""
                cursor.execute(sql)
                record = cursor.fetchall()

                return record

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
        date = timezone.datetime.fromisoformat(date)
        period_ids = f"""
            SELECT period_id
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
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(period_ids)
                period_ids = cursor.fetchall()
                if len(period_ids) == 0:
                    return "Dit bedrijf heeft geen periode tijdens deze datum"
                period_id = period_ids[0][0]
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
    period_ids = f"""
        SELECT period_id
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
    
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(period_ids)
            period_ids = cursor.fetchall()
            if len(period_ids) == 0:
                return "Dit bedrijf heeft geen periode tijdens deze datum"
            period_id = period_ids[0][0]
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
        
bereken_OMZET_tool = FunctionTool.from_defaults(fn=bereken_OMZET)

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
    date = timezone.datetime.fromisoformat(date)
    period_ids = f"""
        SELECT period_id
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
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(period_ids)
            period_ids = cursor.fetchall()
            if len(period_ids) == 0:
                return "Dit bedrijf heeft geen periode tijdens deze datum"
            period_id = period_ids[0][0]
            sql = f"""SELECT *
                FROM account_details
                WHERE 
                    company_id = {company_id} AND
                    account_number SIMILAR TO '60%|61%|62%|63%|64%|65%|66%|67%|68%|70%|71%|72%|73%|74%|75%|76%|77%|78%%' AND
                    period_id = {period_id};
                """
            cursor.execute(sql)
            records = cursor.fetchall()
            gain = sum([float(record[10]) for record in records])
            result = gain * -1
            return result

bereken_VERLIES_tool = FunctionTool.from_defaults(fn=bereken_VERLIES)

def bereken_balanstotaal(company_id:int, date:str):
        '''
        Berekent het balanstotaal van een bedrijf

        Vereiste:
            - De company_id van het bedrijf
            - date (str): eind datum van de gezochte periode in YYYY-MM-DD formaat

        Retourneert:
            - Het eigen vermogen
        
        Details:
        Balanstotaal is het totaal van alle schulden en bezittingen, passiva en activa van een onderneming
        '''
        
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                period_ids = get_period_ids(cursor, company_id, date)
                
                if len(period_ids) == 0:
                    return "Dit bedrijf heeft geen periode tijdens deze datum"
                period_id = period_ids[0][0]
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
            period_ids = get_period_ids(cursor, company_id, date)
            if len(period_ids) == 0:
                return "Dit bedrijf heeft geen periode tijdens deze datum"
            period_id = period_ids[0][0]
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
            period_ids = get_period_ids(cursor, company_id, date)
            if len(period_ids) == 0:
                return "Dit bedrijf heeft geen periode tijdens deze datum"
            period_id = period_ids[0][0]
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
            period_ids = get_period_ids(cursor, company_id, date)
            if len(period_ids) == 0:
                return "Dit bedrijf heeft geen periode tijdens deze datum"
            period_id = period_ids[0][0]
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
            period_ids = get_period_ids(cursor, company_id, date)
            if len(period_ids) == 0:
                return "Dit bedrijf heeft geen periode tijdens deze datum"
            period_id = period_ids[0][0]
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
            period_ids = get_period_ids(cursor, company_id, date)
            if len(period_ids) == 0:
                return "Dit bedrijf heeft geen periode tijdens deze datum"
            period_id = period_ids[0][0]
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
            period_ids = get_period_ids(cursor, company_id, date)
            if len(period_ids) == 0:
                return "Dit bedrijf heeft geen periode tijdens deze datum"
            period_id = period_ids[0][0]
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
            period_ids = get_period_ids(cursor, company_id, date)
            if len(period_ids) == 0:
                return "Dit bedrijf heeft geen periode tijdens deze datum"
            period_id = period_ids[0][0]
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
            period_ids = get_period_ids(cursor, company_id, date)
            if len(period_ids) == 0:
                return "Dit bedrijf heeft geen periode tijdens deze datum"
            period_id = period_ids[0][0]
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
    return ebitda - afschrijvingen

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
            period_ids = get_period_ids(cursor, company_id, date)
            if len(period_ids) == 0:
                return "Dit bedrijf heeft geen periode tijdens deze datum"
            period_id = period_ids[0][0]
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
    
    return (handelsvorderingen / omzet) * 365

def reconciliation_api_call(company_id:int, date:str):
    '''
        Deze tool geeft de reconiliation ids en namen terug van reconiliations van het gegeven bedrijf en datum.

        Vereiste:
            - company_id (int): De company_id van het bedrijf
            - date (str): eind datum van de gezochte periode in YYYY-MM-DD formaat


        Retourneert:
            - reconiliation ids en namen terug van reconiliations van het gegeven bedrijf en datum.
    
        '''
    date = timezone.datetime.fromisoformat(date)
    period_ids = f"""
            SELECT period_id
            FROM periods
            WHERE company_id = {company_id} and DATE_PART('year', end_date) = {date.year} and DATE_PART('month', end_date) = {date.month}
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
        
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(period_ids)
            period_ids = cursor.fetchall()
            period_id = period_ids[0][0]
            print(period_id)
            sql = f"""SELECT reconciliation_id, name
                FROM reconciliations
                WHERE 
                    company_id = {company_id} AND
                    period_id = {period_id};
                """
            cursor.execute(sql)
            records = cursor.fetchall()
            return records

def list_tables():
    """This tool lists all the tables present in the schema."""
    sql = "SELECT table_name FROM information_schema.tables WHERE table_schema='public'"
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            result = cursor.fetchall()
    return result

def describe_tables(table_name:str):
    """This tool describes the table given by the table name. It returns the column names and their types"""
    sql = f"SELECT column_name,data_type FROM information_schema.columns WHERE table_name = {table_name}"
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            result = cursor.fetchall()
    return result

def load_data(sql_query:str):
    """Use this tool as a last resort to create your own query. To correctly use this tool you will need the list_tables_tool and the describe_tables_tool"""
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql_query)
            result = cursor.fetchall()
    return result

