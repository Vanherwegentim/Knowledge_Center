from django.utils import timezone
from psycopg2._psycopg import cursor


def get_period_ids(cursor:cursor, company_id:int, date:str):
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
    cursor.execute(period_ids)
    period_ids = cursor.fetchall()
    if len(period_ids) == 0:
        return "Dit bedrijf heeft geen periode tijdens deze datum"
    
    return period_ids